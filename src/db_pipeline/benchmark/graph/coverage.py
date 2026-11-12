"""Гейт покрытия: каждая из 299 баз отвечает семью инструментами внутри отсечки.

Структурные проверки сверяют граф с ``git rev-list --count``. Живой probe берёт
запросы из дерева самой задачи: символ, слово из subject, строку ошибки и тест
с видимым co-change. Все ``revision`` в ответах обязаны быть в ``visible``.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import convert, paths
from db_pipeline.benchmark.graph import split as split_module
from db_pipeline.benchmark.graph import view as graph_view
from db_pipeline.benchmark.graph.gitgraph import graph_db, table_hashes

_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{3,}")


@beartype
def coverage(data: Path | None = None, names: list[str] | None = None) -> dict[str, object]:
    """Отчёт по задачам. ``names`` ограничивает прогон (для smoke и тестов)."""
    chosen = _names(data) if names is None else list(names)
    db = graph_db(data)
    failures: list[dict[str, object]] = []
    passed = 0
    for name in chosen:
        oracle = convert.load_oracle(name, data)
        base = str(oracle["base"])
        head = str(oracle["head"])
        problems = _check_one(db, data, name, base, head)
        if problems:
            failures.append({"task": name, "problems": problems})
        else:
            passed += 1
    report: dict[str, object] = {
        "tasks": len(chosen),
        "passed": passed,
        "failed": len(failures),
        "failures": failures,
        "ok": len(failures) == 0 and len(chosen) > 0,
        "table_hashes": table_hashes(db) if db.is_file() else {},
    }
    return report


@beartype
def _names(data: Path | None) -> list[str]:
    split = split_module.derive_split(data)
    found: list[str] = []
    for section in split_module.SECTIONS:
        found.extend(split[section])
    return found


@beartype
def _check_one(db: Path, data: Path | None, name: str, base: str, head: str) -> list[str]:
    del name
    problems: list[str] = []
    if not db.is_file():
        return ["нет graph.sqlite"]
    opened = graph_view.open_view(db, base, head)
    try:
        problems.extend(_structural(opened, data, base))
        problems.extend(_probe(opened))
    finally:
        graph_view.close_view(opened)
    return problems


@beartype
def _structural(opened: graph_view.GraphView, data: Path | None, base: str) -> list[str]:
    problems: list[str] = []
    row = opened.conn.execute("SELECT 1 FROM commits WHERE oid = ?", (base,)).fetchone()
    if row is None:
        problems.append("база отсутствует в commits")
    expected = _rev_list_count(paths.pg_clone(data), base)
    if expected >= 0 and len(opened.visible) != expected:
        problems.append(f"видимых {len(opened.visible)} против git {expected}")
    if head_leaks(opened):
        problems.append("head попал в visible")
    for table, query in (
        ("tree_entries", "SELECT 1 FROM tree_entries WHERE commit_oid = ? LIMIT 1"),
        (
            "symbols",
            "SELECT 1 FROM symbols s JOIN tree_entries t ON t.blob_oid = s.blob_oid "
            "AND t.path = s.path WHERE t.commit_oid = ? LIMIT 1",
        ),
        (
            "error_strings",
            "SELECT 1 FROM error_strings e JOIN tree_entries t ON t.blob_oid = e.blob_oid "
            "WHERE t.commit_oid = ? LIMIT 1",
        ),
        (
            "hunks",
            "SELECT 1 FROM hunks h JOIN windows w ON w.oid = h.oid WHERE w.base_oid = ? LIMIT 1",
        ),
    ):
        if opened.conn.execute(query, (base,)).fetchone() is None:
            problems.append(f"пусто: {table}")
    return problems


@beartype
def head_leaks(opened: graph_view.GraphView) -> bool:
    return opened.head_sha != "" and opened.head_sha in opened.visible


@beartype
def _rev_list_count(repo: Path, base: str) -> int:
    result = subprocess.run(
        ["git", "-C", str(repo), "rev-list", "--count", base],
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        return int(result.stdout.strip())
    except ValueError:
        return -1


@beartype
def _probe(opened: graph_view.GraphView) -> list[str]:
    problems: list[str] = []
    symbol = _symbol_name(opened)
    if symbol == "":
        problems.append("нет символа для probe")
    else:
        problems.extend(_expect(opened, "find_code_symbols", graph_view.find_code_symbols(opened, symbol, 5)))
    path = _source_path(opened)
    if path == "":
        problems.append("нет пути для probe")
    else:
        problems.extend(
            _expect(
                opened,
                "read_versioned_source",
                graph_view.read_versioned_source(opened, opened.base_sha, path, limit=200),
            )
        )
    word = _subject_word(opened)
    if word == "":
        problems.append("нет слова subject для probe")
    else:
        problems.extend(
            _expect(opened, "search_git_history", graph_view.search_git_history(opened, word, 5))
        )
        problems.extend(
            _expect(opened, "similar_fixes", graph_view.similar_fixes(opened, word, 5))
        )
    history = _symbol_change(opened)
    if history is None:
        problems.append("нет symbol_history для probe")
    else:
        problems.extend(
            _expect(
                opened,
                "symbol_history",
                graph_view.symbol_history(opened, history[0], history[1], 5),
            )
        )
    test = _cochange_test(opened)
    if test == "":
        problems.append("нет теста с co-change")
    else:
        problems.extend(_expect(opened, "tests_to_code", graph_view.tests_to_code(opened, test, 5)))
    message = _error_message(opened)
    if message == "":
        problems.append("нет errmsg для probe")
    else:
        problems.extend(
            _expect(opened, "find_error_origin", graph_view.find_error_origin(opened, message, 5))
        )
    return problems


@beartype
def _expect(opened: graph_view.GraphView, tool: str, payload: dict[str, object]) -> list[str]:
    rows = payload.get("results")
    if not isinstance(rows, list):
        return [f"{tool}: пустой ответ"]
    items = cast("list[object]", rows)
    if len(items) == 0:
        return [f"{tool}: пустой ответ"]
    problems: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        row = cast("dict[str, object]", item)
        revision = row.get("revision") or row.get("sample_revision")
        if not isinstance(revision, str) or revision == "":
            continue
        if revision not in opened.visible:
            problems.append(f"{tool}: revision вне visible")
    return problems


@beartype
def _symbol_name(opened: graph_view.GraphView) -> str:
    row = opened.conn.execute(
        "SELECT s.name FROM symbols s JOIN tree_entries t "
        "ON t.blob_oid = s.blob_oid AND t.path = s.path "
        "WHERE t.commit_oid = ? AND s.definition = 1 LIMIT 1",
        (opened.base_sha,),
    ).fetchone()
    if row is None:
        row = opened.conn.execute(
            "SELECT s.name FROM symbols s JOIN tree_entries t "
            "ON t.blob_oid = s.blob_oid AND t.path = s.path WHERE t.commit_oid = ? LIMIT 1",
            (opened.base_sha,),
        ).fetchone()
    return "" if row is None else str(row[0])


@beartype
def _source_path(opened: graph_view.GraphView) -> str:
    row = opened.conn.execute(
        "SELECT path FROM tree_entries WHERE commit_oid = ? AND path LIKE '%.c' LIMIT 1",
        (opened.base_sha,),
    ).fetchone()
    return "" if row is None else str(row[0])


@beartype
def _subject_word(opened: graph_view.GraphView) -> str:
    row = opened.conn.execute(
        "SELECT subject, message FROM commits WHERE oid = ?", (opened.base_sha,)
    ).fetchone()
    if row is None:
        return ""
    text = f"{row[0]}\n{row[1]}"
    for match in _TOKEN_RE.finditer(text):
        token = match.group(0)
        letters = sum(1 for char in token if char.isalpha())
        if letters >= 4 and letters * 2 >= len(token):
            return token
    return ""


@beartype
def _symbol_change(opened: graph_view.GraphView) -> tuple[str, str] | None:
    row = opened.conn.execute(
        "SELECT path, name FROM symbol_changes WHERE oid = ? LIMIT 1",
        (opened.base_sha,),
    ).fetchone()
    if row is not None:
        return str(row[0]), str(row[1])
    sample = opened.conn.execute(
        "SELECT s.path, s.name FROM symbol_changes s JOIN commits c ON c.oid = s.oid "
        "WHERE s.oid IN (SELECT oid FROM windows WHERE base_oid = ?) LIMIT 1",
        (opened.base_sha,),
    ).fetchone()
    if sample is None:
        return None
    return str(sample[0]), str(sample[1])


@beartype
def _cochange_test(opened: graph_view.GraphView) -> str:
    """Тест, который в видимом коммите менялся вместе с кодом, не больше 8 путей."""
    rows = opened.conn.execute(
        "SELECT f.oid FROM file_change f JOIN visible_oid v ON v.oid = f.oid "
        "GROUP BY f.oid HAVING count(*) <= 8 "
        "AND sum(CASE WHEN f.path LIKE '%.c' OR f.path LIKE '%.h' THEN 1 ELSE 0 END) > 0 "
        "AND sum(CASE WHEN f.path LIKE '%.sql' OR f.path LIKE '%.out' OR f.path LIKE '%.spec' "
        "THEN 1 ELSE 0 END) > 0 LIMIT 1"
    ).fetchall()
    if len(rows) == 0:
        return ""
    oid = str(rows[0][0])
    paths = opened.conn.execute(
        "SELECT path FROM file_change WHERE oid = ?", (oid,)
    ).fetchall()
    for (path,) in paths:
        text = str(path)
        if text.endswith((".sql", ".out", ".spec")):
            return text
    return ""


@beartype
def _error_message(opened: graph_view.GraphView) -> str:
    row = opened.conn.execute(
        "SELECT e.message FROM error_strings e JOIN tree_entries t ON t.blob_oid = e.blob_oid "
        "WHERE t.commit_oid = ? LIMIT 1",
        (opened.base_sha,),
    ).fetchone()
    return "" if row is None else str(row[0])


@beartype
def write_report(report: dict[str, object], data: Path | None = None) -> Path:
    target = paths.reports_dir(data) / "graph-coverage.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target
