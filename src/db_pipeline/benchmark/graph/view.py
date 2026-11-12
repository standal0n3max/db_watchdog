"""GraphView: одна отсечка ``base_sha`` для всех инструментов.

``visible`` — предки базы, включая её саму, без ``head_sha`` и его потомков.
Файл открывается ``mode=ro&immutable=1``. Инструменты не ходят в сырые таблицы
мимо этого множества: co-change считается на запросе, будущее не хранится.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
import threading
import zlib
from dataclasses import dataclass
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark.graph.cutoff import Cutoff, build_cutoff
from db_pipeline.benchmark.graph.symbols import is_test_path, normalize_message

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")
_MASS_COMMIT = 8


@beartype
@dataclass
class GraphView:
    """Открытый граф и видимые коммиты одной базы."""

    conn: sqlite3.Connection
    base_sha: str
    head_sha: str
    base_time: int
    visible: frozenset[str]
    cutoff: Cutoff
    lock: threading.Lock


@beartype
def open_view(db: Path, base_sha: str, head_sha: str = "") -> GraphView:
    """Строит отсечку один раз. База после сборки уже без WAL."""
    conn = sqlite3.connect(f"file:{db}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    parents, children = _links(conn)
    ancestors = _walk(base_sha, parents)
    hidden: set[str] = set()
    if head_sha != "":
        hidden = _walk(head_sha, children)
        hidden.add(head_sha)
    visible = frozenset(ancestors - hidden)
    row = conn.execute(
        "SELECT author_time FROM commits WHERE oid = ?", (base_sha,)
    ).fetchone()
    base_time = 0 if row is None else int(row[0])
    cutoff = build_cutoff(base_sha, base_time, set(visible), forbidden_head=head_sha)
    conn.execute("CREATE TEMP TABLE visible_oid (oid TEXT PRIMARY KEY)")
    conn.executemany("INSERT INTO visible_oid(oid) VALUES (?)", [(oid,) for oid in visible])
    return GraphView(
        conn=conn,
        base_sha=base_sha,
        head_sha=head_sha,
        base_time=base_time,
        visible=visible,
        cutoff=cutoff,
        lock=threading.Lock(),
    )


@beartype
def close_view(view: GraphView) -> None:
    view.conn.close()


@beartype
def _links(conn: sqlite3.Connection) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    parents: dict[str, list[str]] = {}
    children: dict[str, list[str]] = {}
    for row in conn.execute("SELECT oid, parent_oid, ordinal FROM parents ORDER BY ordinal"):
        oid, parent = str(row[0]), str(row[1])
        parents.setdefault(oid, []).append(parent)
        children.setdefault(parent, []).append(oid)
    return parents, children


@beartype
def _walk(start: str, edges: dict[str, list[str]]) -> set[str]:
    if start == "":
        return set()
    seen: set[str] = set()
    stack = [start]
    while stack:
        oid = stack.pop()
        if oid in seen:
            continue
        seen.add(oid)
        stack.extend(edges.get(oid, []))
    return seen


@beartype
def _meta(view: GraphView) -> dict[str, object]:
    return {
        "base_sha": view.base_sha,
        "base_time": view.base_time,
        "ancestry_enforced": True,
    }


@beartype
def _empty(view: GraphView, reason: str) -> dict[str, object]:
    return {"results": [], "error": reason, "reason": reason, "cutoff": _meta(view)}


@beartype
def _allows(view: GraphView, oid: str) -> bool:
    return view.cutoff.allows(oid, 0)


@beartype
def find_code_symbols(view: GraphView, name: str, limit: int = 20) -> dict[str, object]:
    """Символы дерева базы: revision всегда равен ``base_sha``."""
    with view.lock:
        exact = _symbol_rows(view, name, exact=True)
        partial = _symbol_rows(view, name, exact=False)
    chosen: dict[tuple[str, str], sqlite3.Row] = {}
    for row in exact + partial:
        key = (str(row["path"]), str(row["name"]))
        current = chosen.get(key)
        if current is None or _better_symbol(row, current):
            chosen[key] = row
    ordered = sorted(chosen.values(), key=lambda row: (_symbol_rank(row), str(row["path"])))
    results: list[dict[str, object]] = []
    for row in ordered[:limit]:
        line = int(row["line"])
        end_line = int(row["end_line"])
        results.append(
            {
                "name": str(row["name"]),
                "kind": str(row["kind"]),
                "path": str(row["path"]),
                "line": line,
                "end_line": end_line,
                "revision": view.base_sha,
                "sha256": str(row["sha256"] or ""),
                "cutoff": _meta(view),
            }
        )
    if len(results) == 0:
        return _empty(view, "not_found")
    return {"results": results, "cutoff": _meta(view)}


@beartype
def _symbol_rows(view: GraphView, name: str, exact: bool) -> list[sqlite3.Row]:
    if exact:
        query = (
            "SELECT s.name AS name, s.kind AS kind, s.path AS path, s.line AS line, "
            "s.end_line AS end_line, s.definition AS definition, b.sha256 AS sha256 "
            "FROM symbols s JOIN tree_entries t ON t.blob_oid = s.blob_oid AND t.path = s.path "
            "LEFT JOIN blobs b ON b.blob_oid = s.blob_oid "
            "WHERE t.commit_oid = ? AND s.name = ? LIMIT 4000"
        )
        params: tuple[object, ...] = (view.base_sha, name)
    else:
        escaped = name.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        query = (
            "SELECT s.name AS name, s.kind AS kind, s.path AS path, s.line AS line, "
            "s.end_line AS end_line, s.definition AS definition, b.sha256 AS sha256 "
            "FROM symbols s JOIN tree_entries t ON t.blob_oid = s.blob_oid AND t.path = s.path "
            "LEFT JOIN blobs b ON b.blob_oid = s.blob_oid "
            "WHERE t.commit_oid = ? AND s.name LIKE ? ESCAPE '\\' LIMIT 4000"
        )
        params = (view.base_sha, f"%{escaped}%")
    return view.conn.execute(query, params).fetchall()


@beartype
def _better_symbol(candidate: sqlite3.Row, current: sqlite3.Row) -> bool:
    return _symbol_rank(candidate) < _symbol_rank(current)


@beartype
def _symbol_rank(row: sqlite3.Row) -> tuple[int, int]:
    line = int(row["line"])
    end_line = int(row["end_line"])
    definition = 0 if int(row["definition"]) == 1 or end_line > line else 1
    return definition, line


@beartype
def read_versioned_source(
    view: GraphView,
    revision: str,
    path: str,
    offset: int = 0,
    limit: int = 8192,
    start_line: int | None = None,
    end_line: int | None = None,
) -> dict[str, object]:
    """Фрагмент файла: дерево базы либо blob из ``file_change`` окна."""
    if not _allows(view, revision):
        return _empty(view, "outside_cutoff")
    with view.lock:
        blob = _blob_for(view, revision, path)
        if blob is None:
            return _empty(view, "not_found")
        digest, content = blob
    fragment = _fragment(content, offset, limit, start_line, end_line)
    return {
        "results": [
            {
                "revision": revision,
                "path": path,
                "sha256": digest,
                "size": len(content),
                "start_line": fragment["start_line"],
                "end_line": fragment["end_line"],
                "total_lines": fragment["total_lines"],
                "byte_span": fragment["byte_span"],
                "content": fragment["content"],
                "cutoff": _meta(view),
            }
        ],
        "cutoff": _meta(view),
    }


@beartype
def _blob_for(view: GraphView, revision: str, path: str) -> tuple[str, bytes] | None:
    if revision == view.base_sha:
        row = view.conn.execute(
            "SELECT blob_oid FROM tree_entries WHERE commit_oid = ? AND path = ?",
            (revision, path),
        ).fetchone()
    else:
        row = view.conn.execute(
            "SELECT blob_oid FROM file_change WHERE oid = ? AND path = ? AND blob_oid != ''",
            (revision, path),
        ).fetchone()
    if row is None:
        return None
    stored = view.conn.execute(
        "SELECT sha256, content_zlib FROM blobs WHERE blob_oid = ?", (str(row[0]),)
    ).fetchone()
    if stored is None:
        return None
    return str(stored[0]), zlib.decompress(stored[1])


@beartype
def _fragment(
    content: bytes,
    offset: int,
    limit: int,
    start_line: int | None,
    end_line: int | None,
) -> dict[str, object]:
    if start_line is None and end_line is None:
        start = max(0, min(offset, len(content)))
        end = max(start, min(start + max(0, limit), len(content)))
        text = content[start:end]
        return {
            "start_line": _line_number(content, start),
            "end_line": _line_number(content, max(start, end - 1)),
            "total_lines": content.count(b"\n") + (0 if content.endswith(b"\n") else 1),
            "byte_span": [start, end],
            "content": text.decode("utf-8", "replace"),
        }
    lines = content.splitlines(keepends=True)
    total = len(lines)
    first = max(1, start_line if start_line is not None else 1)
    last = end_line if end_line is not None else total
    if last < first:
        last = first
    last = min(last, total if total > 0 else first)
    start = _offset_of_line(lines, first)
    end = _offset_of_line(lines, last + 1) if last < total else len(content)
    return {
        "start_line": first,
        "end_line": last,
        "total_lines": total,
        "byte_span": [start, end],
        "content": content[start:end].decode("utf-8", "replace"),
    }


@beartype
def _line_number(content: bytes, offset: int) -> int:
    if offset <= 0:
        return 1
    return content.count(b"\n", 0, offset) + 1


@beartype
def _offset_of_line(lines: list[bytes], number: int) -> int:
    if number <= 1:
        return 0
    total = 0
    for index in range(min(number - 1, len(lines))):
        total += len(lines[index])
    return total


@beartype
def search_git_history(view: GraphView, query: str, limit: int = 20) -> dict[str, object]:
    """FTS по сообщениям видимых коммитов и ханкам окна."""
    match = _fts_query(query)
    with view.lock:
        messages = view.conn.execute(
            "SELECT f.oid AS oid, c.subject AS subject, c.message AS message "
            "FROM commits_fts f JOIN commits c ON c.oid = f.oid "
            "JOIN visible_oid v ON v.oid = f.oid "
            "WHERE commits_fts MATCH ? ORDER BY bm25(commits_fts) LIMIT ?",
            (match, max(limit, 1)),
        ).fetchall()
        hunks = view.conn.execute(
            "SELECT f.oid AS oid, f.path AS path, f.added AS added, f.removed AS removed "
            "FROM hunks_fts f JOIN visible_oid v ON v.oid = f.oid "
            "WHERE hunks_fts MATCH ? ORDER BY bm25(hunks_fts) LIMIT ?",
            (match, max(limit, 1)),
        ).fetchall()
    results: list[dict[str, object]] = []
    for row in messages:
        if not _allows(view, str(row["oid"])):
            continue
        snippet = str(row["subject"])
        results.append(_hit(view, str(row["oid"]), "", "message", snippet))
        if len(results) >= limit:
            break
    if len(results) < limit:
        for row in hunks:
            if not _allows(view, str(row["oid"])):
                continue
            snippet = (str(row["added"]) + "\n" + str(row["removed"]))[:400]
            results.append(_hit(view, str(row["oid"]), str(row["path"]), "hunk", snippet))
            if len(results) >= limit:
                break
    if len(results) == 0:
        return _empty(view, "not_found")
    return {"results": results, "cutoff": _meta(view)}


@beartype
def _hit(view: GraphView, revision: str, path: str, kind: str, snippet: str) -> dict[str, object]:
    return {
        "revision": revision,
        "path": path,
        "kind": kind,
        "sha256": hashlib.sha256(snippet.encode("utf-8", "replace")).hexdigest(),
        "snippet": snippet[:400],
        "cutoff": _meta(view),
    }


@beartype
def _fts_query(query: str) -> str:
    tokens = [token for token in _TOKEN_RE.findall(query) if token != ""]
    if len(tokens) == 0:
        return '"__no_match__"'
    return " OR ".join(f'"{token}"*' for token in tokens)


@beartype
def symbol_history(view: GraphView, path: str, name: str, limit: int = 20) -> dict[str, object]:
    """Как менялся символ: от свежих видимых коммитов к старым."""
    with view.lock:
        rows = view.conn.execute(
            "SELECT s.oid AS oid, s.path AS path, s.name AS name, s.kind AS kind, "
            "s.role AS role, s.side AS side, s.sym_line AS sym_line, "
            "s.sym_end_line AS sym_end_line, c.subject AS subject, c.author_time AS author_time "
            "FROM symbol_changes s JOIN commits c ON c.oid = s.oid "
            "WHERE s.path = ? AND s.name = ?",
            (path, name),
        ).fetchall()
    best: dict[str, sqlite3.Row] = {}
    for row in rows:
        oid = str(row["oid"])
        if not _allows(view, oid):
            continue
        current = best.get(oid)
        if current is None or _prefer_change(row, current):
            best[oid] = row
    ordered = sorted(
        best.values(),
        key=lambda row: (int(row["author_time"]), str(row["oid"])),
        reverse=True,
    )
    results: list[dict[str, object]] = []
    for row in ordered[:limit]:
        oid = str(row["oid"])
        results.append(
            {
                "revision": oid,
                "path": str(row["path"]),
                "name": str(row["name"]),
                "kind": str(row["kind"]),
                "role": str(row["role"]),
                "is_definition": str(row["role"]) == "definition",
                "line": int(row["sym_line"]),
                "end_line": int(row["sym_end_line"]),
                "side": str(row["side"]),
                "subject": str(row["subject"] or ""),
                "sha256": "",
                "cutoff": _meta(view),
            }
        )
    if len(results) == 0:
        return _empty(view, "not_found")
    return {"results": results, "cutoff": _meta(view)}


@beartype
def _prefer_change(candidate: sqlite3.Row, current: sqlite3.Row) -> bool:
    candidate_def = str(candidate["role"]) == "definition"
    current_def = str(current["role"]) == "definition"
    if candidate_def != current_def:
        return candidate_def
    return int(candidate["sym_line"]) < int(current["sym_line"])


@beartype
def tests_to_code(view: GraphView, test: str, limit: int = 20) -> dict[str, object]:
    """Co-change на запросе: вес 1/n, коммиты больше 8 путей отброшены."""
    with view.lock:
        touched = view.conn.execute(
            "SELECT oid FROM file_change WHERE path = ?", (test,)
        ).fetchall()
        oids = [str(row[0]) for row in touched if _allows(view, str(row[0]))]
        grouped = _paths_by_commit(view, oids)
    weights: dict[str, float] = {}
    sample: dict[str, str] = {}
    commits_hit: dict[str, int] = {}
    for oid, paths in grouped.items():
        if len(paths) > _MASS_COMMIT or test not in paths:
            continue
        share = 1.0 / len(paths)
        for path in paths:
            if path == test or is_test_path(path):
                continue
            weights[path] = weights.get(path, 0.0) + share
            commits_hit[path] = commits_hit.get(path, 0) + 1
            sample.setdefault(path, oid)
    ordered = sorted(weights, key=lambda path: (-weights[path], path))
    results: list[dict[str, object]] = []
    for path in ordered[:limit]:
        revision = sample[path]
        results.append(
            {
                "test_path": test,
                "code_path": path,
                "path": path,
                "weight": round(weights[path], 4),
                "commits": commits_hit[path],
                "sample_revision": revision,
                "revision": revision,
                "sha256": "",
                "cutoff": _meta(view),
            }
        )
    if len(results) == 0:
        return _empty(view, "not_found")
    return {"results": results, "cutoff": _meta(view)}


@beartype
def _paths_by_commit(view: GraphView, oids: list[str]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    if len(oids) == 0:
        return grouped
    for start in range(0, len(oids), 400):
        chunk = oids[start : start + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = view.conn.execute(
            f"SELECT oid, path FROM file_change WHERE oid IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in rows:
            grouped.setdefault(str(row[0]), []).append(str(row[1]))
    return grouped


@beartype
def similar_fixes(view: GraphView, query: str, limit: int = 20) -> dict[str, object]:
    """Похожие правки: FTS по сообщениям только видимых коммитов."""
    match = _fts_query(query)
    with view.lock:
        rows = view.conn.execute(
            "SELECT f.oid AS oid, c.subject AS subject, c.author_time AS author_time, "
            "c.message AS message FROM commits_fts f "
            "JOIN commits c ON c.oid = f.oid JOIN visible_oid v ON v.oid = f.oid "
            "WHERE commits_fts MATCH ? ORDER BY bm25(commits_fts) LIMIT ?",
            (match, max(limit, 1)),
        ).fetchall()
    results: list[dict[str, object]] = []
    for row in rows:
        oid = str(row["oid"])
        if not _allows(view, oid):
            continue
        message = str(row["message"] or "")
        results.append(
            {
                "revision": oid,
                "path": "",
                "subject": str(row["subject"] or ""),
                "authored_at": str(row["author_time"]),
                "sha256": hashlib.sha256(message.encode("utf-8", "replace")).hexdigest(),
                "cutoff": _meta(view),
            }
        )
    if len(results) == 0:
        return _empty(view, "not_found")
    return {"results": results, "cutoff": _meta(view)}


@beartype
def find_error_origin(view: GraphView, message: str, limit: int = 20) -> dict[str, object]:
    """Откуда печатается ошибка: строки текущего дерева базы, не чужого."""
    wanted = normalize_message(message)
    tokens = [token for token in _TOKEN_RE.findall(wanted) if len(token) >= 3]
    if len(tokens) == 0:
        return _empty(view, "not_found")
    needle = tokens[0]
    with view.lock:
        rows = view.conn.execute(
            "SELECT e.message AS message, e.line AS line, t.path AS path, b.sha256 AS sha256 "
            "FROM error_strings e JOIN tree_entries t ON t.blob_oid = e.blob_oid "
            "LEFT JOIN blobs b ON b.blob_oid = e.blob_oid "
            "WHERE t.commit_oid = ? AND e.message LIKE ? LIMIT 4000",
            (view.base_sha, f"%{needle}%"),
        ).fetchall()
    scored: list[tuple[float, sqlite3.Row]] = []
    for row in rows:
        score = _similarity(wanted, str(row["message"]))
        if score <= 0:
            continue
        scored.append((score, row))
    scored.sort(key=lambda item: (-item[0], str(item[1]["path"]), int(item[1]["line"])))
    results: list[dict[str, object]] = []
    for score, row in scored[:limit]:
        results.append(
            {
                "message": str(row["message"]),
                "path": str(row["path"]),
                "line": int(row["line"]),
                "score": round(score, 4),
                "revision": view.base_sha,
                "sha256": str(row["sha256"] or ""),
                "cutoff": _meta(view),
            }
        )
    if len(results) == 0:
        return _empty(view, "not_found")
    return {"results": results, "cutoff": _meta(view)}


@beartype
def _similarity(left: str, right: str) -> float:
    left_words = {word for word in left.split() if len(word) >= 3}
    right_words = {word for word in right.split() if len(word) >= 3}
    if not left_words or not right_words:
        return 0.0
    common = left_words & right_words
    ratio = len(common) / max(1, min(len(left_words), len(right_words)))
    if ratio < 0.34:
        return 0.0
    return ratio
