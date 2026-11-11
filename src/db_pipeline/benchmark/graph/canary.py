"""Прогон канареек по инструментам графа (шаг D).

Шаг D требует проверить канарейки сначала на v1, затем на v2. Модуль вызывает
ровно те функции, которые регистрирует MCP-сервер (`tools.*` для v1,
`history_tools.*` для v2), на реальной отсечке конкретной задачи, а затем
проверяет:

- **канарейки** — добавленные эталоном строки и ``head_sha`` не встречаются ни в
  одном ответе (``leakguard.scan_responses``);
- **membership** — каждый 40-символьный ``oid`` в ответах есть предок ``base_sha``
  (``leakguard.check_membership``).

Запросы строятся из самого эталона: имена функций из добавленных строк патча,
имя изменённого регрессионного теста, текст ожидаемой ошибки. То есть проверка
идёт по тому, что агент реально мог бы спросить, увидев тест, но не правку.

Соединения открываются напрямую к файлам индексов, без Docker и транспорта:
так проверяются сами инструменты и ``Cutoff``. Транспорт MCP проверяет
``probe_client`` отдельно; вместе они закрывают шаг D.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import paths
from db_pipeline.benchmark.graph import (
    cutoff as cutoff_module,
    history_store,
    history_tools,
    leakguard,
    store,
    tools,
)

# Имя функции C-кода перед открывающей скобкой.
_FUNC_RE = re.compile(r"\b([A-Za-z_]\w{3,})\s*\(")

# Тестовые пути в патче: expected/*.out и sql/*.sql.
_TEST_PATH_RE = re.compile(r"b/(\S+\.(?:out|sql|spec))")

# Инструменты, доступные только при подключённой производной истории.
_V2_TOOLS = ("symbol_history", "tests_to_code", "similar_fixes", "find_error_origin")

# Фразы, характерные для вывода регрессионных тестов.
_ERROR_MARKERS = ("ERROR", "FATAL", "does not exist", "could not", "must be")


@beartype
def canary_queries(task: leakguard.TaskRef, limit: int = 6) -> list[tuple[str, dict[str, object]]]:
    """Строит вызовы инструментов из самого эталона задачи."""
    names: list[str] = []
    for line in leakguard.added_lines(task.gold_patch):
        for match in _FUNC_RE.finditer(line):
            found = match.group(1)
            if found not in names:
                names.append(found)
    queries: list[tuple[str, dict[str, object]]] = []
    for name in names[:limit]:
        queries.append(("find_code_symbols", {"name": name}))
        if task.paths_touched:
            queries.append(
                ("symbol_history", {"path": task.paths_touched[0], "name": name})
            )
        queries.append(("similar_fixes", {"query": name, "limit": 5}))
    for path in _TEST_PATH_RE.findall(task.test_patch)[:2]:
        queries.append(("tests_to_code", {"test": path}))
    for message in expected_messages(task.test_patch)[:2]:
        queries.append(("find_error_origin", {"message": message}))
    for path in task.paths_touched[:2]:
        queries.append(("search_git_history", {"query": Path(path).name, "limit": 5}))
    return queries


@beartype
def expected_messages(test_patch: str) -> list[str]:
    """Строки ожидаемого вывода теста, похожие на сообщения об ошибке."""
    found: list[str] = []
    for line in leakguard.added_lines(test_patch):
        text = line.strip()
        if len(text) < 15:
            continue
        if not any(marker in text for marker in _ERROR_MARKERS):
            continue
        if text not in found:
            found.append(text)
    return found


@beartype
def _ancestors(repo: Path, base: str) -> set[str]:
    """Все предки ``base_sha`` по клону: список законных ревизий."""
    import subprocess

    result = subprocess.run(
        ["git", "-C", str(repo), "rev-list", base],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return set(result.stdout.split())


@beartype
def _history_ready(path: Path) -> bool:
    """Совместимый алиас: готовность производной истории."""
    return history_store.history_ready(path)


@beartype
def run_task(
    name: str,
    data: Path | None = None,
    with_history: bool = True,
) -> dict[str, object]:
    """Прогоняет канарейки по одной задаче на реальных индексах."""
    repo = paths.pg_clone(data)
    task = leakguard.load_task(name, data)
    canary_list = leakguard.canaries(task, repo)
    ancestors = _ancestors(repo, task.base)
    db = paths.graph_dir(data) / "postgres.sqlite"
    history_db = db.parent / "history.sqlite"
    responses: dict[str, str] = {}
    index_open = with_history and history_store.history_ready(history_db)
    with closing(store.open_store(db)) as conn:
        base_time = store.commit_time(conn, task.base) or 0
        reachable = store.ancestors(conn, task.base)
        reachable.add(task.base)
        cutoff = cutoff_module.build_cutoff(
            task.base, base_time, reachable, forbidden_head=task.head
        )
        # v2-инструменты обязаны видеть полную историю, иначе канарейки
        # проверялись бы на пустых ответах и ничего не доказывали.
        cutoff_v2 = (
            history_tools.cutoff_for_base(history_db, task.base, forbidden_head=task.head)
            if index_open
            else cutoff
        )
        index = (
            history_tools.open_index(history_db, db, cutoff_v2) if index_open else None
        )
        try:
            for tool, args in canary_queries(task):
                if index is None and tool in _V2_TOOLS:
                    continue
                responses[tool] = _call(conn, index, cutoff, tool, args)
        finally:
            if index is not None:
                history_tools.close_index(index)
    violations = leakguard.scan_responses(responses, canary_list, task.head)
    membership = leakguard.check_membership(responses, ancestors)
    # Пустой ответ не доказывает изоляцию: канарейка просто не могла попасть.
    # Поэтому фиксируем, какие инструменты вернули данные.
    nonempty = sorted(
        tool
        for tool, text in responses.items()
        if '"results": []' not in text and '"error"' not in text
    )
    return {
        "task": name,
        "layer": "v2" if index_open else "v1",
        "canaries": len(canary_list),
        "calls": sorted(responses),
        "nonempty_calls": nonempty,
        "empty_calls": sorted(set(responses) - set(nonempty)),
        "canary_violations": violations,
        "membership_violations": membership,
        "ok": len(violations) == 0 and len(membership) == 0,
    }


@beartype
def _call(
    conn: sqlite3.Connection,
    index: history_tools.HistoryIndex | None,
    cutoff: cutoff_module.Cutoff,
    tool: str,
    args: dict[str, object],
) -> str:
    """Вызывает инструмент и возвращает его ответ как JSON-текст."""
    if tool == "find_code_symbols":
        payload = tools.find_code_symbols(conn, cutoff, str(args["name"]))
    elif tool == "search_git_history":
        payload = tools.search_git_history(
            conn, cutoff, str(args["query"]), int(cast("int", args["limit"]))
        )
    elif index is None:
        return ""
    else:
        if tool == "symbol_history":
            payload = history_tools.symbol_history(
                index, str(args["path"]), str(args["name"])
            )
        elif tool == "tests_to_code":
            payload = history_tools.tests_to_code(index, str(args["test"]))
        elif tool == "similar_fixes":
            payload = history_tools.similar_fixes(
                index, str(args["query"]), int(cast("int", args["limit"]))
            )
        elif tool == "find_error_origin":
            payload = history_tools.find_error_origin(index, str(args["message"]))
        else:
            return ""
    return json.dumps(payload, ensure_ascii=False)


@beartype
def main(argv: list[str]) -> int:
    if len(argv) < 1 or len(argv) > 2:
        print("usage: canary TASK [TASK...]", file=sys.stderr)
        return 2
    reports = [run_task(name) for name in argv]
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    return 0 if all(bool(report["ok"]) for report in reports) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
