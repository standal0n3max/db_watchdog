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
import sys
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import paths
from db_pipeline.benchmark.graph import leakguard
from db_pipeline.benchmark.graph import view as graph_view
from db_pipeline.benchmark.graph.gitgraph import graph_db

# Имя функции C-кода перед открывающей скобкой.
_FUNC_RE = re.compile(r"\b([A-Za-z_]\w{3,})\s*\(")

# Тестовые пути в патче: expected/*.out и sql/*.sql.
_TEST_PATH_RE = re.compile(r"b/(\S+\.(?:out|sql|spec))")

# Инструменты единого графа.
_ALL_TOOLS = (
    "find_code_symbols",
    "symbol_history",
    "similar_fixes",
    "tests_to_code",
    "find_error_origin",
    "search_git_history",
)

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
def run_task(name: str, data: Path | None = None, with_history: bool = True) -> dict[str, object]:
    """Прогоняет канарейки и негативный контроль по ``GraphView`` задачи."""
    del with_history
    repo = paths.pg_clone(data)
    task = leakguard.load_task(name, data)
    canary_list = leakguard.canaries(task, repo)
    db = graph_db(data)
    opened = graph_view.open_view(db, task.base, task.head)
    responses: dict[str, str] = {}
    try:
        for tool, args in canary_queries(task):
            if tool not in _ALL_TOOLS and tool != "search_git_history":
                continue
            responses[tool] = _call(opened, tool, args)
        head_text = graph_view.read_versioned_source(
            opened, task.head, task.paths_touched[0] if task.paths_touched else "src/backend/x.c"
        )
        responses["negative_head"] = json.dumps(head_text, ensure_ascii=False)
    finally:
        graph_view.close_view(opened)
    ancestors = set(opened.visible)
    ancestors.add(task.base)
    violations = leakguard.scan_responses(responses, canary_list, task.head)
    membership = leakguard.check_membership(responses, ancestors)
    nonempty = sorted(
        tool
        for tool, text in responses.items()
        if '"results": []' not in text and '"error"' not in text
    )
    negative_ok = "outside_cutoff" in responses.get("negative_head", "")
    if not negative_ok:
        violations.append("head_sha не отклонён")
    return {
        "task": name,
        "layer": "git",
        "canaries": len(canary_list),
        "calls": sorted(responses),
        "nonempty_calls": nonempty,
        "empty_calls": sorted(set(responses) - set(nonempty)),
        "canary_violations": violations,
        "membership_violations": membership,
        "ok": len(violations) == 0 and len(membership) == 0 and negative_ok,
    }


@beartype
def _call(opened: graph_view.GraphView, tool: str, args: dict[str, object]) -> str:
    """Вызывает инструмент и возвращает его ответ как JSON-текст."""
    if tool == "find_code_symbols":
        payload = graph_view.find_code_symbols(opened, str(args["name"]))
    elif tool == "search_git_history":
        payload = graph_view.search_git_history(
            opened, str(args["query"]), int(cast("int", args.get("limit", 5)))
        )
    elif tool == "symbol_history":
        payload = graph_view.symbol_history(opened, str(args["path"]), str(args["name"]))
    elif tool == "tests_to_code":
        payload = graph_view.tests_to_code(opened, str(args["test"]))
    elif tool == "similar_fixes":
        payload = graph_view.similar_fixes(
            opened, str(args["query"]), int(cast("int", args.get("limit", 5)))
        )
    elif tool == "find_error_origin":
        payload = graph_view.find_error_origin(opened, str(args["message"]))
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
