"""Сводка графа, которую оркестратор кладёт в промпт до первого шага.

Функция одна: ``build_briefing``. Она читает только публичные поля задачи
(``graded_by``, ``contract``, первая строка ``goal``) и отвечает через уже
открытый ``GraphView``. Эталон не открывается. Пустой ответ (все ``not_found``)
тоже воздействие: текст пишется как есть, прогон не перезапускается.

Бюджет текста около 1500 токенов. Лишние строки отрезаются с конца, порядок
секций фиксирован.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import cast

from beartype import beartype

from db_pipeline.benchmark.graph import view as graph_view
from db_pipeline.benchmark.graph.symbols import is_test_path
from db_pipeline.benchmark.graph.view import GraphView
from db_pipeline.benchmark.models import as_str_list

TOKEN_BUDGET = 1500
TESTS_LIMIT = 5
ORIGIN_LIMIT = 3
FIXES_LIMIT = 3


@beartype
@dataclass(frozen=True)
class Briefing:
    """Текст сводки, его хэш и список вызовов, из которых он собран."""

    text: str
    sha256: str
    tokens: int
    calls: tuple[dict[str, object], ...]
    delivered: bool = True

    def to_dict(self) -> dict[str, object]:
        return {
            "text": self.text,
            "sha256": self.sha256,
            "tokens": self.tokens,
            "calls": [dict(item) for item in self.calls],
            "delivered": self.delivered,
        }


@beartype
def estimate_tokens(text: str) -> int:
    """Оценка токенов: 4 символа на токен, как в заметке про шаг 0."""
    if text == "":
        return 0
    return max(1, (len(text) + 3) // 4)


@beartype
def build_briefing(view: GraphView, task: dict[str, object]) -> Briefing:
    """Собирает сводку по публичной задаче внутри уже открытой отсечки."""
    lines: list[str] = []
    calls: list[dict[str, object]] = []
    lines.append("tests_to_code")
    graded = as_str_list(task.get("graded_by"))
    if len(graded) == 0:
        lines.append("- none")
    for test in graded:
        payload = graph_view.tests_to_code(view, test, TESTS_LIMIT)
        calls.append(_call("tests_to_code", {"test": test, "limit": TESTS_LIMIT}, payload))
        lines.extend(_test_lines(test, payload))
    lines.append("find_error_origin")
    contract = as_str_list(task.get("contract"))
    if len(contract) == 0:
        lines.append("- none")
    for message in contract:
        payload = graph_view.find_error_origin(view, message, ORIGIN_LIMIT)
        calls.append(
            _call("find_error_origin", {"message": message, "limit": ORIGIN_LIMIT}, payload)
        )
        lines.extend(_origin_lines(message, payload))
    goal = str(task.get("goal") or "")
    query = goal.splitlines()[0].strip() if goal.strip() != "" else ""
    lines.append("similar_fixes")
    if query == "":
        lines.append("- none")
    else:
        payload = graph_view.similar_fixes(view, query, FIXES_LIMIT)
        calls.append(_call("similar_fixes", {"query": query, "limit": FIXES_LIMIT}, payload))
        lines.extend(_fix_lines(view, payload))
    text = _fit("\n".join(lines))
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return Briefing(
        text=text,
        sha256=digest,
        tokens=estimate_tokens(text),
        calls=tuple(calls),
        delivered=True,
    )


@beartype
def mentioned_paths(text: str) -> list[str]:
    """Пути репозитория, которые сводка назвала. Порядок стабильный."""
    found: list[str] = []
    for token in text.replace(",", " ").replace(";", " ").split():
        cleaned = token.strip("`'\"()[]")
        if not cleaned.startswith(("src/", "contrib/", "doc/")):
            continue
        if cleaned not in found:
            found.append(cleaned)
    return found


@beartype
def _fit(text: str) -> str:
    """Режет текст с конца по строкам, пока оценка токенов не войдёт в бюджет."""
    if estimate_tokens(text) <= TOKEN_BUDGET:
        return text
    rows = text.splitlines()
    while len(rows) > 1 and estimate_tokens("\n".join(rows)) > TOKEN_BUDGET:
        rows.pop()
    trimmed = "\n".join(rows)
    if estimate_tokens(trimmed) <= TOKEN_BUDGET:
        return trimmed
    # Одна строка всё ещё длиннее бюджета: режем по символам детерминированно.
    keep = TOKEN_BUDGET * 4
    return trimmed[:keep]


@beartype
def _call(tool: str, args: dict[str, object], payload: dict[str, object]) -> dict[str, object]:
    reason = str(payload.get("reason") or payload.get("error") or "")
    results = payload.get("results")
    count = len(cast("list[object]", results)) if isinstance(results, list) else 0
    return {
        "tool": tool,
        "args": args,
        "status": "not_found" if count == 0 else "ok",
        "hits": count,
        "reason": reason,
    }


@beartype
def _test_lines(test: str, payload: dict[str, object]) -> list[str]:
    results = _results(payload)
    if len(results) == 0:
        return [f"- {test}: not_found"]
    lines: list[str] = []
    for item in results:
        path = str(item.get("code_path") or item.get("path") or "")
        weight = item.get("weight")
        lines.append(f"- {test} -> {path} weight={weight}")
    return lines


@beartype
def _origin_lines(message: str, payload: dict[str, object]) -> list[str]:
    results = _results(payload)
    if len(results) == 0:
        return [f"- {message}: not_found"]
    lines: list[str] = []
    for item in results:
        path = str(item.get("path") or "")
        line = item.get("line")
        lines.append(f"- {message} @ {path}:{line}")
    return lines


@beartype
def _fix_lines(view: GraphView, payload: dict[str, object]) -> list[str]:
    results = _results(payload)
    if len(results) == 0:
        return ["- not_found"]
    lines: list[str] = []
    for item in results:
        subject = str(item.get("subject") or "").replace("\n", " ")
        revision = str(item.get("revision") or "")
        files = _changed_files(view, revision)
        shown = ", ".join(files[:6]) if len(files) > 0 else "no files"
        lines.append(f"- {subject} [{shown}]")
    return lines


@beartype
def _changed_files(view: GraphView, revision: str) -> list[str]:
    oid = graph_view.resolve_oid(view, revision)
    if oid == "":
        return []
    with view.lock:
        rows = view.conn.execute(
            "SELECT path FROM file_change WHERE oid = ? ORDER BY path",
            (oid,),
        ).fetchall()
    found: list[str] = []
    for row in rows:
        path = str(row[0])
        if path == "" or is_test_path(path):
            continue
        if path not in found:
            found.append(path)
    return found


@beartype
def _results(payload: dict[str, object]) -> list[dict[str, object]]:
    raw = payload.get("results")
    if not isinstance(raw, list):
        return []
    found: list[dict[str, object]] = []
    for item in cast("list[object]", raw):
        if isinstance(item, dict):
            row = cast("dict[object, object]", item)
            found.append({str(key): value for key, value in row.items()})
    return found
