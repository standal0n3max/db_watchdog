"""Аудит недоопределённых задач по тестовому патчу эталона.

Эталон читается только здесь и только для курирования: агент его не видит.
Задача помечается, если в ``expected/*.out`` изменился вывод уже существующего
запроса или добавлены ``LINE n:``, ``ERROR``, ``DETAIL``, ``HINT`` с текстом,
которого нет ни в ``goal``, ни в ``contract``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark import convert, paths
from db_pipeline.benchmark.models import as_str_list

_SQL_RE = re.compile(
    r"^(?:SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|ALTER|DROP|VALUES|EXPLAIN|COPY|DO|MERGE|TABLE)\b",
    re.IGNORECASE,
)
_DIAG_RE = re.compile(r"^(ERROR|DETAIL|HINT):\s*(.*)$")
_LINE_RE = re.compile(r"^LINE\s+\d+:\s*(.*)$")


@beartype
def reasons_for(test_patch: str, goal: str, contract: list[str]) -> list[str]:
    """Коды причин. Пустой список — постановка покрывает ожидаемый вывод."""
    visible = "\n".join([goal, *contract])
    reasons: list[str] = []
    changed = False
    hidden: list[str] = []
    for _path, hunk in _out_hunks(test_patch):
        if _changes_existing_query(hunk):
            changed = True
        for line in hunk:
            if not line.startswith("+") or line.startswith("+++"):
                continue
            missing = _hidden_diagnostic(line[1:].strip(), visible)
            if missing != "" and missing not in hidden:
                hidden.append(missing)
    if changed:
        reasons.append("changed_existing_output")
    reasons.extend(hidden[:3])
    return reasons


@beartype
def audit_names(names: list[str], data: Path | None = None) -> dict[str, object]:
    """Помечает задачи. Поля каждой строки: ``underspecified`` и ``reason``."""
    rows: list[dict[str, object]] = []
    flagged: list[str] = []
    for name in names:
        public = convert.load_public(name, data)
        oracle = convert.load_oracle(name, data)
        found = reasons_for(
            str(oracle.get("test") or ""),
            str(public.get("goal") or ""),
            as_str_list(public.get("contract")),
        )
        underspecified = len(found) > 0
        if underspecified:
            flagged.append(name)
        rows.append(
            {
                "task": name,
                "underspecified": underspecified,
                "reason": "; ".join(found),
            }
        )
    return {
        "tasks_total": len(rows),
        "underspecified": flagged,
        "tasks": rows,
    }


@beartype
def write_report(report: dict[str, object], data: Path | None = None) -> Path:
    target = paths.reports_dir(data) / "spec-audit.json"
    target.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return target


@beartype
def _out_hunks(patch: str) -> list[tuple[str, list[str]]]:
    found: list[tuple[str, list[str]]] = []
    for chunk in patch.split("diff --git ")[1:]:
        lines = chunk.splitlines()
        if len(lines) == 0:
            continue
        path = _path_from_header(lines[0])
        if "/expected/" not in path or not path.endswith(".out"):
            continue
        hunk: list[str] = []
        opened = False
        for line in lines[1:]:
            if line.startswith("@@"):
                if opened and len(hunk) > 0:
                    found.append((path, hunk))
                hunk = []
                opened = True
                continue
            if opened:
                hunk.append(line)
        if opened and len(hunk) > 0:
            found.append((path, hunk))
    return found


@beartype
def _path_from_header(header: str) -> str:
    if not header.startswith("a/") or " b/" not in header:
        return ""
    return header[2:].split(" b/", 1)[0]


@beartype
def _changes_existing_query(lines: list[str]) -> bool:
    """Правка вывода запроса, который уже был в ожидаемом файле."""
    mode = ""
    for line in lines:
        if line == "" or line.startswith(("@@", "+++", "---")):
            continue
        prefix = line[0]
        if prefix not in "+- ":
            continue
        body = line[1:].strip()
        is_sql = _SQL_RE.match(body) is not None
        if is_sql and prefix in " -":
            mode = "existing"
            continue
        if is_sql and prefix == "+":
            mode = "new"
            continue
        if mode == "existing" and prefix in "+-":
            return True
    return False


@beartype
def _hidden_diagnostic(body: str, visible: str) -> str:
    line_match = _LINE_RE.match(body)
    if line_match is not None:
        payload = line_match.group(1).strip()
        if payload != "" and payload not in visible and body not in visible:
            return "hidden_error_position: " + body[:120]
        return ""
    diag = _DIAG_RE.match(body)
    if diag is None:
        return ""
    payload = diag.group(2).strip()
    if payload == "" or payload in visible or body in visible:
        return ""
    return "hidden_error_text: " + body[:120]
