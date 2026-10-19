from __future__ import annotations

import json
from pathlib import Path

from beartype import beartype

from db_pipeline.config import load_cores, require_core
from db_pipeline.filter_rules import filter_candidate
from db_pipeline.models import ChangeView, FileChange
from db_pipeline.parsing import as_dict, as_int, as_object_list, as_str, load_json_file, optional_int
from db_pipeline.paths import project_root, resolve_data


@beartype
def write_sql_labeled_report(
    core_id: str,
    fixture: Path | None = None,
    data: Path | None = None,
) -> tuple[int, int]:
    source = (
        project_root() / "tests" / "fixtures" / f"labeled_sql_{core_id}.json"
        if fixture is None
        else fixture
    )
    root = resolve_data(data)
    payload = as_dict(load_json_file(source), "fixture")
    rows = as_object_list(payload.get("changes"), "fixture.changes")
    core = require_core(load_cores(), core_id)
    matched = 0
    lines: list[str] = []
    mismatches: list[str] = []
    for row in rows:
        item = as_dict(row, "change")
        change = _change(item, core_id)
        expected = as_str(item.get("expected"), "expected")
        decision = filter_candidate(change, core, set(), sql_only=True)
        actual = "accept" if decision.accept else "reject"
        ok = actual == expected
        if ok:
            matched += 1
        else:
            mismatches.append(
                f"{change.sha[:12]}: разметка {expected}, фильтр {actual} ({decision.reason_code})"
            )
        record = {
            "sha": change.sha,
            "subject": change.subject,
            "expected": expected,
            "decision": actual,
            "reason_code": decision.reason_code,
            "reason": decision.reason,
            "match": ok,
        }
        lines.append(json.dumps(record, ensure_ascii=False))
    (root / f"{core_id}_sql_filter.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    notes = root / f"{core_id}_sql_filter_notes.md"
    # Совпадение с замороженной двадцаткой «код ядра и прежний тест».
    # Узкий срез func.c, adt/ и core_functions/ больше не измеряется.
    header = (
        f"# Фильтр ядра, {core_id}\n\n"
        f"Совпадение с замороженной разметкой: {matched} из {len(rows)}.\n"
    )
    if len(mismatches) == 0:
        notes.write_text(header + "\nРасхождений нет.\n", encoding="utf-8")
    else:
        body = "\n".join(f"- {line}" for line in mismatches)
        notes.write_text(header + "\nРасхождения:\n\n" + body + "\n", encoding="utf-8")
    return matched, len(rows)


@beartype
def _change(item: dict[str, object], core_id: str) -> ChangeView:
    files: list[FileChange] = []
    for raw in as_object_list(item.get("files"), "files"):
        row = as_dict(raw, "file")
        files.append(
            FileChange(
                path=as_str(row.get("path"), "path"),
                additions=as_int(row.get("additions"), "additions"),
                deletions=as_int(row.get("deletions"), "deletions"),
                status=as_str(row.get("status", "modified"), "status"),
            )
        )
    return ChangeView(
        sha=as_str(item.get("sha"), "sha"),
        repository_id=core_id,
        subject=as_str(item.get("subject"), "subject"),
        body=as_str(item.get("body"), "body"),
        parent_count=as_int(item.get("parent_count"), "parent_count"),
        files=files,
        pr_number=optional_int(item.get("pr_number")),
        base_sha=as_str(item.get("base_sha", ""), "base_sha"),
    )
