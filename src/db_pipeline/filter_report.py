"""Сверка фильтра DuckDB с замороженной двадцаткой.

``write_filter_report(fixture, data)`` читает ``tests/fixtures/labeled_changes.json``, гоняет ``filter_candidate`` без ``sql_only`` и пишет ``data/filter_sample.jsonl``. Расхождения — в ``data/filter_notes.md``. Метки фикстуры не правятся: если совпадение падает, правятся префиксы в ``repos.toml``. Возвращает пару (сколько совпало, сколько строк).
"""

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
def write_filter_report(fixture: Path | None = None, data: Path | None = None) -> tuple[int, int]:
    source = project_root() / "tests" / "fixtures" / "labeled_changes.json" if fixture is None else fixture
    root = resolve_data(data)
    payload = as_dict(load_json_file(source), "fixture")
    rows = as_object_list(payload.get("changes"), "fixture.changes")
    core = require_core(load_cores(), "duckdb")
    matched = 0
    lines: list[str] = []
    mismatches: list[str] = []
    for row in rows:
        item = as_dict(row, "change")
        change = _change(item)
        expected = as_str(item.get("expected"), "expected")
        decision = filter_candidate(change, core, set())
        actual = "accept" if decision.accept else "reject"
        ok = actual == expected
        if ok:
            matched += 1
        else:
            mismatches.append(
                f"PR {change.pr_number}: разметка {expected}, фильтр {actual} ({decision.reason_code})"
            )
        record = {
            "pr_number": change.pr_number,
            "sha": change.sha,
            "subject": change.subject,
            "expected": expected,
            "decision": actual,
            "reason_code": decision.reason_code,
            "reason": decision.reason,
            "match": ok,
        }
        lines.append(json.dumps(record, ensure_ascii=False))
    report = root / "filter_sample.jsonl"
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    notes = root / "filter_notes.md"
    if len(mismatches) == 0:
        if notes.exists():
            notes.unlink()
    else:
        text = "# Расхождения фильтра\n\n" + "\n".join(f"- {line}" for line in mismatches) + "\n"
        notes.write_text(text, encoding="utf-8")
    return matched, len(rows)


@beartype
def _change(item: dict[str, object]) -> ChangeView:
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
        repository_id="duckdb",
        subject=as_str(item.get("subject"), "subject"),
        body=as_str(item.get("body"), "body"),
        parent_count=as_int(item.get("parent_count"), "parent_count"),
        files=files,
        pr_number=optional_int(item.get("pr_number")),
        base_sha=as_str(item.get("base_sha", ""), "base_sha"),
    )
