from __future__ import annotations

import json
from pathlib import Path

from beartype import beartype

from db_pipeline.config import CoreConfig, load_cores, require_core
from db_pipeline.filter_rules import filter_candidate
from db_pipeline.git_ops import commit_body, commit_parents, commit_subject, file_changes
from db_pipeline.models import ChangeView, FileChange
from db_pipeline.parsing import as_dict, as_int, as_object_list, as_str, as_str_list, load_json_file
from db_pipeline.paths import project_root, resolve_data
from db_pipeline.samples import PostgresSample, bundled_git_dir, load_samples


@beartype
def write_postgres_labeled_report(fixture: Path | None = None, data: Path | None = None) -> tuple[int, int]:
    source = project_root() / "tests" / "fixtures" / "labeled_postgres.json" if fixture is None else fixture
    root = resolve_data(data)
    payload = as_dict(load_json_file(source), "fixture")
    rows = as_object_list(payload.get("changes"), "fixture.changes")
    core = require_core(load_cores(), "postgres")
    matched, lines, mismatches = _score_rows(rows, core)
    report = root / "postgres_labeled.jsonl"
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    _write_notes(root, matched, len(rows), None, None, mismatches)
    return matched, len(rows)


@beartype
def write_postgres_corpus_report(data: Path | None = None) -> tuple[int, int]:
    root = resolve_data(data)
    core = require_core(load_cores(), "postgres")
    git_dir = bundled_git_dir(core)
    if git_dir is None:
        raise RuntimeError("нет локального зеркала pg-gym/stand/pg.git")
    samples = load_samples(core)
    lines: list[str] = []
    mismatches: list[str] = []
    matched = 0
    for sample in samples:
        change = _change_from_git(git_dir, sample)
        decision = filter_candidate(change, core, set(), sample.solution_paths, sample.test_files)
        expected = _list_rule(sample.solution_paths, sample.test_files, [item.path for item in change.files])
        actual = "accept" if decision.accept else "reject"
        ok = actual == expected
        if ok:
            matched += 1
        else:
            mismatches.append(f"{sample.name}: правило {expected}, фильтр {actual} ({decision.reason_code})")
        record = {
            "name": sample.name,
            "sha": sample.sha,
            "expected": expected,
            "decision": actual,
            "reason_code": decision.reason_code,
            "reason": decision.reason,
            "match": ok,
        }
        lines.append(json.dumps(record, ensure_ascii=False))
    (root / "postgres_filter.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    labeled_path = project_root() / "tests" / "fixtures" / "labeled_postgres.json"
    labeled_matched = 0
    labeled_total = 0
    labeled_mismatches: list[str] = []
    if labeled_path.is_file():
        payload = as_dict(load_json_file(labeled_path), "fixture")
        rows = as_object_list(payload.get("changes"), "fixture.changes")
        labeled_matched, _lines, labeled_mismatches = _score_rows(rows, core)
        labeled_total = len(rows)
    _write_notes(root, labeled_matched, labeled_total, matched, len(samples), labeled_mismatches + mismatches)
    return matched, len(samples)


@beartype
def _score_rows(
    rows: list[object],
    core: CoreConfig,
) -> tuple[int, list[str], list[str]]:
    matched = 0
    lines: list[str] = []
    mismatches: list[str] = []
    for row in rows:
        item = as_dict(row, "change")
        change, solution_paths, test_paths = _change_from_row(item)
        expected = as_str(item.get("expected"), "expected")
        decision = filter_candidate(change, core, set(), solution_paths, test_paths)
        actual = "accept" if decision.accept else "reject"
        ok = actual == expected
        if ok:
            matched += 1
        else:
            mismatches.append(f"{change.sha[:12]}: разметка {expected}, фильтр {actual} ({decision.reason_code})")
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
    return matched, lines, mismatches


@beartype
def _list_rule(solution_paths: list[str], test_paths: list[str], paths: list[str]) -> str:
    if len(solution_paths) == 0 or len(test_paths) == 0:
        return "reject"
    allowed = set(solution_paths) | set(test_paths)
    if any(path not in allowed for path in paths):
        return "reject"
    has_code = any(path in solution_paths for path in paths)
    has_test = any(path in test_paths for path in paths)
    if has_code and has_test:
        return "accept"
    return "reject"


@beartype
def _change_from_git(git_dir: Path, sample: PostgresSample) -> ChangeView:
    parents = commit_parents(git_dir, sample.sha)
    return ChangeView(
        sha=sample.sha,
        repository_id="postgres",
        subject=commit_subject(git_dir, sample.sha),
        body=commit_body(git_dir, sample.sha),
        parent_count=len(parents),
        files=file_changes(git_dir, sample.parent, sample.sha),
        pr_number=None,
        base_sha=sample.parent,
    )


@beartype
def _change_from_row(item: dict[str, object]) -> tuple[ChangeView, list[str], list[str]]:
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
    change = ChangeView(
        sha=as_str(item.get("sha"), "sha"),
        repository_id="postgres",
        subject=as_str(item.get("subject"), "subject"),
        body=as_str(item.get("body"), "body"),
        parent_count=as_int(item.get("parent_count"), "parent_count"),
        files=files,
        pr_number=None,
        base_sha=as_str(item.get("base_sha"), "base_sha"),
    )
    return (
        change,
        as_str_list(item.get("solution_paths"), "solution_paths"),
        as_str_list(item.get("test_paths"), "test_paths"),
    )


@beartype
def _write_notes(
    root: Path,
    labeled_matched: int,
    labeled_total: int,
    corpus_matched: int | None,
    corpus_total: int | None,
    mismatches: list[str],
) -> None:
    lines = ["# Фильтр PostgreSQL", ""]
    if labeled_total > 0:
        lines.append(f"Заранее отмеченная двадцатка: {labeled_matched} из {labeled_total}.")
    if corpus_matched is not None and corpus_total is not None:
        lines.append(f"Все образцы без Docker: {corpus_matched} из {corpus_total}.")
    lines.append("")
    if len(mismatches) == 0:
        lines.append("Расхождений нет.")
    else:
        lines.append("Расхождения:")
        lines.extend(f"- {item}" for item in mismatches)
    (root / "postgres_filter_notes.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
