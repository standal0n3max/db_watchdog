"""Журнал просмотренных sha: одна запись ``data/journal/<sha>.json``.

``create_journal`` создаёт файл через ``O_EXCL`` и возвращает ложь, если запись уже есть. ``write_json_once`` так же не перезаписывает JSON задачи или прогона. ``update_journal`` дописывает поля в существующую запись.

``known_shas`` — все файлы журнала. ``harvest_known_shas`` из этого множества выпускает записи, которые надо разобрать повторно: ``нет правки SQL-кода`` без запуска оракула для любого ядра и ``нет правки SQL-тестов`` без запуска оракула для DuckDB. ``accepted_count(data, repository_id)`` считает записи с ``oracle_status=accepted``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from beartype import beartype

from db_pipeline.errors import PipelineError
from db_pipeline.filter_rules import MISSING_SQL_CODE_REASON, MISSING_SQL_TEST_REASON
from db_pipeline.parsing import as_dict, load_json_file


@beartype
def journal_file(data: Path, sha: str) -> Path:
    return data / "journal" / f"{sha}.json"


@beartype
def known_shas(data: Path) -> set[str]:
    folder = data / "journal"
    if not folder.exists():
        return set()
    return {path.stem for path in folder.glob("*.json")}


@beartype
def accepted_count(data: Path, repository_id: str) -> int:
    folder = data / "journal"
    if not folder.exists():
        return 0
    total = 0
    for path in folder.glob("*.json"):
        record = as_dict(load_json_file(path), "journal")
        if record.get("repository_id") == repository_id and record.get("oracle_status") == "accepted":
            total += 1
    return total


@beartype
def harvest_known_shas(data: Path) -> set[str]:
    # Пропуск «нет правки SQL-кода» без оракула можно разобрать снова.
    # Принятые, отказы оракула, merge, документация и «нет теста» остаются известными.
    folder = data / "journal"
    if not folder.exists():
        return set()
    known: set[str] = set()
    for path in folder.glob("*.json"):
        record = as_dict(load_json_file(path), "journal")
        if reopen_harvest_record(record):
            continue
        sha = record.get("sha")
        if isinstance(sha, str) and sha != "":
            known.add(sha)
        else:
            known.add(path.stem)
    return known


@beartype
def reopen_missing_sql_code(record: dict[str, object]) -> bool:
    if record.get("reason") != MISSING_SQL_CODE_REASON:
        return False
    status = record.get("oracle_status")
    return status in ("skipped", "", None)


@beartype
def reopen_harvest_record(record: dict[str, object]) -> bool:
    if reopen_missing_sql_code(record):
        return True
    if record.get("repository_id") != "duckdb":
        return False
    if record.get("reason") != MISSING_SQL_TEST_REASON:
        return False
    status = record.get("oracle_status")
    return status in ("skipped", "", None)


@beartype
def create_journal(data: Path, record: dict[str, object]) -> bool:
    sha = record.get("sha")
    if not isinstance(sha, str) or sha == "":
        raise PipelineError("journal", "пустой sha")
    path = journal_file(data, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(record, ensure_ascii=False, indent=2) + "\n"
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(payload)
    return True


@beartype
def read_journal(data: Path, sha: str) -> dict[str, object] | None:
    path = journal_file(data, sha)
    if not path.exists():
        return None
    return as_dict(load_json_file(path), "journal")


@beartype
def update_journal(data: Path, sha: str, fields: dict[str, object]) -> None:
    current = read_journal(data, sha)
    if current is None:
        raise PipelineError("journal", f"нет записи {sha}")
    current.update(fields)
    journal_file(data, sha).write_text(
        json.dumps(current, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


@beartype
def write_json_once(path: Path, record: dict[str, object]) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(record, ensure_ascii=False, indent=2) + "\n"
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(payload)
    return True


@beartype
def write_json(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
