from __future__ import annotations

import json
import os
from pathlib import Path

from beartype import beartype

from db_pipeline.errors import PipelineError
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
