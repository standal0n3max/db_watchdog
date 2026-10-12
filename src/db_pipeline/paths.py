from __future__ import annotations

from pathlib import Path

from beartype import beartype


@beartype
def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


@beartype
def resolve_data(data: Path | None) -> Path:
    if data is not None:
        data.mkdir(parents=True, exist_ok=True)
        return data
    path = project_root() / "data"
    path.mkdir(parents=True, exist_ok=True)
    return path
