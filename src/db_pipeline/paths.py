"""Корень пакета и каталог рабочих данных.

``project_root`` — два уровня выше этого файла, каталог ``db_pipeline`` с ``repos.toml``. ``resolve_data(data)`` возвращает переданный каталог или ``<корень>/data`` и создаёт его.
"""

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
