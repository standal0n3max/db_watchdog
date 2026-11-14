"""Каталоги локальных данных benchmark.

Публичные задачи, приватные oracle и prep-патчи лежат в игнорируемом
``data/benchmark/postgres``. Запуски — в ``data/runs``, отчёты — в ``data/reports``,
индекс графа — в ``data/graph``. Ничего из этого в git не попадает.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from beartype import beartype

from db_pipeline.paths import project_root, resolve_data

__all__ = [
    "CORE",
    "benchmark_root",
    "docker_dir",
    "graph_dir",
    "oracle_dir",
    "pg_clone",
    "prep_dir",
    "project_root",
    "release_dir",
    "reports_dir",
    "resolve_data",
    "runs_dir",
    "tasks_dir",
]

CORE = "postgres"


@beartype
def benchmark_root(data: Path | None = None) -> Path:
    path = resolve_data(data) / "benchmark" / CORE
    path.mkdir(parents=True, exist_ok=True)
    return path


@beartype
def tasks_dir(data: Path | None = None) -> Path:
    path = benchmark_root(data) / "tasks"
    path.mkdir(parents=True, exist_ok=True)
    return path


@beartype
def oracle_dir(data: Path | None = None) -> Path:
    path = benchmark_root(data) / "oracle"
    path.mkdir(parents=True, exist_ok=True)
    return path


@beartype
def prep_dir(data: Path | None = None) -> Path:
    path = benchmark_root(data) / "prep"
    path.mkdir(parents=True, exist_ok=True)
    return path


@beartype
def runs_dir(data: Path | None = None) -> Path:
    path = resolve_data(data) / "runs"
    path.mkdir(parents=True, exist_ok=True)
    return path


@beartype
def reports_dir(data: Path | None = None) -> Path:
    path = resolve_data(data) / "reports"
    path.mkdir(parents=True, exist_ok=True)
    return path


@beartype
def graph_dir(data: Path | None = None) -> Path:
    path = resolve_data(data) / "graph"
    path.mkdir(parents=True, exist_ok=True)
    return path


@beartype
def pg_clone(data: Path | None = None) -> Path:
    return resolve_data(data) / "clones" / "postgres"


@beartype
def release_dir(data: Path | None = None, version: str = "") -> Path:
    """Каталог локального релиза датасета: ``data/releases/<version>``."""
    base = resolve_data(data) / "releases"
    return base if version == "" else base / version


@beartype
def docker_dir() -> Path:
    return project_root() / "docker"


@beartype
def write_atomic(path: Path, text: str) -> None:
    """Пишет файл через временный и ``os.replace``: читатель не видит обрывок."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
    )
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
