"""Доступ только для чтения к архиву проверенных данных PRISM.

Архив ``prism-knowledge-20261014`` — внешний неизменяемый снимок данных, из
которого граф v2 берёт производные таблицы. Мы никогда не пишем в архив: все
соединения открываются как ``file:<path>?mode=ro&immutable=1``, поэтому SQLite
не создаёт ``-wal``/``-shm`` рядом с чужими файлами и не берёт блокировки.

Используются три базы:

- ``transitions.sqlite3`` — переходы кода: ``processed_commits``, ``paths``,
  ``hunks``, ``hunk_sides``, ``symbol_overlaps``, ``failures``;
- ``exact-symbols.sqlite3`` — точные символы tree-sitter по blob'ам с ролью
  ``parsed_definition`` / ``parsed_declaration``;
- ``postgresql-git-corpus.sqlite3`` — индекс git: ``commits`` (в т.ч.
  ``patch_id``), ``parents``, ``changed_paths``.

Путь к архиву берётся из ``BENCH_PRISM_ROOT``; без переменной ищется
``prism-knowledge-20261014`` рядом с корнем проекта. Отсутствие архива — не
ошибка импорта: модуль сообщает об этом только при обращении к данным.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from beartype import beartype

from db_pipeline.paths import project_root

ARCHIVE_NAME = "prism-knowledge-20261014"

TRANSITIONS_REL = "storage/provenance/enrichment/full-code-20261011/transitions.sqlite3"
EXACT_SYMBOLS_REL = "storage/provenance/enrichment/full-code-20261011/exact-symbols.sqlite3"
GIT_CORPUS_REL = "storage/provenance/indexes/postgresql-git-corpus.sqlite3"
VERSIONED_SOURCES_REL = "storage/provenance/indexes/postgresql-versioned-sources.sqlite3"


@beartype
@dataclass(frozen=True)
class PrismArchive:
    """Пути к базам архива и к git-клону, дающему байты исходников."""

    root: Path
    transitions: Path
    exact_symbols: Path
    git_corpus: Path
    versioned_sources: Path

    def present(self) -> list[str]:
        """Возвращает список отсутствующих обязательных баз."""
        missing: list[str] = []
        for path in (self.transitions, self.exact_symbols, self.git_corpus):
            if not path.is_file():
                missing.append(str(path))
        return missing


@beartype
def archive_root() -> Path:
    """Корень архива: ``BENCH_PRISM_ROOT`` либо сосед корня проекта."""
    raw = os.environ.get("BENCH_PRISM_ROOT", "")
    if raw != "":
        return Path(raw)
    return project_root().parent / ARCHIVE_NAME


@beartype
def load_archive(root: Path | None = None) -> PrismArchive:
    base = archive_root() if root is None else root
    return PrismArchive(
        root=base,
        transitions=base / TRANSITIONS_REL,
        exact_symbols=base / EXACT_SYMBOLS_REL,
        git_corpus=base / GIT_CORPUS_REL,
        versioned_sources=base / VERSIONED_SOURCES_REL,
    )


@beartype
def readonly_uri(path: Path) -> str:
    """URI ``file:...`` для неизменяемого чтения без WAL и блокировок.

    ``Path.as_uri`` percent-кодирует кириллицу в пути, что обязательно: SQLite
    разбирает URI и иначе не откроет файл.
    """
    return path.resolve().as_uri() + "?mode=ro&immutable=1"


@beartype
def open_readonly(path: Path) -> sqlite3.Connection:
    """Открывает базу архива только для чтения с ``row_factory`` по имени."""
    if not path.is_file():
        raise FileNotFoundError(f"нет базы PRISM: {path}")
    conn = sqlite3.connect(readonly_uri(path), uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


@beartype
def file_sha256(path: Path, chunk: int = 1 << 20) -> str:
    """SHA-256 файла архива одной линейной проходкой (для manifest)."""
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk)
            if block == b"":
                break
            digest.update(block)
    return digest.hexdigest()
