"""Схема и доступ к производному ``history.sqlite``.

Таблицы графа v2 построены из архива PRISM и локального git-клона:

- ``commits`` — метаданные коммитов мастера (дата, сообщение, ``patch_id``);
- ``symbol_history`` — история символов: ``hunk ⋈ symbol_overlap`` по пути и
  имени, с переводом байтовых смещений в номера строк;
- ``co_change`` — матрица совместных изменений ``тест → код`` с весом;
- ``error_messages`` — нормализованные шаблоны ``ereport``/``errmsg`` по blob'ам
  деревьев баз (единственный собственный индекс графа v2);
- ``manifest`` — версия билдера, sha256 архива и хэш схемы.

Файл лежит в ``data/graph/history.sqlite`` и в git не попадает. Запись идёт
одной транзакцией на таблицу, чтение — обычным SQLite-соединением.
"""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

from beartype import beartype

SCHEMA_VERSION = 3

SCHEMA = """
CREATE TABLE IF NOT EXISTS commits (
    oid         TEXT PRIMARY KEY,
    authored_at TEXT NOT NULL DEFAULT '',
    committed_at TEXT NOT NULL DEFAULT '',
    subject     TEXT NOT NULL DEFAULT '',
    message     TEXT NOT NULL DEFAULT '',
    patch_id    TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS symbol_history (
    oid        TEXT NOT NULL,
    path       TEXT NOT NULL,
    name       TEXT NOT NULL,
    kind       TEXT NOT NULL DEFAULT '',
    role       TEXT NOT NULL DEFAULT '',
    line       INTEGER NOT NULL DEFAULT 0,
    end_line   INTEGER NOT NULL DEFAULT 0,
    sym_line   INTEGER NOT NULL DEFAULT 0,
    sym_end_line INTEGER NOT NULL DEFAULT 0,
    old_start  INTEGER NOT NULL DEFAULT 0,
    old_end    INTEGER NOT NULL DEFAULT 0,
    new_start  INTEGER NOT NULL DEFAULT 0,
    new_end    INTEGER NOT NULL DEFAULT 0,
    sym_start  INTEGER NOT NULL DEFAULT 0,
    sym_end    INTEGER NOT NULL DEFAULT 0,
    side       TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (oid, path, name, side, old_start, sym_line)
);
CREATE TABLE IF NOT EXISTS co_change (
    test_path  TEXT NOT NULL,
    code_path  TEXT NOT NULL,
    weight     REAL NOT NULL DEFAULT 0,
    commits    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (test_path, code_path)
);
CREATE TABLE IF NOT EXISTS path_commits (
    path  TEXT NOT NULL,
    oid   TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (path, oid)
);
CREATE TABLE IF NOT EXISTS parents (
    oid        TEXT NOT NULL,
    parent_oid TEXT NOT NULL,
    ordinal    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (oid, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_parents_parent ON parents(parent_oid);
CREATE TABLE IF NOT EXISTS error_messages (
    message   TEXT NOT NULL,
    base_sha  TEXT NOT NULL,
    path      TEXT NOT NULL,
    line      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (message, base_sha, path, line)
);
CREATE TABLE IF NOT EXISTS manifest (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE VIRTUAL TABLE IF NOT EXISTS commits_fts USING fts5(
    oid UNINDEXED, subject, message, tokenize='unicode61'
);
CREATE INDEX IF NOT EXISTS idx_symbol_history_name ON symbol_history(name, oid);
CREATE INDEX IF NOT EXISTS idx_symbol_history_path ON symbol_history(path, oid);
CREATE INDEX IF NOT EXISTS idx_co_change_test ON co_change(test_path, weight DESC);
CREATE INDEX IF NOT EXISTS idx_co_change_code ON co_change(code_path, weight DESC);
CREATE INDEX IF NOT EXISTS idx_path_commits_path ON path_commits(path, oid);
CREATE INDEX IF NOT EXISTS idx_error_message ON error_messages(message, base_sha);
"""


@beartype
def open_history(path: Path) -> sqlite3.Connection:
    """Открывает (или создаёт) ``history.sqlite`` с полной схемой."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


@beartype
def open_history_readonly(path: Path) -> sqlite3.Connection:
    """Открывает ``history.sqlite`` только для чтения (для MCP-сервера)."""
    if not path.is_file():
        raise FileNotFoundError(f"нет history.sqlite: {path}")
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


@beartype
def schema_sha256() -> str:
    """Хэш текста схемы: попадает в manifest и проверяется тестом."""
    return hashlib.sha256(SCHEMA.encode("utf-8")).hexdigest()


@beartype
def reset_schema(conn: sqlite3.Connection) -> None:
    """Удаляет таблицы и создаёт схему заново: нужно при смене ``SCHEMA_VERSION``.

    ``CREATE TABLE IF NOT EXISTS`` не добавит новую колонку в существующую
    таблицу, поэтому полная пересборка обязана начать со ``DROP``.
    """
    for table in ("commits_fts", "commits", "parents", "symbol_history", "co_change", "path_commits", "error_messages", "manifest"):
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.executescript(SCHEMA)
    conn.commit()
    reset_parents_cache()


# Кэш карты родителей: аудит и канарейки строят отсечку на каждую задачу, а
# таблица одна и та же. Без кэша это 105k строк на задачу.
_PARENTS_MAP_CACHE: dict[str, dict[str, list[str]]] = {}


def _database_key(conn: sqlite3.Connection) -> str:
    """Ключ кэша по файлу базы: соединения читают одну историю."""
    for row in conn.execute("PRAGMA database_list"):
        name = str(row[1])
        if name == "main":
            return str(row[2])
    return ""


@beartype
def ancestors(conn: sqlite3.Connection, oid: str) -> set[str]:
    """Все предки ``oid`` по таблице ``parents`` (сам ``oid`` не включается).

    Это полная история из корпуса PRISM, а не окно v1-индекса: только так
    ``symbol_history`` и ``similar_fixes`` видят всю правку, а не последние 250
    коммитов.
    """
    key = _database_key(conn)
    mapping = _PARENTS_MAP_CACHE.get(key) if key != "" else None
    if mapping is None:
        mapping = parents_map(conn)
        if key != "":
            _PARENTS_MAP_CACHE[key] = mapping
    found: set[str] = set()
    stack = list(mapping.get(oid, []))
    while stack:
        current = stack.pop()
        if current == "" or current in found:
            continue
        found.add(current)
        stack.extend(mapping.get(current, []))
    return found


@beartype
def reset_parents_cache() -> None:
    """Сбрасывает кэш карты родителей: тесты и пересборка не должны врать."""
    _PARENTS_MAP_CACHE.clear()


@beartype
def parents_map(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """Карта ``oid → родители`` из таблицы ``parents`` одной проходкой."""
    mapping: dict[str, list[str]] = {}
    for row in conn.execute("SELECT oid, parent_oid FROM parents ORDER BY oid, ordinal"):
        oid = str(row[0])
        parent = str(row[1])
        if oid == parent or parent == "":
            continue
        mapping.setdefault(oid, []).append(parent)
    return mapping


@beartype
def write_manifest(conn: sqlite3.Connection, values: dict[str, str]) -> None:
    """Записывает пары manifest, включая версию схемы и её хэш."""
    payload = dict(values)
    payload["schema_version"] = str(SCHEMA_VERSION)
    payload["schema_sha256"] = schema_sha256()
    conn.executemany(
        "INSERT OR REPLACE INTO manifest(key, value) VALUES (?, ?)",
        sorted(payload.items()),
    )


@beartype
def read_manifest(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute("SELECT key, value FROM manifest").fetchall()
    return {str(row["key"]): str(row["value"]) for row in rows}


@beartype
def history_ready(path: Path) -> bool:
    """``history.sqlite`` дописан и совпадает по версии схемы.

    Сборка идёт минуты и пишет таблицы по очереди, поэтому «файл существует» не
    значит «файл готов». Признак готовности — manifest с ``builder_version`` и
    текущей ``schema_version``.
    """
    if not path.is_file():
        return False
    try:
        conn = open_history_readonly(path)
    except (sqlite3.Error, FileNotFoundError):
        return False
    try:
        values = read_manifest(conn)
    except sqlite3.Error:
        return False
    finally:
        conn.close()
    if values.get("builder_version", "") == "":
        return False
    return values.get("schema_version", "") == str(SCHEMA_VERSION)


@beartype
def count_rows(path: Path) -> dict[str, int]:
    """Счётчики строк: попадают в отчёт сборки."""
    counts: dict[str, int] = {}
    with closing(open_history_readonly(path)) as conn:
        for table in ("commits", "symbol_history", "co_change", "path_commits", "error_messages"):
            counts[table] = int(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
    return counts
