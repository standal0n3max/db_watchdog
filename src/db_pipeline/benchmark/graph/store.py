"""Локальный индекс графа: SQLite со схемой и FTS.

Индексируется публичная история PostgreSQL: commits/messages, ревизии файлов,
hunks и символы C-кода. Revert и отрицательные результаты хранятся отдельными
записями, ответы и нулевые результаты пишутся в audit log.

Индекс и полный mirror лежат в ``data/graph/`` и в git не попадают.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from beartype import beartype

SCHEMA = """
CREATE TABLE IF NOT EXISTS commits (
    sha         TEXT PRIMARY KEY,
    parents     TEXT NOT NULL DEFAULT '',
    message     TEXT NOT NULL DEFAULT '',
    author_time INTEGER NOT NULL DEFAULT 0,
    is_revert   INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS files (
    commit_sha TEXT NOT NULL,
    path       TEXT NOT NULL,
    blob_sha   TEXT NOT NULL DEFAULT '',
    sha256     TEXT NOT NULL DEFAULT '',
    size       INTEGER NOT NULL DEFAULT 0,
    revision   TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (commit_sha, path)
);
CREATE TABLE IF NOT EXISTS hunks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_sha TEXT NOT NULL,
    path       TEXT NOT NULL,
    added      TEXT NOT NULL DEFAULT '',
    removed    TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS symbols (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_sha TEXT NOT NULL,
    path       TEXT NOT NULL,
    name       TEXT NOT NULL,
    kind       TEXT NOT NULL DEFAULT 'function',
    line       INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS audit (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL DEFAULT '',
    tool         TEXT NOT NULL,
    args         TEXT NOT NULL DEFAULT '',
    empty_result INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
CREATE INDEX IF NOT EXISTS idx_symbols_name ON symbols(name);
CREATE INDEX IF NOT EXISTS idx_hunks_path ON hunks(path);
CREATE VIRTUAL TABLE IF NOT EXISTS search USING fts5(
    commit_sha, path, kind, body, tokenize='unicode61'
);
"""


@beartype
def open_store(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


@beartype
def add_commit(
    conn: sqlite3.Connection,
    sha: str,
    parents: list[str],
    message: str,
    author_time: int,
) -> None:
    conn.execute(
        (
            "INSERT OR REPLACE INTO commits(sha, parents, message, author_time, is_revert) "
            "VALUES (?, ?, ?, ?, ?)"
        ),
        (sha, " ".join(parents), message, author_time, 1 if _is_revert(message) else 0),
    )
    conn.execute(
        "INSERT INTO search(commit_sha, path, kind, body) VALUES (?, ?, ?, ?)",
        (sha, "", "message", message),
    )


@beartype
def add_file(
    conn: sqlite3.Connection,
    commit_sha: str,
    path: str,
    blob_sha: str,
    sha256: str,
    size: int,
    revision: str,
) -> None:
    conn.execute(
        (
            "INSERT OR REPLACE INTO files(commit_sha, path, blob_sha, sha256, size, revision) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        ),
        (commit_sha, path, blob_sha, sha256, size, revision),
    )


@beartype
def add_hunk(
    conn: sqlite3.Connection,
    commit_sha: str,
    path: str,
    added: str,
    removed: str,
) -> None:
    conn.execute(
        "INSERT INTO hunks(commit_sha, path, added, removed) VALUES (?, ?, ?, ?)",
        (commit_sha, path, added, removed),
    )
    conn.execute(
        "INSERT INTO search(commit_sha, path, kind, body) VALUES (?, ?, ?, ?)",
        (commit_sha, path, "hunk", f"{added}\n{removed}"),
    )


@beartype
def add_symbol(
    conn: sqlite3.Connection,
    commit_sha: str,
    path: str,
    name: str,
    kind: str,
    line: int,
) -> None:
    conn.execute(
        "INSERT INTO symbols(commit_sha, path, name, kind, line) VALUES (?, ?, ?, ?, ?)",
        (commit_sha, path, name, kind, line),
    )
    conn.execute(
        "INSERT INTO search(commit_sha, path, kind, body) VALUES (?, ?, ?, ?)",
        (commit_sha, path, "symbol", name),
    )


@beartype
def log_audit(
    conn: sqlite3.Connection,
    tool: str,
    args: str,
    empty_result: bool,
    ts: str = "",
) -> None:
    conn.execute(
        "INSERT INTO audit(ts, tool, args, empty_result) VALUES (?, ?, ?, ?)",
        (ts, tool, args, 1 if empty_result else 0),
    )
    conn.commit()


@beartype
def _is_revert(message: str) -> bool:
    first = message.splitlines()[0].lower() if message else ""
    return first.startswith(("revert", "back out", "backout", "undo "))


@beartype
def count_rows(path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    with closing(open_store(path)) as conn:
        for table in ("commits", "files", "hunks", "symbols"):
            counts[table] = int(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
    return counts
