"""Локальный индекс графа: SQLite со схемой и FTS.

Индексируется публичная история PostgreSQL: commits/messages, изменённые ревизии
файлов, hunks и символы C-кода. Revert и отрицательные результаты хранятся
отдельными записями, ответы и нулевые результаты пишутся в audit log.

Дедупликация: bytes исходников лежат в нормализованной ``blobs`` по SHA-256,
ревизии файлов хранятся только по изменившимся путям (``revisions``), а символы
дедуплицированы по ``(path, sha256, name, line)``. Это даёт локальный
навигационный слой без многократной обработки неизменившихся файлов.

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
CREATE TABLE IF NOT EXISTS revisions (
    commit_sha TEXT NOT NULL,
    path       TEXT NOT NULL,
    sha256     TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (commit_sha, path)
);
CREATE TABLE IF NOT EXISTS blobs (
    sha256  TEXT PRIMARY KEY,
    content BLOB NOT NULL,
    size    INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS hunks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_sha TEXT NOT NULL,
    path       TEXT NOT NULL,
    old_start  INTEGER NOT NULL DEFAULT 0,
    old_count  INTEGER NOT NULL DEFAULT 0,
    new_start  INTEGER NOT NULL DEFAULT 0,
    new_count  INTEGER NOT NULL DEFAULT 0,
    added      TEXT NOT NULL DEFAULT '',
    removed    TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS symbols (
    path       TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    name       TEXT NOT NULL,
    kind       TEXT NOT NULL DEFAULT 'function',
    line       INTEGER NOT NULL DEFAULT 0,
    end_line   INTEGER NOT NULL DEFAULT 0,
    revision   TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (path, sha256, name, line)
);
CREATE TABLE IF NOT EXISTS audit (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL DEFAULT '',
    tool         TEXT NOT NULL,
    args         TEXT NOT NULL DEFAULT '',
    empty_result INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_revisions_path ON revisions(path);
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
def add_revision(
    conn: sqlite3.Connection,
    commit_sha: str,
    path: str,
    sha256: str,
) -> None:
    """Ревизия файла: хранится только для изменившихся путей коммита."""
    conn.execute(
        "INSERT OR REPLACE INTO revisions(commit_sha, path, sha256) VALUES (?, ?, ?)",
        (commit_sha, path, sha256),
    )


@beartype
def add_blob(conn: sqlite3.Connection, sha256: str, content: bytes) -> None:
    """Хранит bytes исходника в нормализованной таблице: дедупликация по SHA-256."""
    conn.execute(
        "INSERT OR IGNORE INTO blobs(sha256, content, size) VALUES (?, ?, ?)",
        (sha256, content, len(content)),
    )


@beartype
def get_blob(conn: sqlite3.Connection, sha256: str) -> bytes | None:
    row = conn.execute(
        "SELECT content FROM blobs WHERE sha256 = ?", (sha256,)
    ).fetchone()
    if row is None:
        return None
    content = row["content"]
    return bytes(content) if isinstance(content, (bytes, bytearray)) else None


@beartype
def blob_size(conn: sqlite3.Connection, sha256: str) -> int:
    row = conn.execute("SELECT size FROM blobs WHERE sha256 = ?", (sha256,)).fetchone()
    return 0 if row is None else int(row["size"])


@beartype
def add_hunk(
    conn: sqlite3.Connection,
    commit_sha: str,
    path: str,
    added: str,
    removed: str,
    old_start: int = 0,
    old_count: int = 0,
    new_start: int = 0,
    new_count: int = 0,
) -> None:
    """Один реальный hunk диффа с диапазоном строк ``@@ -a,b +c,d @@``."""
    conn.execute(
        (
            "INSERT INTO hunks(commit_sha, path, old_start, old_count, new_start, "
            "new_count, added, removed) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        (commit_sha, path, old_start, old_count, new_start, new_count, added, removed),
    )
    conn.execute(
        "INSERT INTO search(commit_sha, path, kind, body) VALUES (?, ?, ?, ?)",
        (commit_sha, path, "hunk", f"{added}\n{removed}"),
    )


@beartype
def add_symbol(
    conn: sqlite3.Connection,
    path: str,
    sha256: str,
    name: str,
    kind: str,
    line: int,
    revision: str,
    end_line: int = 0,
    body: str = "",
) -> None:
    """Символ кода: определение/объявление/тип/макрос.

    В FTS-таблицу ``search`` пишется и имя, и тело символа (``kind='code'``),
    чтобы поиск по фрагменту исходника находил определение, а не только коммиты.
    """
    cursor = conn.execute(
        (
            "INSERT OR IGNORE INTO symbols(path, sha256, name, kind, line, end_line, revision) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        ),
        (path, sha256, name, kind, line, end_line, revision),
    )
    if cursor.rowcount == 0:
        return
    conn.execute(
        "INSERT INTO search(commit_sha, path, kind, body) VALUES (?, ?, ?, ?)",
        (revision, path, "symbol", name),
    )
    if body != "":
        conn.execute(
            "INSERT INTO search(commit_sha, path, kind, body) VALUES (?, ?, ?, ?)",
            (revision, path, "code", f"{name}\n{body}"),
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
def parents_map(conn: sqlite3.Connection) -> dict[str, list[str]]:
    rows = conn.execute("SELECT sha, parents FROM commits").fetchall()
    return {str(row["sha"]): str(row["parents"]).split() for row in rows}


@beartype
def commit_time(conn: sqlite3.Connection, sha: str) -> int | None:
    row = conn.execute(
        "SELECT author_time FROM commits WHERE sha = ?", (sha,)
    ).fetchone()
    if row is None:
        return None
    return int(row["author_time"])


@beartype
def parent_of(conn: sqlite3.Connection, sha: str) -> str:
    """Первый родитель коммита: используется как parent graph при отсечке."""
    parents = parents_map(conn).get(sha, [])
    return parents[0] if parents else ""


@beartype
def descendants(conn: sqlite3.Connection, sha: str) -> set[str]:
    """Все потомки ``sha`` в индексе (транзитивное замыкание по parents)."""
    children: dict[str, list[str]] = {}
    for child, parents in parents_map(conn).items():
        for parent in parents:
            children.setdefault(parent, []).append(child)
    found: set[str] = set()
    stack = list(children.get(sha, []))
    while stack:
        current = stack.pop()
        if current in found:
            continue
        found.add(current)
        stack.extend(children.get(current, []))
    return found


@beartype
def ancestors(conn: sqlite3.Connection, sha: str) -> set[str]:
    """Все предки ``sha`` в индексе (сам ``sha`` не включается)."""
    parents = parents_map(conn)
    found: set[str] = set()
    stack = list(parents.get(sha, []))
    while stack:
        current = stack.pop()
        if current in found or current == "":
            continue
        found.add(current)
        stack.extend(parents.get(current, []))
    return found


@beartype
def revision_sha256(conn: sqlite3.Connection, commit_sha: str, path: str) -> str:
    """SHA-256 файла на коммите, если он менялся именно там."""
    row = conn.execute(
        "SELECT sha256 FROM revisions WHERE commit_sha = ? AND path = ?",
        (commit_sha, path),
    ).fetchone()
    return "" if row is None else str(row["sha256"])


@beartype
def resolve_sha256(
    conn: sqlite3.Connection,
    revision: str,
    path: str,
    max_depth: int = 20000,
) -> str:
    """SHA-256 файла на ревизии: идёт назад по предкам до ближайшего изменения.

    ``revisions`` хранит только изменившиеся пути, поэтому для произвольного
    предка файл берётся из ближайшего коммита, где он менялся.
    """
    parents = parents_map(conn)
    seen: set[str] = set()
    stack = [revision]
    depth = 0
    while stack and depth < max_depth:
        current = stack.pop()
        depth += 1
        if current in seen or current == "":
            continue
        seen.add(current)
        digest = revision_sha256(conn, current, path)
        if digest != "":
            return digest
        stack.extend(parents.get(current, []))
    return ""


@beartype
def _is_revert(message: str) -> bool:
    first = message.splitlines()[0].lower() if message else ""
    return first.startswith(("revert", "back out", "backout", "undo "))


@beartype
def count_rows(path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    with closing(open_store(path)) as conn:
        for table in ("commits", "revisions", "hunks", "symbols", "blobs"):
            counts[table] = int(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
    return counts
