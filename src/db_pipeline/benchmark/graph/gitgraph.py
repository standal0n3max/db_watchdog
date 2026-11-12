"""Сборка единого ``graph.sqlite`` из локального клона PostgreSQL.

Факты ключуются ``commit_oid`` и ``blob_oid``. Стадии ``extract → trees →
symbols → derive → validate`` пропускают уже известные oid, поэтому новая
задача дописывает только свои коммиты и дерево базы. Повторная сборка с пустой
базы даёт те же хэши таблиц: порядок вставки следует ``git rev-list``.

Окно — последние 50 предков каждой базы: только для него пишутся ханки и
полнотекст ханков. Деревья, символы и строки ошибок хранятся для баз задач.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, cast

from beartype import beartype

from db_pipeline.benchmark import paths
from db_pipeline.benchmark.graph.symbols import (
    Symbol,
    error_messages,
    has_symbols,
    is_source_path,
    parse_hunk_header,
    scan_symbols,
)
from db_pipeline.errors import PipelineError

BUILDER_VERSION = "git-native-1"
SCHEMA_VERSION = 1
WINDOW_SIZE = 50
_CHUNK = 80

SCHEMA = """
CREATE TABLE IF NOT EXISTS commits (
    oid TEXT PRIMARY KEY,
    parents TEXT NOT NULL,
    author_time INTEGER NOT NULL,
    subject TEXT NOT NULL,
    message TEXT NOT NULL,
    patch_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS parents (
    oid TEXT NOT NULL,
    parent_oid TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (oid, ordinal)
);
CREATE INDEX IF NOT EXISTS parents_parent ON parents(parent_oid);
CREATE TABLE IF NOT EXISTS file_change (
    oid TEXT NOT NULL,
    path TEXT NOT NULL,
    status TEXT NOT NULL,
    blob_oid TEXT NOT NULL,
    PRIMARY KEY (oid, path)
);
CREATE INDEX IF NOT EXISTS file_change_path ON file_change(path);
CREATE TABLE IF NOT EXISTS tree_entries (
    commit_oid TEXT NOT NULL,
    path TEXT NOT NULL,
    blob_oid TEXT NOT NULL,
    PRIMARY KEY (commit_oid, path)
);
CREATE INDEX IF NOT EXISTS tree_entries_blob ON tree_entries(blob_oid);
CREATE TABLE IF NOT EXISTS symbols (
    blob_oid TEXT NOT NULL,
    path TEXT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    line INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    definition INTEGER NOT NULL,
    PRIMARY KEY (blob_oid, path, name, line)
);
CREATE INDEX IF NOT EXISTS symbols_name ON symbols(name);
CREATE TABLE IF NOT EXISTS symbol_changes (
    oid TEXT NOT NULL,
    path TEXT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    role TEXT NOT NULL,
    side TEXT NOT NULL,
    sym_line INTEGER NOT NULL,
    sym_end_line INTEGER NOT NULL,
    PRIMARY KEY (oid, path, name, side, sym_line)
);
CREATE INDEX IF NOT EXISTS symbol_changes_lookup ON symbol_changes(path, name);
CREATE TABLE IF NOT EXISTS hunks (
    oid TEXT NOT NULL,
    path TEXT NOT NULL,
    ranges TEXT NOT NULL,
    added TEXT NOT NULL,
    removed TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS blobs (
    blob_oid TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    content_zlib BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS error_strings (
    blob_oid TEXT NOT NULL,
    message TEXT NOT NULL,
    line INTEGER NOT NULL,
    PRIMARY KEY (blob_oid, message, line)
);
CREATE TABLE IF NOT EXISTS windows (
    base_oid TEXT NOT NULL,
    oid TEXT NOT NULL,
    PRIMARY KEY (base_oid, oid)
);
CREATE TABLE IF NOT EXISTS stage_done (
    stage TEXT NOT NULL,
    oid TEXT NOT NULL,
    PRIMARY KEY (stage, oid)
);
CREATE TABLE IF NOT EXISTS manifest (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE VIRTUAL TABLE IF NOT EXISTS commits_fts USING fts5(
    oid UNINDEXED, subject, message, tokenize='unicode61'
);
CREATE VIRTUAL TABLE IF NOT EXISTS hunks_fts USING fts5(
    oid UNINDEXED, path UNINDEXED, added, removed, tokenize='unicode61'
);
"""

_HASH_COLUMNS: dict[str, str] = {
    "commits": "oid, parents, author_time, subject, message, patch_id",
    "parents": "oid, parent_oid, ordinal",
    "file_change": "oid, path, status, blob_oid",
    "tree_entries": "commit_oid, path, blob_oid",
    "symbols": "blob_oid, path, name, kind, line, end_line, definition",
    "symbol_changes": "oid, path, name, kind, role, side, sym_line, sym_end_line",
    "hunks": "oid, path, ranges, added, removed",
    "blobs": "blob_oid, sha256, content_zlib",
    "error_strings": "blob_oid, message, line",
    "windows": "base_oid, oid",
}


@beartype
@dataclass
class _Hunk:
    old_start: int
    old_count: int
    new_start: int
    new_count: int
    added: list[str] = field(default_factory=list[str])
    removed: list[str] = field(default_factory=list[str])


@beartype
@dataclass
class _FileDiff:
    path: str
    hunks: list[_Hunk] = field(default_factory=list[_Hunk])


@beartype
def schema_sha256() -> str:
    """Хэш текста схемы: попадает в manifest и не зависит от данных."""
    return hashlib.sha256(SCHEMA.encode("utf-8")).hexdigest()


@beartype
def graph_db(data: Path | None = None) -> Path:
    """Путь единого файла графа."""
    return paths.graph_dir(data) / "graph.sqlite"


@beartype
def task_bases(data: Path | None = None) -> list[str]:
    """Уникальные ``base`` из oracle, по возрастанию."""
    found: set[str] = set()
    for path in sorted(paths.oracle_dir(data).glob("*.json")):
        raw: object = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            continue
        mapping = cast("dict[str, object]", raw)
        base = mapping.get("base")
        if isinstance(base, str) and base != "":
            found.add(base)
    return sorted(found)


@beartype
def connect(db: Path) -> sqlite3.Connection:
    """Открывает файл на запись и создаёт схему, если её ещё нет."""
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    return conn


@beartype
def finish(conn: sqlite3.Connection) -> None:
    """Сбрасывает WAL, чтобы файл читался с ``immutable=1`` одним куском."""
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.commit()


@beartype
def table_hashes(db: Path) -> dict[str, str]:
    """SHA-256 содержимого каждой фактической таблицы, в стабильном порядке."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        hashes: dict[str, str] = {}
        for table, columns in _HASH_COLUMNS.items():
            digest = hashlib.sha256()
            query = f"SELECT {columns} FROM {table} ORDER BY {columns}"
            for row in conn.execute(query):
                payload = repr(tuple(row)).encode("utf-8", "replace")
                digest.update(payload)
                digest.update(b"\n")
            hashes[table] = digest.hexdigest()
        return hashes
    finally:
        conn.close()


@beartype
def build(
    data: Path | None = None,
    db: Path | None = None,
    repo: Path | None = None,
    bases: list[str] | None = None,
    fetch: bool = False,
) -> dict[str, object]:
    """Полная или инкрементальная сборка. Повтор по тем же oid ничего не дублирует."""
    active_repo = paths.pg_clone(data) if repo is None else repo
    if not (active_repo / ".git").exists():
        raise PipelineError("graph", f"нет клона {active_repo}")
    if fetch:
        _git(active_repo, ["fetch", "origin", "master"])
    chosen = task_bases(data) if bases is None else list(bases)
    if len(chosen) == 0:
        raise PipelineError("graph", "нет баз задач")
    target = graph_db(data) if db is None else db
    conn = connect(target)
    try:
        commits = _rev_list(active_repo, chosen)
        _log(f"коммитов {len(commits)}, баз {len(chosen)}")
        _refresh_windows(conn, active_repo, chosen)
        _stage_extract(conn, active_repo, commits)
        _stage_trees(conn, active_repo, chosen)
        _stage_symbols(conn, active_repo)
        _stage_derive(conn, active_repo)
        manifest = _stage_validate(conn, chosen)
        finish(conn)
    finally:
        conn.close()
    counts = _counts(target)
    return {"ok": True, "db": str(target), "counts": counts, "manifest": manifest}


@beartype
@dataclass(frozen=True)
class TreeEntry:
    """Одна запись ``ls-tree``: путь, режим, тип и blob."""

    path: str
    mode: str
    kind: str
    blob_sha: str
    size: int


@beartype
def list_tree(repo: Path, sha: str) -> list[TreeEntry]:
    """Дерево коммита одним ``ls-tree``. Нужно разовой сверке ``verify_a``."""
    result = _git(repo, ["ls-tree", "--long", "-r", sha])
    entries: list[TreeEntry] = []
    for raw in result.stdout.splitlines():
        line = raw.decode("utf-8", "replace")
        left, separator, path = line.partition("\t")
        if separator == "":
            continue
        fields = left.split()
        if len(fields) < 4:
            continue
        mode, kind, blob, size_text = fields[0], fields[1], fields[2], fields[3]
        try:
            size = int(size_text)
        except ValueError:
            size = 0
        entries.append(TreeEntry(path=path, mode=mode, kind=kind, blob_sha=blob, size=size))
    return entries


@beartype
def _counts(db: Path) -> dict[str, int]:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        found: dict[str, int] = {}
        for table in _HASH_COLUMNS:
            row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
            found[table] = 0 if row is None else int(row[0])
        return found
    finally:
        conn.close()


@beartype
def _log(text: str) -> None:
    print(text, file=sys.stderr)


@beartype
def _git(repo: Path, args: list[str], data: bytes | None = None) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", "-C", str(repo), "-c", "core.quotepath=false", *args],
        input=data,
        capture_output=True,
        check=False,
    )


@beartype
def _rev_list(repo: Path, bases: list[str]) -> list[str]:
    result = _git(repo, ["rev-list", "--stdin"], ("\n".join(bases) + "\n").encode())
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace")[:400]
        raise PipelineError("graph", f"rev-list: {detail}")
    seen: list[str] = []
    known: set[str] = set()
    for line in result.stdout.splitlines():
        oid = line.decode().strip()
        if oid == "" or oid in known:
            continue
        known.add(oid)
        seen.append(oid)
    return seen


@beartype
def _window(repo: Path, base: str) -> list[str]:
    result = _git(repo, ["rev-list", f"--max-count={WINDOW_SIZE}", base])
    return [line.decode().strip() for line in result.stdout.splitlines() if line.strip()]


@beartype
def _refresh_windows(conn: sqlite3.Connection, repo: Path, bases: list[str]) -> None:
    conn.execute("DELETE FROM windows")
    rows: list[tuple[str, str]] = []
    for base in bases:
        for oid in _window(repo, base):
            rows.append((base, oid))
    conn.executemany("INSERT OR IGNORE INTO windows(base_oid, oid) VALUES (?, ?)", rows)
    conn.commit()


@beartype
def _done(conn: sqlite3.Connection, stage: str) -> set[str]:
    rows = conn.execute("SELECT oid FROM stage_done WHERE stage = ?", (stage,)).fetchall()
    return {str(row[0]) for row in rows}


@beartype
def _mark(conn: sqlite3.Connection, stage: str, oids: list[str]) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO stage_done(stage, oid) VALUES (?, ?)",
        [(stage, oid) for oid in oids],
    )


@beartype
def _stage_extract(conn: sqlite3.Connection, repo: Path, commits: list[str]) -> None:
    known = {str(row[0]) for row in conn.execute("SELECT oid FROM commits")}
    todo = [oid for oid in commits if oid not in known]
    _log(f"extract: новых {len(todo)}")
    for start in range(0, len(todo), _CHUNK):
        batch = todo[start : start + _CHUNK]
        metas = _read_commits(repo, batch)
        changes = _read_changes(repo, batch)
        patches = _read_patch_ids(repo, batch)
        for oid in batch:
            meta = metas.get(oid)
            if meta is None:
                continue
            parents, author_time, subject, message = meta
            conn.execute(
                "INSERT OR REPLACE INTO commits(oid, parents, author_time, subject, message, patch_id) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (oid, " ".join(parents), author_time, subject, message, patches.get(oid, "")),
            )
            conn.executemany(
                "INSERT OR REPLACE INTO parents(oid, parent_oid, ordinal) VALUES (?, ?, ?)",
                [(oid, parent, index) for index, parent in enumerate(parents)],
            )
            conn.execute(
                "INSERT INTO commits_fts(oid, subject, message) VALUES (?, ?, ?)",
                (oid, subject, message),
            )
            conn.executemany(
                "INSERT OR REPLACE INTO file_change(oid, path, status, blob_oid) VALUES (?, ?, ?, ?)",
                [
                    (oid, path, status, blob)
                    for path, status, blob in changes.get(oid, [])
                    if path.startswith(("src/", "contrib/"))
                ],
            )
        conn.commit()
        _log(f"extract {min(start + _CHUNK, len(todo))}/{len(todo)}")


@beartype
def _read_commits(
    repo: Path, oids: list[str]
) -> dict[str, tuple[list[str], int, str, str]]:
    """``oid → (parents, author_time, subject, message)`` одним ``cat-file``."""
    if len(oids) == 0:
        return {}
    result = _git(repo, ["cat-file", "--batch"], ("\n".join(oids) + "\n").encode())
    found: dict[str, tuple[list[str], int, str, str]] = {}
    data = result.stdout
    offset = 0
    while offset < len(data):
        end = data.find(b"\n", offset)
        if end < 0:
            break
        header = data[offset:end].decode("utf-8", "replace")
        offset = end + 1
        parts = header.split()
        if len(parts) < 3 or parts[1] == "missing":
            continue
        try:
            size = int(parts[2])
        except ValueError:
            continue
        body = data[offset : offset + size]
        offset += size + 1
        found[parts[0]] = _parse_commit(body)
    return found


@beartype
def _parse_commit(raw: bytes) -> tuple[list[str], int, str, str]:
    text = raw.decode("utf-8", "replace")
    header, _, body = text.partition("\n\n")
    parents: list[str] = []
    author_time = 0
    for line in header.splitlines():
        if line.startswith("parent "):
            parents.append(line[len("parent ") :].strip())
        elif line.startswith("author "):
            fields = line.split()
            if len(fields) >= 2:
                try:
                    author_time = int(fields[-2])
                except ValueError:
                    author_time = 0
    lines = body.splitlines()
    subject = lines[0] if lines else ""
    return parents, author_time, subject, body


@beartype
def _read_changes(repo: Path, oids: list[str]) -> dict[str, list[tuple[str, str, str]]]:
    """Изменения первого родителя под ``src/`` и ``contrib/``."""
    result = _git(
        repo,
        ["diff-tree", "-r", "--raw", "--root", "-M", "--stdin", "--", "src/", "contrib/"],
        ("\n".join(oids) + "\n").encode(),
    )
    found: dict[str, list[tuple[str, str, str]]] = {oid: [] for oid in oids}
    current = ""
    for raw_line in result.stdout.splitlines():
        line = raw_line.decode("utf-8", "replace")
        if line == "":
            continue
        if not line.startswith(":"):
            current = line.strip()
            found.setdefault(current, [])
            continue
        meta, _, path = line.partition("\t")
        fields = meta.split()
        if len(fields) < 5 or current == "":
            continue
        status = fields[4][:1]
        new_sha = fields[3]
        final = path.split("\t")[-1]
        blob = "" if status == "D" or set(new_sha) <= {"0"} else new_sha
        found[current].append((final, status, blob))
    return found


@beartype
def _read_patch_ids(repo: Path, oids: list[str]) -> dict[str, str]:
    """``git patch-id --stable`` для пачки коммитов."""
    if len(oids) == 0:
        return {}
    log = subprocess.Popen(
        [
            "git", "-C", str(repo), "-c", "core.quotepath=false",
            "log", "--no-walk", "--stdin", "--pretty=format:%H%n", "-p", "--no-color",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    patch = subprocess.Popen(
        ["git", "-C", str(repo), "patch-id", "--stable"],
        stdin=log.stdout,
        stdout=subprocess.PIPE,
    )
    if log.stdout is not None:
        log.stdout.close()
    if log.stdin is not None:
        log.stdin.write(("\n".join(oids) + "\n").encode())
        log.stdin.close()
    output = b"" if patch.stdout is None else patch.stdout.read()
    if patch.stdout is not None:
        patch.stdout.close()
    log.wait()
    patch.wait()
    found: dict[str, str] = {}
    for line in output.decode("utf-8", "replace").splitlines():
        parts = line.split()
        if len(parts) >= 2:
            found[parts[1]] = parts[0]
    return found


@beartype
def _stage_trees(conn: sqlite3.Connection, repo: Path, bases: list[str]) -> None:
    done = _done(conn, "trees")
    todo = [base for base in bases if base not in done]
    _log(f"trees: новых баз {len(todo)}")
    reader = _Cat(repo)
    stored = {str(row[0]) for row in conn.execute("SELECT blob_oid FROM blobs")}
    try:
        for index, base in enumerate(todo, start=1):
            entries = _ls_tree(repo, base)
            rows = [
                (base, entry[0], entry[1])
                for entry in entries
                if is_source_path(entry[0]) and entry[2] == "blob" and not entry[3].startswith("120")
            ]
            conn.executemany(
                "INSERT OR REPLACE INTO tree_entries(commit_oid, path, blob_oid) VALUES (?, ?, ?)",
                rows,
            )
            for item in rows:
                path, blob = item[1], item[2]
                if blob in stored:
                    continue
                _put_blob(conn, reader, blob, path)
                stored.add(blob)
            _mark(conn, "trees", [base])
            conn.commit()
            _log(f"trees {index}/{len(todo)} {base[:12]}")
    finally:
        reader.close()


@beartype
def _ls_tree(repo: Path, sha: str) -> list[tuple[str, str, str, str]]:
    """``(path, blob, kind, mode)`` всего дерева."""
    result = _git(repo, ["ls-tree", "--long", "-r", sha])
    entries: list[tuple[str, str, str, str]] = []
    for raw in result.stdout.splitlines():
        line = raw.decode("utf-8", "replace")
        left, separator, path = line.partition("\t")
        if separator == "":
            continue
        fields = left.split()
        if len(fields) < 4:
            continue
        mode, kind, blob = fields[0], fields[1], fields[2]
        entries.append((path, blob, kind, mode))
    return entries


@beartype
def _put_blob(conn: sqlite3.Connection, reader: _Cat, blob: str, path: str) -> None:
    if blob == "" or set(blob) <= {"0"}:
        return
    exists = conn.execute("SELECT 1 FROM blobs WHERE blob_oid = ?", (blob,)).fetchone()
    if exists is not None:
        return
    data = reader.read(blob)
    digest = hashlib.sha256(data).hexdigest()
    conn.execute(
        "INSERT INTO blobs(blob_oid, sha256, content_zlib) VALUES (?, ?, ?)",
        (blob, digest, zlib.compress(data, level=6)),
    )
    if path.endswith(".c"):
        text = data.decode("utf-8", "replace")
        conn.executemany(
            "INSERT OR IGNORE INTO error_strings(blob_oid, message, line) VALUES (?, ?, ?)",
            [(blob, message, line) for message, line in error_messages(text)],
        )


@beartype
class _Cat:
    """Долгоживущий ``git cat-file --batch``."""

    def __init__(self, repo: Path) -> None:
        self._proc = subprocess.Popen(
            ["git", "-C", str(repo), "cat-file", "--batch"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

    @beartype
    def read(self, oid: str) -> bytes:
        stdin = self._proc.stdin
        stdout = self._proc.stdout
        if stdin is None or stdout is None or oid == "":
            return b""
        stdin.write(f"{oid}\n".encode())
        stdin.flush()
        header = stdout.readline()
        if b"missing" in header:
            return b""
        parts = header.split()
        if len(parts) < 3:
            return b""
        try:
            size = int(parts[2])
        except ValueError:
            return b""
        content = stdout.read(size)
        stdout.read(1)
        return content

    @beartype
    def close(self) -> None:
        stdin = self._proc.stdin
        if stdin is not None and not stdin.closed:
            stdin.close()
        stdout = self._proc.stdout
        if stdout is not None and not stdout.closed:
            stdout.close()
        self._proc.wait(timeout=30)


@beartype
def _stage_symbols(conn: sqlite3.Connection, repo: Path) -> None:
    done = _done(conn, "symbols")
    rows = conn.execute(
        "SELECT DISTINCT blob_oid, path FROM tree_entries "
        "WHERE path LIKE '%.c' OR path LIKE '%.h' "
        "UNION SELECT fc.blob_oid, fc.path FROM file_change fc "
        "JOIN windows w ON w.oid = fc.oid "
        "WHERE (fc.path LIKE '%.c' OR fc.path LIKE '%.h') AND fc.blob_oid != ''"
    ).fetchall()
    pending = [(str(row[0]), str(row[1])) for row in rows if f"{row[0]} {row[1]}" not in done]
    _log(f"symbols: blob-путей {len(pending)}")
    reader = _Cat(repo)
    try:
        for index, (blob, path) in enumerate(pending, start=1):
            if not has_symbols(path):
                continue
            stored = conn.execute(
                "SELECT content_zlib FROM blobs WHERE blob_oid = ?", (blob,)
            ).fetchone()
            if stored is None:
                _put_blob(conn, reader, blob, path)
                stored = conn.execute(
                    "SELECT content_zlib FROM blobs WHERE blob_oid = ?", (blob,)
                ).fetchone()
            if stored is None:
                continue
            text = zlib.decompress(stored[0]).decode("utf-8", "replace")
            _insert_symbols(conn, blob, path, text.splitlines())
            _mark(conn, "symbols", [f"{blob} {path}"])
            if index % 200 == 0:
                conn.commit()
                _log(f"symbols {index}/{len(pending)}")
        conn.commit()
    finally:
        reader.close()


@beartype
def _insert_symbols(conn: sqlite3.Connection, blob: str, path: str, lines: list[str]) -> None:
    found = scan_symbols(path, lines)
    conn.executemany(
        "INSERT OR IGNORE INTO symbols(blob_oid, path, name, kind, line, end_line, definition) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (
                blob,
                path,
                symbol.name,
                symbol.kind,
                symbol.line,
                symbol.end_line,
                1 if symbol.definition else 0,
            )
            for symbol in found
        ],
    )


@beartype
def _stage_derive(conn: sqlite3.Connection, repo: Path) -> None:
    symbols_done = _done(conn, "changes")
    hunks_done = _done(conn, "hunks")
    window = {str(row[0]) for row in conn.execute("SELECT oid FROM windows")}
    commits = [str(row[0]) for row in conn.execute("SELECT oid FROM commits")]
    todo = [
        oid
        for oid in commits
        if oid not in symbols_done or (oid in window and oid not in hunks_done)
    ]
    _log(f"derive: коммитов {len(todo)}")
    for start in range(0, len(todo), _CHUNK):
        batch = todo[start : start + _CHUNK]
        blobs = _read_blob_sides(repo, batch)
        _walk_diffs(conn, repo, batch, blobs, window, symbols_done, hunks_done)
        conn.commit()
        _log(f"derive {min(start + _CHUNK, len(todo))}/{len(todo)}")


@beartype
def _read_blob_sides(repo: Path, oids: list[str]) -> dict[tuple[str, str], tuple[str, str]]:
    """``(commit, path) → (old_blob, new_blob)``."""
    result = _git(
        repo,
        ["diff-tree", "-r", "--raw", "--root", "-M", "--stdin", "--", "src/", "contrib/"],
        ("\n".join(oids) + "\n").encode(),
    )
    found: dict[tuple[str, str], tuple[str, str]] = {}
    current = ""
    for raw_line in result.stdout.splitlines():
        line = raw_line.decode("utf-8", "replace")
        if line == "":
            continue
        if not line.startswith(":"):
            current = line.strip()
            continue
        meta, _, path = line.partition("\t")
        fields = meta.split()
        if len(fields) < 5:
            continue
        old_sha, new_sha = fields[2], fields[3]
        final = path.split("\t")[-1]
        old_blob = "" if set(old_sha) <= {"0"} else old_sha
        new_blob = "" if set(new_sha) <= {"0"} else new_sha
        found[(current, final)] = (old_blob, new_blob)
    return found


@beartype
def _walk_diffs(
    conn: sqlite3.Connection,
    repo: Path,
    oids: list[str],
    blobs: dict[tuple[str, str], tuple[str, str]],
    window: set[str],
    symbols_done: set[str],
    hunks_done: set[str],
) -> None:
    log = subprocess.Popen(
        [
            "git", "-C", str(repo), "-c", "core.quotepath=false",
            "log", "--no-walk", "--stdin", "-U0", "--pretty=format:COMMIT %H %P",
            "--no-color", "--", "src/", "contrib/",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    if log.stdin is not None:
        log.stdin.write(("\n".join(oids) + "\n").encode())
        log.stdin.close()
    stdout = log.stdout
    if stdout is None:
        log.wait()
        return
    reader = _Cat(repo)
    cache: dict[str, list[Symbol]] = {}
    seen: set[str] = set()
    try:
        _consume_diffs(
            conn, stdout, blobs, window, symbols_done, hunks_done, reader, cache, seen
        )
    finally:
        reader.close()
        if log.stdout is not None and not log.stdout.closed:
            log.stdout.close()
        log.wait()
    missing = [oid for oid in oids if oid not in seen]
    for oid in missing:
        if oid not in symbols_done:
            _mark(conn, "changes", [oid])
        if oid in window and oid not in hunks_done:
            _mark(conn, "hunks", [oid])


@beartype
def _consume_diffs(
    conn: sqlite3.Connection,
    stdout: IO[bytes],
    blobs: dict[tuple[str, str], tuple[str, str]],
    window: set[str],
    symbols_done: set[str],
    hunks_done: set[str],
    reader: _Cat,
    cache: dict[str, list[Symbol]],
    seen: set[str],
) -> None:
    current = ""
    path = ""
    hunk: _Hunk | None = None
    files: list[_FileDiff] = []
    active: _FileDiff | None = None

    def flush_hunk() -> None:
        nonlocal hunk
        if hunk is not None and active is not None and (hunk.added or hunk.removed):
            active.hunks.append(hunk)
        hunk = None

    def flush_file() -> None:
        nonlocal active
        flush_hunk()
        if active is not None and active.path != "":
            files.append(active)
        active = None

    def flush_commit() -> None:
        nonlocal files
        flush_file()
        if current == "":
            return
        seen.add(current)
        _apply_commit(
            conn, current, files, blobs, window, symbols_done, hunks_done, reader, cache
        )
        files = []

    for raw in stdout:
        line = raw.decode("utf-8", "replace").rstrip("\n")
        if line.startswith("COMMIT "):
            flush_commit()
            parts = line.split()
            current = parts[1] if len(parts) > 1 else ""
            path = ""
            continue
        if line.startswith("diff --git "):
            flush_file()
            marker = " b/"
            index = line.rfind(marker)
            path = line[index + len(marker) :] if index >= 0 else ""
            active = _FileDiff(path=path)
            continue
        if line.startswith("@@"):
            flush_hunk()
            old_start, old_count, new_start, new_count = parse_hunk_header(line)
            hunk = _Hunk(old_start, old_count, new_start, new_count)
            continue
        if hunk is None:
            continue
        if line.startswith("+") and not line.startswith("+++"):
            hunk.added.append(line[1:])
        elif line.startswith("-") and not line.startswith("---"):
            hunk.removed.append(line[1:])
    flush_commit()


@beartype
def _apply_commit(
    conn: sqlite3.Connection,
    oid: str,
    files: list[_FileDiff],
    blobs: dict[tuple[str, str], tuple[str, str]],
    window: set[str],
    symbols_done: set[str],
    hunks_done: set[str],
    reader: _Cat,
    cache: dict[str, list[Symbol]],
) -> None:
    want_symbols = oid not in symbols_done
    want_hunks = oid in window and oid not in hunks_done
    symbol_rows: list[tuple[str, str, str, str, str, str, int, int]] = []
    hunk_rows: list[tuple[str, str, str, str, str]] = []
    for item in files:
        if want_hunks:
            for hunk in item.hunks:
                ranges = f"{hunk.old_start},{hunk.old_count},{hunk.new_start},{hunk.new_count}"
                hunk_rows.append(
                    (oid, item.path, ranges, "\n".join(hunk.added), "\n".join(hunk.removed))
                )
        if not want_symbols or not has_symbols(item.path):
            continue
        old_blob, new_blob = blobs.get((oid, item.path), ("", ""))
        old_symbols = _cached_symbols(reader, cache, old_blob, item.path)
        new_symbols = _cached_symbols(reader, cache, new_blob, item.path)
        for hunk in item.hunks:
            symbol_rows.extend(
                _hit_symbols(oid, item.path, old_symbols, "old", hunk.old_start, hunk.old_count)
            )
            symbol_rows.extend(
                _hit_symbols(oid, item.path, new_symbols, "new", hunk.new_start, hunk.new_count)
            )
    if want_symbols:
        conn.executemany(
            "INSERT OR IGNORE INTO symbol_changes("
            "oid, path, name, kind, role, side, sym_line, sym_end_line) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            symbol_rows,
        )
        _mark(conn, "changes", [oid])
        symbols_done.add(oid)
    if want_hunks:
        conn.executemany(
            "INSERT INTO hunks(oid, path, ranges, added, removed) VALUES (?, ?, ?, ?, ?)",
            hunk_rows,
        )
        conn.executemany(
            "INSERT INTO hunks_fts(oid, path, added, removed) VALUES (?, ?, ?, ?)",
            [(oid, path, added, removed) for oid, path, _ranges, added, removed in hunk_rows],
        )
        _mark(conn, "hunks", [oid])
        hunks_done.add(oid)


@beartype
def _cached_symbols(
    reader: _Cat, cache: dict[str, list[Symbol]], blob: str, path: str
) -> list[Symbol]:
    if blob == "":
        return []
    cached = cache.get(blob)
    if cached is not None:
        return cached
    text = reader.read(blob).decode("utf-8", "replace")
    found = scan_symbols(path, text.splitlines())
    if len(cache) > 4000:
        cache.clear()
    cache[blob] = found
    return found


@beartype
def _hit_symbols(
    oid: str,
    path: str,
    symbols: list[Symbol],
    side: str,
    start: int,
    count: int,
) -> list[tuple[str, str, str, str, str, str, int, int]]:
    rows: list[tuple[str, str, str, str, str, str, int, int]] = []
    for symbol in symbols:
        if not _overlaps(symbol.line, symbol.end_line, start, count):
            continue
        role = "definition" if symbol.definition else "declaration"
        rows.append(
            (oid, path, symbol.name, symbol.kind, role, side, symbol.line, symbol.end_line)
        )
    return rows


@beartype
def _overlaps(line: int, end_line: int, start: int, count: int) -> bool:
    if start <= 0:
        return False
    if count <= 0:
        return line <= start <= end_line
    last = start + count - 1
    return line <= last and start <= end_line


@beartype
def _stage_validate(conn: sqlite3.Connection, bases: list[str]) -> dict[str, str]:
    counts = {
        table: int(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
        for table in ("commits", "tree_entries", "symbols", "blobs", "error_strings")
    }
    manifest = {
        "builder": BUILDER_VERSION,
        "schema_version": str(SCHEMA_VERSION),
        "schema_sha256": schema_sha256(),
        "bases": "\n".join(bases),
        "counts": json.dumps(counts, sort_keys=True),
    }
    conn.executemany(
        "INSERT OR REPLACE INTO manifest(key, value) VALUES (?, ?)",
        list(manifest.items()),
    )
    conn.commit()
    return manifest
