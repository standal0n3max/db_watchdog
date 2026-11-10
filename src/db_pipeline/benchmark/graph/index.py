"""Построение локального индекса графа.

Индексируется публичная git-история PostgreSQL до cutoff: commits/messages,
ревизии файлов, hunks и символы C-кода. Revert-коммиты сохраняются как отдельные
записи. ``head_sha`` и его потомки в индекс не попадают: обход идёт от
``base_sha`` по первому родителю.

Полный mirror и SQLite-индекс лежат в ``data/graph/`` и в git не попадают.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark import paths
from db_pipeline.benchmark.graph import store

_FUNC_RE = re.compile(
    r"^[A-Za-z_][\w \t*]*\b([A-Za-z_]\w*)\s*\(",
    re.MULTILINE,
)


@beartype
def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout


@beartype
def reachable_commits(repo: Path, base_sha: str, limit: int) -> list[str]:
    output = git(repo, "rev-list", f"--max-count={limit}", base_sha)
    return [line.strip() for line in output.splitlines() if line.strip()]


@beartype
def build_index(
    data: Path | None = None,
    limit: int = 2000,
    base_sha: str = "",
) -> dict[str, object]:
    repo = paths.pg_clone(data)
    if not (repo / ".git").exists():
        return {"ok": False, "error": f"нет клона {repo}"}
    head = base_sha or git(repo, "rev-parse", "HEAD").strip()
    if head == "":
        return {"ok": False, "error": "пустой base_sha"}
    commits = reachable_commits(repo, head, limit)
    db = paths.graph_dir(data) / "postgres.sqlite"
    conn = store.open_store(db)
    try:
        for sha in commits:
            _index_commit(repo, conn, sha)
        conn.commit()
    finally:
        conn.close()
    return {
        "ok": True,
        "db": str(db),
        "base_sha": head,
        "commits": len(commits),
        "rows": store.count_rows(db),
    }


@beartype
def _index_commit(repo: Path, conn: object, sha: str) -> None:
    import sqlite3

    assert isinstance(conn, sqlite3.Connection)
    meta = git(repo, "show", "-s", "--format=%P%x09%at%x09%s", sha).strip()
    parents_part, _, rest = meta.partition("\t")
    time_part, _, subject = rest.partition("\t")
    body = git(repo, "show", "-s", "--format=%B", sha).strip()
    try:
        author_time = int(time_part)
    except ValueError:
        author_time = 0
    store.add_commit(conn, sha, parents_part.split(), body or subject, author_time)

    tree = git(repo, "ls-tree", "-r", sha)
    for line in tree.splitlines():
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue
        left, path = parts
        fields = left.split()
        if len(fields) < 3:
            continue
        mode, kind, blob = fields[0], fields[1], fields[2]
        if not _is_source(path, mode, kind):
            continue
        size, digest = _blob_stat(repo, blob)
        store.add_file(conn, sha, path, blob, digest, size, sha)
        if path.endswith(".c"):
            symbols = git(repo, "show", f"{sha}:{path}")
            _index_symbols(conn, sha, path, symbols)
    _index_hunks(repo, conn, sha)


@beartype
def _is_source(path: str, mode: str, kind: str) -> bool:
    if kind != "blob":
        return False
    if mode.startswith("120000"):
        return False
    return path.startswith(("src/", "contrib/")) and path.endswith(
        (".c", ".h", ".sql", ".out")
    )


@beartype
def _blob_stat(repo: Path, blob: str) -> tuple[int, str]:
    import hashlib

    result = subprocess.run(
        ["git", "-C", str(repo), "cat-file", "blob", blob],
        capture_output=True,
        check=False,
    )
    data = result.stdout if result.returncode == 0 else b""
    return len(data), hashlib.sha256(data).hexdigest()


@beartype
def _index_symbols(conn: object, sha: str, path: str, source: str) -> None:
    import sqlite3

    assert isinstance(conn, sqlite3.Connection)
    for match in _FUNC_RE.finditer(source):
        name = match.group(1)
        line = source.count("\n", 0, match.start()) + 1
        store.add_symbol(conn, sha, path, name, "function", line)


@beartype
def _index_hunks(repo: Path, conn: object, sha: str) -> None:
    parents = git(repo, "show", "-s", "--format=%P", sha).split()
    if not parents:
        return
    diff = git(repo, "diff", "--unified=0", parents[0], sha, "--", "src/", "contrib/")
    current_path = ""
    added: list[str] = []
    removed: list[str] = []
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            _flush_hunk(conn, sha, current_path, added, removed)
            added, removed = [], []
            current_path = line[len("+++ b/") :]
        elif line.startswith("+") and not line.startswith("+++"):
            added.append(line[1:])
        elif line.startswith("-") and not line.startswith("---"):
            removed.append(line[1:])
    _flush_hunk(conn, sha, current_path, added, removed)


@beartype
def _flush_hunk(
    conn: object,
    sha: str,
    path: str,
    added: list[str],
    removed: list[str],
) -> None:
    import sqlite3

    assert isinstance(conn, sqlite3.Connection)
    if path == "" or (not added and not removed):
        return
    store.add_hunk(conn, sha, path, "\n".join(added), "\n".join(removed))
