"""Три MCP-инструмента графа с отсечкой.

``find_code_symbols`` — поиск символов C-кода по имени на ревизиях до cutoff.
``read_versioned_source`` — байты файла на закреплённой ревизии с SHA-256,
byte span и cutoff metadata.
``search_git_history`` — поиск по сообщениям коммитов и hunks.

Каждый ответ несёт revision, path, SHA-256 и cutoff metadata. Любая revision
обязана проходить ``Cutoff.allows``; ``head_sha`` и его потомки никогда не
выдаются. Нулевой результат пишется в audit log.
"""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, datetime

from beartype import beartype

from db_pipeline.benchmark.graph import store
from db_pipeline.benchmark.graph.cutoff import Cutoff


@beartype
def _now() -> str:
    return datetime.now(UTC).isoformat()


@beartype
def _audit(conn: sqlite3.Connection, tool: str, args: str, empty: bool) -> None:
    store.log_audit(conn, tool, args, empty, ts=_now())


@beartype
def _cutoff_meta(cutoff: Cutoff) -> dict[str, object]:
    return {
        "base_sha": cutoff.base_sha,
        "base_time": cutoff.base_time,
        "ancestry_enforced": True,
    }


@beartype
def _allowed(conn: sqlite3.Connection, cutoff: Cutoff, sha: str) -> bool:
    row = conn.execute(
        "SELECT author_time FROM commits WHERE sha = ?", (sha,)
    ).fetchone()
    if row is None:
        return False
    return cutoff.allows(sha, int(row["author_time"]))


@beartype
def find_code_symbols(
    conn: sqlite3.Connection,
    cutoff: Cutoff,
    name: str,
    limit: int = 20,
) -> dict[str, object]:
    rows = conn.execute(
        (
            "SELECT s.name, s.kind, s.path, s.line, s.commit_sha, c.author_time "
            "FROM symbols s JOIN commits c ON c.sha = s.commit_sha "
            "WHERE s.name LIKE ? ORDER BY c.author_time DESC LIMIT ?"
        ),
        (f"%{name}%", limit * 4),
    ).fetchall()
    results: list[dict[str, object]] = []
    for row in rows:
        sha = str(row["commit_sha"])
        if not cutoff.allows(sha, int(row["author_time"])):
            continue
        results.append(
            {
                "name": row["name"],
                "kind": row["kind"],
                "path": row["path"],
                "line": row["line"],
                "revision": sha,
                "cutoff": _cutoff_meta(cutoff),
            }
        )
        if len(results) >= limit:
            break
    _audit(conn, "find_code_symbols", name, len(results) == 0)
    return {"results": results, "cutoff": _cutoff_meta(cutoff)}


@beartype
def read_versioned_source(
    conn: sqlite3.Connection,
    cutoff: Cutoff,
    revision: str,
    path: str,
) -> dict[str, object]:
    if not _allowed(conn, cutoff, revision):
        _audit(conn, "read_versioned_source", f"{revision}:{path}", True)
        return {
            "error": "cutoff",
            "reason": "revision вне cutoff или не предок base_sha",
            "results": [],
            "cutoff": _cutoff_meta(cutoff),
        }
    row = conn.execute(
        "SELECT blob_sha, sha256, size FROM files WHERE commit_sha = ? AND path = ?",
        (revision, path),
    ).fetchone()
    if row is None:
        _audit(conn, "read_versioned_source", f"{revision}:{path}", True)
        return {"error": "not_found", "results": [], "cutoff": _cutoff_meta(cutoff)}
    _audit(conn, "read_versioned_source", f"{revision}:{path}", False)
    return {
        "results": [
            {
                "revision": revision,
                "path": path,
                "blob_sha": row["blob_sha"],
                "sha256": row["sha256"],
                "size": row["size"],
                "byte_span": [0, int(row["size"])],
                "cutoff": _cutoff_meta(cutoff),
            }
        ],
        "cutoff": _cutoff_meta(cutoff),
    }


@beartype
def search_git_history(
    conn: sqlite3.Connection,
    cutoff: Cutoff,
    query: str,
    limit: int = 20,
) -> dict[str, object]:
    rows = conn.execute(
        (
            "SELECT commit_sha, path, kind, body FROM search WHERE search MATCH ? LIMIT ?"
        ),
        (_fts_query(query), limit * 4),
    ).fetchall()
    results: list[dict[str, object]] = []
    for row in rows:
        sha = str(row["commit_sha"])
        commit = conn.execute(
            "SELECT author_time FROM commits WHERE sha = ?", (sha,)
        ).fetchone()
        if commit is None or not cutoff.allows(sha, int(commit["author_time"])):
            continue
        results.append(
            {
                "revision": sha,
                "path": row["path"],
                "kind": row["kind"],
                "sha256": hashlib.sha256(str(row["body"]).encode()).hexdigest(),
                "snippet": str(row["body"])[:400],
                "cutoff": _cutoff_meta(cutoff),
            }
        )
        if len(results) >= limit:
            break
    _audit(conn, "search_git_history", query, len(results) == 0)
    return {"results": results, "cutoff": _cutoff_meta(cutoff)}


@beartype
def _fts_query(query: str) -> str:
    tokens = [token for token in query.replace('"', " ").split() if token != ""]
    if len(tokens) == 0:
        return '""'
    return " AND ".join(f'"{token}"' for token in tokens)
