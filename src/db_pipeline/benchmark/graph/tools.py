"""Три MCP-инструмента графа с отсечкой.

``find_code_symbols`` — поиск символов C-кода по имени на ревизиях до cutoff:
сначала точное совпадение имени, затем подстрока, и определение предпочтительнее
прототипа.
``read_versioned_source`` — реальный фрагмент файла на закреплённой ревизии с
SHA-256, диапазоном строк и cutoff metadata.
``search_git_history`` — терпимый полнотекстовый поиск по сообщениям коммитов,
hunks и телам кода.

Каждый ответ несёт revision, path, SHA-256 и cutoff metadata. Любая revision
обязана проходить ``Cutoff.allows``; ``head_sha`` и его потомки никогда не
выдаются. Нулевой результат пишется в audit log.

Ревизии файлов хранятся только для изменившихся путей, поэтому SHA-256 и bytes
берутся из ближайшего предка, где файл менялся.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
from datetime import UTC, datetime

from beartype import beartype

from db_pipeline.benchmark.graph import store
from db_pipeline.benchmark.graph.cutoff import Cutoff

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


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
    if not cutoff.allows(sha, store.commit_time(conn, sha) or 0):
        return False
    return store.commit_time(conn, sha) is not None


@beartype
def find_code_symbols(
    conn: sqlite3.Connection,
    cutoff: Cutoff,
    name: str,
    limit: int = 20,
) -> dict[str, object]:
    """Ищет символ по имени: точное совпадение, затем подстрока.

    Возвращает определение (не прототип) с диапазоном ``line``/``end_line``,
    ``kind``, ``path``, ``revision`` и ``sha256``. Результаты дедуплицируются по
    последней ревизии ≤ cutoff: одна запись на ``(path, name)``.
    """
    exact = _candidate_rows(conn, name, exact=True)
    partial = _candidate_rows(conn, name, exact=False)
    results = _resolve_symbols(conn, cutoff, exact, partial, limit)
    _audit(conn, "find_code_symbols", name, len(results) == 0)
    return {"results": results, "cutoff": _cutoff_meta(cutoff)}


@beartype
def _candidate_rows(
    conn: sqlite3.Connection,
    name: str,
    exact: bool,
) -> list[sqlite3.Row]:
    if exact:
        query = (
            "SELECT name, kind, path, line, end_line, revision, sha256 FROM symbols "
            "WHERE name = ? LIMIT ?"
        )
        params: tuple[object, ...] = (name, 4000)
    else:
        escaped = name.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        query = (
            "SELECT name, kind, path, line, end_line, revision, sha256 FROM symbols "
            "WHERE name LIKE ? ESCAPE '\\' LIMIT ?"
        )
        params = (f"%{escaped}%", 4000)
    return conn.execute(query, params).fetchall()


@beartype
def _resolve_symbols(
    conn: sqlite3.Connection,
    cutoff: Cutoff,
    exact: list[sqlite3.Row],
    partial: list[sqlite3.Row],
    limit: int,
) -> list[dict[str, object]]:
    """Дедуплицирует кандидатов по ``(path, name)`` и оставляет свежую ревизию."""
    chosen: dict[tuple[str, str], sqlite3.Row] = {}
    for row in exact + partial:
        revision = str(row["revision"])
        if not cutoff.allows(revision, store.commit_time(conn, revision) or 0):
            continue
        key = (str(row["path"]), str(row["name"]))
        current = chosen.get(key)
        if current is None or _is_better(conn, row, current):
            chosen[key] = row
    ordered = sorted(
        chosen.values(),
        key=lambda row: (-(store.commit_time(conn, str(row["revision"])) or 0), _rank(row)),
    )
    results: list[dict[str, object]] = []
    for row in ordered[:limit]:
        results.append(_symbol_payload(row, cutoff))
    return results


@beartype
def _is_better(
    conn: sqlite3.Connection,
    candidate: sqlite3.Row,
    current: sqlite3.Row,
) -> bool:
    """Кандидат лучше, если ревизия свежее или это определение, а не прототип."""
    candidate_time = store.commit_time(conn, str(candidate["revision"])) or 0
    current_time = store.commit_time(conn, str(current["revision"])) or 0
    if candidate_time != current_time:
        return candidate_time > current_time
    return _rank(candidate) < _rank(current)


@beartype
def _rank(row: sqlite3.Row) -> int:
    """Определение (``end_line`` > ``line``) выше прототипа."""
    line = int(row["line"])
    end_line = int(row["end_line"]) if row["end_line"] is not None else line
    if end_line > line:
        return 0
    return 1


@beartype
def _symbol_payload(row: sqlite3.Row, cutoff: Cutoff) -> dict[str, object]:
    line = int(row["line"])
    end_line = int(row["end_line"]) if row["end_line"] is not None else line
    return {
        "name": row["name"],
        "kind": row["kind"],
        "path": row["path"],
        "line": line,
        "end_line": end_line,
        "revision": str(row["revision"]),
        "sha256": row["sha256"],
        "cutoff": _cutoff_meta(cutoff),
    }


@beartype
def read_versioned_source(
    conn: sqlite3.Connection,
    cutoff: Cutoff,
    revision: str,
    path: str,
    offset: int = 0,
    limit: int = 8192,
    start_line: int | None = None,
    end_line: int | None = None,
) -> dict[str, object]:
    """Возвращает фрагмент исходника на ревизии до cutoff.

    По умолчанию ``offset``/``limit`` — это байты (обратная совместимость). Если
    задан ``start_line``/``end_line``, они имеют приоритет и трактуются как
    номера строк 1-based: возвращаются целые строки в этом диапазоне.
    """
    if not _allowed(conn, cutoff, revision):
        _audit(conn, "read_versioned_source", f"{revision}:{path}", True)
        return {
            "error": "cutoff",
            "reason": "revision вне cutoff или не предок base_sha",
            "results": [],
            "cutoff": _cutoff_meta(cutoff),
        }
    digest = store.resolve_sha256(conn, revision, path)
    if digest == "":
        _audit(conn, "read_versioned_source", f"{revision}:{path}", True)
        return {"error": "not_found", "results": [], "cutoff": _cutoff_meta(cutoff)}
    content = store.get_blob(conn, digest)
    if content is None:
        _audit(conn, "read_versioned_source", f"{revision}:{path}", True)
        return {
            "error": "bytes_missing",
            "reason": "bytes исходника отсутствуют в blob-таблице",
            "results": [],
            "cutoff": _cutoff_meta(cutoff),
        }
    _audit(conn, "read_versioned_source", f"{revision}:{path}", False)
    fragment = _fragment(content, offset, limit, start_line, end_line)
    return {
        "results": [
            {
                "revision": revision,
                "path": path,
                "sha256": digest,
                "size": len(content),
                "start_line": fragment["start_line"],
                "end_line": fragment["end_line"],
                "total_lines": fragment["total_lines"],
                "byte_span": fragment["byte_span"],
                "content": fragment["content"],
                "cutoff": _cutoff_meta(cutoff),
            }
        ],
        "cutoff": _cutoff_meta(cutoff),
    }


@beartype
def _fragment(
    content: bytes,
    offset: int,
    limit: int,
    start_line: int | None,
    end_line: int | None,
) -> dict[str, object]:
    """Считает byte span и текст фрагмента: по строкам либо по байтам."""
    if start_line is None and end_line is None:
        start = max(0, min(offset, len(content)))
        end = max(start, min(start + max(0, limit), len(content)))
        text = content[start:end]
        first = _line_number(content, start)
        return {
            "start_line": first,
            "end_line": _line_number(content, max(start, end - 1)),
            "total_lines": content.count(b"\n") + 1,
            "byte_span": [start, end],
            "content": text.decode("utf-8", "replace"),
        }
    lines = content.splitlines(keepends=True)
    total = len(lines)
    first = max(1, start_line if start_line is not None else 1)
    last = end_line if end_line is not None else total
    if last < first:
        last = first
    last = min(last, total)
    start = _offset_of_line(lines, first)
    end = _offset_of_line(lines, last + 1) if last < total else len(content)
    text = content[start:end]
    return {
        "start_line": first,
        "end_line": last,
        "total_lines": total,
        "byte_span": [start, end],
        "content": text.decode("utf-8", "replace"),
    }


@beartype
def _line_number(content: bytes, offset: int) -> int:
    if offset <= 0:
        return 1
    return content.count(b"\n", 0, offset) + 1


@beartype
def _offset_of_line(lines: list[bytes], number: int) -> int:
    """Байтовое смещение начала строки ``number`` (1-based)."""
    if number <= 1:
        return 0
    total = 0
    for index in range(min(number - 1, len(lines))):
        total += len(lines[index])
    return total


@beartype
def search_git_history(
    conn: sqlite3.Connection,
    cutoff: Cutoff,
    query: str,
    limit: int = 20,
) -> dict[str, object]:
    """Терпимый поиск: OR по токенам с префиксами, ранжирование по bm25.

    Ищет по сообщениям коммитов, hunks, именам символов и телам кода, поэтому
    естественно-языковой запрос находит кандидатов, а не ноль строк.
    """
    match = _fts_query(query)
    rows = conn.execute(
        (
            "SELECT commit_sha, path, kind, body FROM search "
            "WHERE search MATCH ? ORDER BY bm25(search) LIMIT ?"
        ),
        (match, max(limit * 8, 40)),
    ).fetchall()
    results: list[dict[str, object]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        sha = str(row["commit_sha"])
        if not cutoff.allows(sha, store.commit_time(conn, sha) or 0):
            continue
        key = (sha, str(row["path"]), str(row["kind"]))
        if key in seen:
            continue
        seen.add(key)
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
    """Строит FTS5-запрос: OR по токенам с префиксным совпадением.

    Токены кавычат и добавляют ``*``, чтобы ``to_char`` и ``cross-type`` искались
    как префиксы, а многословный запрос не давал ноль из-за строгого ``AND``.
    """
    tokens = [token for token in _TOKEN_RE.findall(query) if token != ""]
    if len(tokens) == 0:
        return '"__no_match__"'
    parts = [f'"{token}"*' for token in tokens]
    return " OR ".join(parts)
