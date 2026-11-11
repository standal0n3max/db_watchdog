"""MCP-клиент графа для дешёвой пробы: запускается внутри internal-сети.

Скрипт вызывается оркестратором через ``docker exec`` в самом graph-container,
поэтому проверяет ровно тот путь, которым пользуется task-container: тот же
internal DNS, тот же Streamable HTTP endpoint. Публикация порта на хост не нужна:
Docker не публикует порты контейнеров во внутренней сети.

Клиент делает ``initialize``, ``list_tools``, по одному вызову каждого из трёх
инструментов и проверяет отказ по запрещённому ``head_sha``. Итог печатается
одной JSON-строкой в stdout; при ошибке возвращается ненулевой код.
"""

from __future__ import annotations

import asyncio
import json
import sys
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING, cast

from beartype import beartype

from db_pipeline.benchmark.graph import store

if TYPE_CHECKING:
    from mcp import ClientSession

EXPECTED_TOOLS = ["find_code_symbols", "read_versioned_source", "search_git_history"]
EXPECTED_TOOLS_V2 = [
    "find_code_symbols",
    "read_versioned_source",
    "search_git_history",
    "symbol_history",
    "tests_to_code",
    "similar_fixes",
    "find_error_origin",
]
# ``list_tools`` возвращает имена отсортированными, поэтому сравниваем именно с
# сортированным списком: иначе проверка падала бы на порядке, а не на составе.
EXPECTED_TOOLS_V2_SORTED = sorted(EXPECTED_TOOLS_V2)


@beartype
def _first_symbol(db: Path, revision: str) -> str:
    with closing(store.open_store(db)) as conn:
        row = conn.execute(
            "SELECT name FROM symbols WHERE revision = ? LIMIT 1", (revision,)
        ).fetchone()
        if row is None:
            row = conn.execute("SELECT name FROM symbols LIMIT 1").fetchone()
    return "" if row is None else str(row["name"])


@beartype
def _first_revision_path(db: Path, revision: str) -> tuple[str, str]:
    """Возвращает (revision, path) с реальной ревизией C-файла в индексе."""
    with closing(store.open_store(db)) as conn:
        row = conn.execute(
            "SELECT commit_sha, path FROM revisions WHERE path LIKE 'src/%.c' "
            "AND commit_sha = ? LIMIT 1",
            (revision,),
        ).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT commit_sha, path FROM revisions WHERE path LIKE 'src/%.c' LIMIT 1"
            ).fetchone()
    if row is None:
        return revision, ""
    return str(row["commit_sha"]), str(row["path"])


@beartype
def _text(result: object) -> str:
    content = getattr(result, "content", None)
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in cast("list[object]", content):
        text = getattr(item, "text", None)
        if isinstance(text, str):
            parts.append(text)
    return "\n".join(parts)[:4000]


@beartype
async def run_cutoff_check(url: str, head_sha: str) -> dict[str, object]:
    """Проверяет только отказ по запрещённому ``head_sha``."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    report: dict[str, object] = {"url": url, "head_sha": head_sha}
    async with streamable_http_client(url) as streams:
        read_stream, write_stream = streams[0], streams[1]
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            refused = await session.call_tool(
                "read_versioned_source",
                {"revision": head_sha, "path": "src/backend/x.c"},
            )
            report["head_refusal"] = _text(refused)
    report["ok"] = "cutoff" in str(report.get("head_refusal"))
    return report


@beartype
async def run_probe(url: str, db: Path, base_sha: str, head_sha: str) -> dict[str, object]:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    report: dict[str, object] = {"url": url, "base_sha": base_sha, "head_sha": head_sha}
    async with streamable_http_client(url) as streams:
        read_stream, write_stream = streams[0], streams[1]
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            tools = await session.list_tools()
            tool_names = sorted(tool.name for tool in tools.tools)
            report["tools"] = tool_names
            symbol = _first_symbol(db, base_sha)
            revision, path = _first_revision_path(db, base_sha)
            report["symbol"] = symbol
            report["revision"] = revision
            report["path"] = path
            if symbol != "":
                found = await session.call_tool("find_code_symbols", {"name": symbol})
                report["find_code_symbols"] = _text(found)
            if path != "":
                source = await session.call_tool(
                    "read_versioned_source", {"revision": revision, "path": path}
                )
                report["read_versioned_source"] = _text(source)
            history = await session.call_tool("search_git_history", {"query": "fix", "limit": 3})
            report["search_git_history"] = _text(history)
            if "symbol_history" in tool_names:
                report.update(await _probe_v2(session))
            refused = await session.call_tool(
                "read_versioned_source",
                {"revision": head_sha, "path": path or "src/backend/x.c"},
            )
            report["head_refusal"] = _text(refused)
    v2_ok = True
    if "symbol_history" in tool_names:
        v2_ok = all(
            "results" in str(report.get(name))
            for name in (
                "symbol_history",
                "tests_to_code",
                "similar_fixes",
                "find_error_origin",
            )
        )
    report["ok"] = (
        v2_ok
        and tool_names in (EXPECTED_TOOLS, EXPECTED_TOOLS_V2_SORTED)
        and "results" in str(report.get("find_code_symbols"))
        and "content" in str(report.get("read_versioned_source"))
        and "cutoff" in str(report.get("head_refusal"))
    )
    return report


@beartype
async def _probe_v2(session: object) -> dict[str, object]:
    """По одному вызову каждого инструмента v2: форма ответа, а не качество.

    ``session`` принимается как ``object``: ``ClientSession`` импортируется
    только под ``TYPE_CHECKING``, и beartype не может разрешить строковую
    аннотацию в рантайме. Внутри тип восстанавливается ``cast``.
    """
    active = cast("ClientSession", session)
    report: dict[str, object] = {}
    history = await active.call_tool(
        "symbol_history", {"path": "src/backend/executor/nodeHashjoin.c", "name": "ExecInitHashJoin"}
    )
    report["symbol_history"] = _text(history)
    tests = await active.call_tool(
        "tests_to_code", {"test": "src/test/regress/sql/aggregates.sql"}
    )
    report["tests_to_code"] = _text(tests)
    similar = await active.call_tool(
        "similar_fixes", {"query": "hash join", "limit": 3}
    )
    report["similar_fixes"] = _text(similar)
    origin = await active.call_tool(
        "find_error_origin", {"message": "could not open file"}
    )
    report["find_error_origin"] = _text(origin)
    return report


@beartype
def main(argv: list[str]) -> int:
    if len(argv) == 2:
        # Режим проверки только cutoff: URL HEAD_SHA.
        url, head_sha = argv
        report = asyncio.run(run_cutoff_check(url, head_sha))
        print(json.dumps(report, ensure_ascii=False))
        return 0 if report.get("ok") else 1
    if len(argv) != 4:
        print("usage: probe_client URL DB BASE_SHA HEAD_SHA | probe_client URL HEAD_SHA", file=sys.stderr)
        return 2
    url, db_text, base_sha, head_sha = argv
    report = asyncio.run(run_probe(url, Path(db_text), base_sha, head_sha))
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
