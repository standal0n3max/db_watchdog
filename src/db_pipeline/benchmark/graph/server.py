"""MCP-сервер графа: семь инструментов и одна отсечка GraphView.

Имена и сигнатуры инструментов те же, что у прежних слоёв v1 и v2. Отсечка
считается на старте по ``parents`` и больше не откладывается: файл
``graph.sqlite`` открывается ``mode=ro&immutable=1``.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from beartype import beartype
from mcp.server.mcpserver import MCPServer

from db_pipeline.benchmark import paths
from db_pipeline.benchmark.graph import view as graph_view
from db_pipeline.benchmark.graph.view import GraphView


@beartype
def _db_path() -> Path:
    raw = os.environ.get("BENCH_GRAPH_DB", "")
    if raw != "":
        return Path(raw)
    return paths.graph_dir() / "graph.sqlite"


@beartype
def _wait_for_db(db: Path, timeout: int = 180) -> bool:
    """Ждёт, пока ``docker cp`` допишет файл: размер должен стабилизироваться."""
    deadline = time.monotonic() + timeout
    last = -1
    stable = 0
    while time.monotonic() < deadline:
        if db.is_file():
            size = db.stat().st_size
            if size > 0 and size == last:
                stable += 1
                if stable >= 2:
                    return True
            else:
                stable = 0
            last = size
        time.sleep(1)
    return db.is_file()


@beartype
def serve() -> None:
    db = _db_path()
    _wait_for_db(db)
    base = os.environ.get("BENCH_GRAPH_BASE_SHA", "")
    head = os.environ.get("BENCH_GRAPH_HEAD_SHA", "")
    opened = graph_view.open_view(db, base, head)
    server = MCPServer("postgres-graph")
    _register(server, opened)
    port = int(os.environ.get("BENCH_GRAPH_PORT", "8765"))
    path = os.environ.get("BENCH_GRAPH_PATH", "/mcp")
    server.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
        streamable_http_path=path,
        json_response=True,
    )


@beartype
def _register(server: MCPServer, opened: GraphView) -> None:
    @server.tool()
    def find_code_symbols(name: str, limit: int = 10) -> str:
        """Locate C symbols by name. Call this tool as ``graph_find_code_symbols``.

        ``name`` is a bare identifier such as ``heap_insert`` (not a path).
        Every revision is an ancestor of base_sha. ``limit`` caps the hits.
        """
        return graph_view.compact_text(graph_view.find_code_symbols(opened, name, limit))

    @server.tool()
    def read_versioned_source(
        revision: str,
        path: str,
        offset: int = 0,
        limit: int = 8192,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        """Read a source fragment at a pinned revision. Call it as
        ``graph_read_versioned_source``.

        ``revision`` must be an ancestor of base_sha. ``path`` is
        repository-relative. ``start_line``/``end_line`` (1-based) take priority
        and return at most about 400 lines; a longer span sets ``truncated``.
        """
        return graph_view.compact_text(
            graph_view.read_versioned_source(
                opened, revision, path, offset, limit, start_line, end_line
            )
        )

    @server.tool()
    def search_git_history(query: str, limit: int = 10) -> str:
        """Search commit messages and hunks. Call this tool as
        ``graph_search_git_history``.

        ``query`` is free text. Results stay within ancestors of base_sha.
        """
        return graph_view.compact_text(graph_view.search_git_history(opened, query, limit))

    @server.tool()
    def symbol_history(path: str, name: str, limit: int = 10) -> str:
        """Show how one function changed over time. Call this tool as
        ``graph_symbol_history``.

        ``path`` is the file, ``name`` is the bare symbol. Each record carries
        the symbol line range and whether it is a definition. Nothing after
        the cutoff is returned.
        """
        return graph_view.compact_text(graph_view.symbol_history(opened, path, name, limit))

    @server.tool()
    def tests_to_code(test: str, limit: int = 10) -> str:
        """Map a regression test to the code it exercises. Call this tool as
        ``graph_tests_to_code``.

        ``test`` is a repository-relative test path. Results are ranked by
        co-change weight and limited to ancestors of base_sha.
        """
        return graph_view.compact_text(graph_view.tests_to_code(opened, test, limit))

    @server.tool()
    def similar_fixes(query: str, limit: int = 10) -> str:
        """Find past commits with a similar intent. Call this tool as
        ``graph_similar_fixes``.

        ``query`` is free text, for example ``fix heap corruption on concurrent
        insert``. Only ancestors of base_sha are searched.
        """
        return graph_view.compact_text(graph_view.similar_fixes(opened, query, limit))

    @server.tool()
    def find_error_origin(message: str, limit: int = 10) -> str:
        """Locate the code that prints an error message. Call this tool as
        ``graph_find_error_origin``.

        ``message`` is the literal string, for example ``cache lookup failed``.
        Placeholders such as ``%s`` are normalized. Results stay at base_sha.
        """
        return graph_view.compact_text(graph_view.find_error_origin(opened, message, limit))


if __name__ == "__main__":
    serve()
