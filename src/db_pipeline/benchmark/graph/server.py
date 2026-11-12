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
    def find_code_symbols(name: str, limit: int = 20) -> dict[str, object]:
        """Locate C symbols (functions, structs, macros) by name in history.

        Use this to find which files and revisions define or reference a symbol
        before editing. Call it as ``graph_find_code_symbols`` when the MCP
        server prefix is exposed, or as ``find_code_symbols`` when it is not:
        the tool name is always the plain one above.

        ``name`` is a bare identifier such as ``heap_insert`` (not a path).
        Returns matching revisions; every revision is an ancestor of base_sha,
        so nothing after the cutoff leaks. ``limit`` caps the number of hits.
        """
        return graph_view.find_code_symbols(opened, name, limit)

    @server.tool()
    def read_versioned_source(
        revision: str,
        path: str,
        offset: int = 0,
        limit: int = 8192,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> dict[str, object]:
        """Read a source file fragment at a pinned revision before the cutoff.

        Use this when you have a path and a revision (from find_code_symbols or
        search_git_history) and want the surrounding code. Call it as
        ``graph_read_versioned_source`` with the MCP prefix, or
        ``read_versioned_source`` without it: the tool name is the plain one.

        ``revision`` must be an ancestor of base_sha; any later revision is
        rejected. ``path`` is repository-relative (``src/backend/...``).
        ``offset``/``limit`` are bytes; ``start_line``/``end_line`` (1-based)
        take priority and return whole lines.
        """
        return graph_view.read_versioned_source(
            opened, revision, path, offset, limit, start_line, end_line
        )

    @server.tool()
    def search_git_history(query: str, limit: int = 20) -> dict[str, object]:
        """Search commit messages and hunks for a natural-language query.

        Use this to find when and why code changed, for example a bug-fix phrase
        from the task description. Call it as ``graph_search_git_history`` with
        the MCP prefix or ``search_git_history`` without it: the name is plain.

        ``query`` is free text; results are limited to ancestors of base_sha.
        """
        return graph_view.search_git_history(opened, query, limit)

    @server.tool()
    def symbol_history(path: str, name: str, limit: int = 20) -> dict[str, object]:
        """Reach for this when a function exists but its current shape does not
        explain the bug: it shows what the function used to do, revision by
        revision, so a recent behavior change becomes visible.

        Skip it if you only need the code as it is now or cannot name the
        function yet. ``path`` is the file, ``name`` is the bare symbol; each
        record carries the symbol's own line range and whether it is a
        definition or a prototype. Nothing after the cutoff is returned.
        """
        return graph_view.symbol_history(opened, path, name, limit)

    @server.tool()
    def tests_to_code(test: str, limit: int = 20) -> dict[str, object]:
        """Reach for this when the task or a failing test names a test file and
        you need the production code that test actually exercises, without
        tracing call paths by hand.

        Skip it if the task does not mention a test path. ``test`` is a
        repository-relative test path; results are ranked by co-change weight
        and limited to ancestors of base_sha.
        """
        return graph_view.tests_to_code(opened, test, limit)

    @server.tool()
    def similar_fixes(query: str, limit: int = 20) -> dict[str, object]:
        """Reach for this when you know the symptom but not the code: it finds
        past commits whose intent resembles your description, showing how a
        comparable bug was fixed before.

        Skip it if you already know the file or symbol to look at. ``query`` is
        free text, for example ``fix heap corruption on concurrent insert``.
        Only ancestors of base_sha are searched.
        """
        return graph_view.similar_fixes(opened, query, limit)

    @server.tool()
    def find_error_origin(message: str, limit: int = 20) -> dict[str, object]:
        """Reach for this when an error string is all you have: it points at the
        ``ereport``/``errmsg`` site that prints that message, turning log text
        into a file and line.

        Skip it if the task mentions no error text. ``message`` is the literal
        string, for example ``cache lookup failed``; placeholders such as
        ``%s`` are normalized before matching. Results are limited to ancestors
        of base_sha.
        """
        return graph_view.find_error_origin(opened, message, limit)


if __name__ == "__main__":
    serve()
