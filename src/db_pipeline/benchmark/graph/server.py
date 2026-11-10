"""MCP-сервер графа с отсечкой, Streamable HTTP.

Сервер открывает три инструмента (``find_code_symbols``, ``read_versioned_source``,
``search_git_history``) поверх локального индекса. Все ответы проходят через
``Cutoff``: любая revision обязана быть предком ``base_sha``, поэтому
``head_sha`` и его потомки не выдаются.

Сервер живёт в отдельном контейнере и слушает Streamable HTTP. Порт и путь
берутся из переменных ``BENCH_GRAPH_PORT`` и ``BENCH_GRAPH_PATH``.

Индекс открывается в режиме только для чтения на запросы: ``audit`` пишется в
ту же базу, поэтому соединение переиспользуется.
"""

from __future__ import annotations

import os
from contextlib import closing
from pathlib import Path

from beartype import beartype
from mcp.server.mcpserver import MCPServer

from db_pipeline.benchmark import paths
from db_pipeline.benchmark.graph import store, tools
from db_pipeline.benchmark.graph.cutoff import Cutoff, build_cutoff


@beartype
def _db_path() -> Path:
    raw = os.environ.get("BENCH_GRAPH_DB", "")
    if raw != "":
        return Path(raw)
    return paths.graph_dir() / "postgres.sqlite"


@beartype
def _cutoff() -> Cutoff:
    raw = os.environ.get("BENCH_GRAPH_BASE_SHA", "")
    limit = int(os.environ.get("BENCH_GRAPH_LIMIT", "4000"))
    import subprocess

    repo = paths.pg_clone()
    base = raw or _git(repo, "rev-parse", "HEAD")
    if base == "":
        return build_cutoff("", 0, set())
    time_text = _git(repo, "show", "-s", "--format=%ct", base)
    try:
        base_time = int(time_text or "0")
    except ValueError:
        base_time = 0
    reachable = subprocess.run(
        ["git", "-C", str(repo), "rev-list", f"--max-count={limit}", base],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.split()
    head = os.environ.get("BENCH_GRAPH_HEAD_SHA", "")
    return build_cutoff(base, base_time, set(reachable), forbidden_head=head)


@beartype
def _git(repo: Path, *args: str) -> str:
    import subprocess

    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()


@beartype
def serve() -> None:
    server = MCPServer("postgres-graph")
    db = _db_path()
    cutoff = _cutoff()

    @server.tool()
    def find_code_symbols(name: str, limit: int = 20) -> dict[str, object]:
        """Find C-code symbols by name on revisions at or before the cutoff."""
        with closing(store.open_store(db)) as conn:
            return tools.find_code_symbols(conn, cutoff, name, limit)

    @server.tool()
    def read_versioned_source(revision: str, path: str) -> dict[str, object]:
        """Read file metadata at a pinned revision if it is an ancestor of base_sha."""
        with closing(store.open_store(db)) as conn:
            return tools.read_versioned_source(conn, cutoff, revision, path)

    @server.tool()
    def search_git_history(query: str, limit: int = 20) -> dict[str, object]:
        """Search commit messages and hunks before the cutoff."""
        with closing(store.open_store(db)) as conn:
            return tools.search_git_history(conn, cutoff, query, limit)

    port = int(os.environ.get("BENCH_GRAPH_PORT", "8765"))
    path = os.environ.get("BENCH_GRAPH_PATH", "/mcp")
    server.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
        streamable_http_path=path,
        json_response=True,
    )


if __name__ == "__main__":
    serve()
