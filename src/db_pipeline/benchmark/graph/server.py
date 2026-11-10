"""MCP-сервер графа с отсечкой, Streamable HTTP.

Сервер открывает три инструмента (``find_code_symbols``, ``read_versioned_source``,
``search_git_history``) поверх локального индекса. Все ответы проходят через
``Cutoff``: любая revision обязана быть предком ``base_sha``, поэтому
``head_sha`` и его потомки не выдаются.

Сервер живёт в отдельном контейнере и слушает Streamable HTTP. Порт и путь
берутся из переменных ``BENCH_GRAPH_PORT`` и ``BENCH_GRAPH_PATH``. Отсечка
вычисляется по самому индексу (parent graph), без доступа к git-клону:
достаточно ``base_sha`` и запрещённого ``head_sha``.

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
def cutoff_from_index(db: Path, base_sha: str, forbidden_head: str = "") -> Cutoff:
    """Строит отсечку по индексу: предки ``base_sha`` минус ``head_sha``.

    ``head_sha`` и все его потомки в индексе не выдаются. Доступ к git не нужен:
    предковство берётся из таблицы ``commits``.
    """
    with closing(store.open_store(db)) as conn:
        base_time = store.commit_time(conn, base_sha) or 0
        reachable = store.ancestors(conn, base_sha)
        reachable.add(base_sha)
    return build_cutoff(base_sha, base_time, reachable, forbidden_head=forbidden_head)


@beartype
def _cutoff(db: Path) -> Cutoff:
    base = os.environ.get("BENCH_GRAPH_BASE_SHA", "")
    head = os.environ.get("BENCH_GRAPH_HEAD_SHA", "")
    if base == "":
        return build_cutoff("", 0, set(), forbidden_head=head)
    return cutoff_from_index(db, base, forbidden_head=head)


@beartype
def _wait_for_db(db: Path, timeout: int = 120) -> bool:
    """Ждёт, пока оркестратор запишет индекс: ``docker cp`` даёт root-unwritable файл."""
    import time

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
    server = MCPServer("postgres-graph")
    cutoff = _cutoff(db)

    @server.tool()
    def find_code_symbols(name: str, limit: int = 20) -> dict[str, object]:
        """Find C-code symbols by name on revisions at or before the cutoff."""
        with closing(store.open_store(db)) as conn:
            return tools.find_code_symbols(conn, cutoff, name, limit)

    @server.tool()
    def read_versioned_source(
        revision: str,
        path: str,
        offset: int = 0,
        limit: int = 8192,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> dict[str, object]:
        """Read a file fragment at a pinned revision if it is an ancestor of base_sha.

        ``offset``/``limit`` are bytes; ``start_line``/``end_line`` (1-based) take
        priority and return whole lines.
        """
        with closing(store.open_store(db)) as conn:
            return tools.read_versioned_source(
                conn, cutoff, revision, path, offset, limit, start_line, end_line
            )

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
