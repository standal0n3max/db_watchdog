"""MCP-сервер графа с отсечкой, Streamable HTTP.

Сервер открывает три инструмента v1 (``find_code_symbols``,
``read_versioned_source``, ``search_git_history``) поверх локального индекса и,
если передан производный ``history.sqlite``, добавляет четыре инструмента v2
(``symbol_history``, ``tests_to_code``, ``similar_fixes``, ``find_error_origin``).

Все ответы проходят через ``Cutoff``: любая revision обязана быть предком
``base_sha``, поэтому ``head_sha`` и его потомки не выдаются. Для v2 отсечка
строится по полной цепочке ``parents`` из ``history.sqlite``: окно v1-индекса
(250 коммитов) обрезало бы историю и обесценивало инструменты. Если
``history.sqlite`` нет, работает отсечка по v1-индексу — как у v1-инструментов.

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
from typing import Callable

from beartype import beartype
from mcp.server.mcpserver import MCPServer

from db_pipeline.benchmark import paths
from db_pipeline.benchmark.graph import history_tools, store, tools
from db_pipeline.benchmark.graph.cutoff import Cutoff, build_cutoff


@beartype
def _db_path() -> Path:
    raw = os.environ.get("BENCH_GRAPH_DB", "")
    if raw != "":
        return Path(raw)
    return paths.graph_dir() / "postgres.sqlite"


@beartype
def _history_path(db: Path) -> Path | None:
    """Производный ``history.sqlite``: рядом с индексом либо из переменной."""
    raw = os.environ.get("BENCH_GRAPH_HISTORY_DB", "")
    candidate = Path(raw) if raw != "" else db.parent / "history.sqlite"
    if candidate.is_file():
        return candidate
    return None


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
def _cutoffs(db: Path) -> tuple[Cutoff, Callable[[], Cutoff]]:
    """Отсечки для v1 и v2: у v2 — полная цепочка ``parents`` из истории.

    v1-индекс хранит лишь 250 последних коммитов, поэтому общая отсечка обрезала
    бы ``symbol_history`` и ``similar_fixes`` до почти пустых ответов. Для v2
    берётся ``history.sqlite``.

    v2 считается **лениво**: оркестратор копирует ``history.sqlite`` (2,3 ГБ)
    уже после старта контейнера, и чтение на старте ловило бы файл в процессе
    записи («database disk image is malformed»). На первом вызове v2 файл уже
    дописан.
    """
    base = os.environ.get("BENCH_GRAPH_BASE_SHA", "")
    head = os.environ.get("BENCH_GRAPH_HEAD_SHA", "")
    v1 = (
        build_cutoff("", 0, set(), forbidden_head=head)
        if base == ""
        else cutoff_from_index(db, base, forbidden_head=head)
    )
    history_db = _history_path(db)
    cache: dict[str, Cutoff] = {}

    def v2() -> Cutoff:
        if history_db is None or base == "":
            return v1
        if "value" not in cache:
            cache["value"] = history_tools.cutoff_for_base(
                history_db, base, forbidden_head=head
            )
        return cache["value"]

    return v1, v2


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
    cutoff, cutoff_v2 = _cutoffs(db)

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

    history_db = _history_path(db)
    if history_db is not None:

        @server.tool()
        def symbol_history(path: str, name: str, limit: int = 20) -> dict[str, object]:
            """History of a C symbol in a file: commits up to the cutoff.

            Returns each change with the symbol's own line range (not the hunk's)
            and whether the record is a definition or a prototype.
            """
            index = history_tools.open_index(history_db, db, cutoff_v2())
            try:
                return history_tools.symbol_history(index, path, name, limit)
            finally:
                history_tools.close_index(index)

        @server.tool()
        def tests_to_code(test: str, limit: int = 20) -> dict[str, object]:
            """Code files historically changed together with a regression test."""
            index = history_tools.open_index(history_db, db, cutoff_v2())
            try:
                return history_tools.tests_to_code(index, test, limit)
            finally:
                history_tools.close_index(index)

        @server.tool()
        def similar_fixes(query: str, limit: int = 20) -> dict[str, object]:
            """Past commits whose message resembles a natural-language query."""
            index = history_tools.open_index(history_db, db, cutoff_v2())
            try:
                return history_tools.similar_fixes(index, query, limit)
            finally:
                history_tools.close_index(index)

        @server.tool()
        def find_error_origin(message: str, limit: int = 20) -> dict[str, object]:
            """Locate the C source that prints a given PostgreSQL error message."""
            index = history_tools.open_index(history_db, db, cutoff_v2())
            try:
                return history_tools.find_error_origin(index, message, limit)
            finally:
                history_tools.close_index(index)

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
