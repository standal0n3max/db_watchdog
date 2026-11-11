"""Lifecycle парного smoke: сеть, model-gateway и per-task graph-container.

Один model-gateway обслуживает все прогоны: он подключён к внешней сети (доступ
к OpenRouter) и к внутренней сети задач с alias ``model-gateway``. Task-container
видит только внутреннюю сеть и не имеет внешнего egress.

На каждый graph-run поднимается отдельный одноразовый graph-container с копией
SQLite-индекса через ``docker cp``: ``base_sha`` и запрещённый ``head_sha``
берутся из oracle текущей задачи, cutoff вычисляется по parent graph индекса.
Контейнер удаляется после прогона.

Секреты: ``OPENROUTER_API_KEY`` уходит только в model-gateway, ``LANGFUSE_*``
остаются у хостового оркестратора. Task-container реальных ключей не получает.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark import container, paths
from db_pipeline.benchmark.config import BenchmarkConfig
from db_pipeline.errors import PipelineError

GATEWAY_ALIAS = "model-gateway"
GATEWAY_PORT = "8080"
GATEWAY_HEALTH_PATH = "/health"
GRAPH_ALIAS = "graph"
GRAPH_PORT = "8765"
GRAPH_PATH = "/mcp"
GRAPH_DB_IN_CONTAINER = "/tmp/graph.sqlite"
HISTORY_DB_IN_CONTAINER = "/tmp/history.sqlite"


@beartype
@dataclass
class SmokeSession:
    """Одноразовая smoke-сессия: сеть, gateway и учёт созданных контейнеров."""

    cfg: BenchmarkConfig
    network: str
    gateway_id: str
    context: str = ""
    graph_ids: list[str] = field(default_factory=list[str])
    cleanup_ok: bool = True


@beartype
def open_session(
    cfg: BenchmarkConfig,
    data: Path | None = None,
    context: str = "",
) -> SmokeSession:
    """Создаёт внутреннюю сеть и поднимает model-gateway с внешним egress."""
    suffix = uuid.uuid4().hex[:8]
    internal = f"{cfg.internal_network}-{suffix}"
    container.network_create(internal, internal=True, context=context)
    external = cfg.network or "bridge"
    gateway_id = _start_gateway(cfg, external, internal, context)
    return SmokeSession(cfg=cfg, network=internal, gateway_id=gateway_id, context=context)


@beartype
def _start_gateway(
    cfg: BenchmarkConfig,
    external: str,
    internal: str,
    context: str,
) -> str:
    key = _openrouter_key()
    if key == "":
        raise PipelineError("gateway", "OPENROUTER_API_KEY не задан")
    spec = container.ContainerSpec(
        image=cfg.gateway_image,
        workspace="/app",
        network=external,
        ports={"0": GATEWAY_PORT},
        labels={"bench.role": "model-gateway"},
        environment={
            "OPENROUTER_API_KEY": key,
            "BENCH_ALLOWED_MODELS": cfg.model,
        },
    )
    gateway_id = container.create_container(
        spec, name=f"bench-gateway-{uuid.uuid4().hex[:8]}", context=context
    )
    container.network_connect(internal, gateway_id, alias=GATEWAY_ALIAS, context=context)
    container.start_detached(gateway_id, context=context)
    host_port = container.published_port(gateway_id, GATEWAY_PORT, context=context)
    if host_port == "":
        container.remove_container(gateway_id, context=context)
        raise PipelineError("gateway", "не удалось определить опубликованный порт")
    url = f"http://127.0.0.1:{host_port}{GATEWAY_HEALTH_PATH}"
    if not container.wait_http_ready(url, timeout=120):
        container.remove_container(gateway_id, context=context)
        raise PipelineError("gateway", "gateway не прошёл readiness /health")
    return gateway_id


@beartype
def _openrouter_key() -> str:
    import os

    return os.environ.get("OPENROUTER_API_KEY", "")


@beartype
def start_graph(
    session: SmokeSession,
    base_sha: str,
    head_sha: str,
    data: Path | None = None,
) -> str:
    """Поднимает одноразовый graph-container для конкретной задачи.

    Если рядом с индексом лежит производный ``history.sqlite``, он тоже
    копируется в контейнер и включает инструменты v2. Его отсутствие не ломает
    v1: контейнер просто остаётся с тремя инструментами.
    """
    db = paths.graph_dir(data) / "postgres.sqlite"
    if not db.is_file():
        raise PipelineError("graph", f"нет индекса {db}: сначала graph-index")
    history_db = db.parent / "history.sqlite"
    has_history = history_db.is_file()
    environment = {
        "BENCH_GRAPH_DB": GRAPH_DB_IN_CONTAINER,
        "BENCH_GRAPH_BASE_SHA": base_sha,
        "BENCH_GRAPH_HEAD_SHA": head_sha,
        "BENCH_GRAPH_PORT": GRAPH_PORT,
        "BENCH_GRAPH_PATH": GRAPH_PATH,
    }
    if has_history:
        environment["BENCH_GRAPH_HISTORY_DB"] = HISTORY_DB_IN_CONTAINER
    spec = container.ContainerSpec(
        image=session.cfg.graph_image,
        workspace="/opt/db-pipeline",
        network=session.network,
        network_alias=GRAPH_ALIAS,
        labels={"bench.role": "graph"},
        environment=environment,
    )
    graph_id = container.create_container(spec, name=f"bench-graph-{uuid.uuid4().hex[:8]}", context=session.context)
    container.start_detached(graph_id, context=session.context)
    container.write_in(graph_id, GRAPH_DB_IN_CONTAINER, db.read_bytes(), context=session.context)
    if has_history:
        container.write_in(
            graph_id,
            HISTORY_DB_IN_CONTAINER,
            history_db.read_bytes(),
            context=session.context,
        )
    session.graph_ids.append(graph_id)
    if not _graph_ready(graph_id, session.context):
        container.remove_container(graph_id, context=session.context)
        session.graph_ids.remove(graph_id)
        raise PipelineError("graph", "graph MCP не прошёл readiness")
    return graph_id


@beartype
def _graph_ready(graph_id: str, context: str) -> bool:
    """Readiness MCP: TCP-проба порта внутри контейнера через python."""
    import time

    probe = (
        "python -c \"import socket;s=socket.socket();s.settimeout(1);"
        f"s.connect(('127.0.0.1',{GRAPH_PORT}));print('up')\""
    )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        code, output = _exec(graph_id, probe, context)
        if code == 0 and "up" in output:
            return True
        time.sleep(1)
    return False


@beartype
def _exec(container_id: str, script: str, context: str) -> tuple[int, str]:
    import subprocess

    result = subprocess.run(
        container.docker_command(
            "exec", "-i", container_id, "sh", "-lc", script, context=context
        ),
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, result.stdout + result.stderr


@beartype
def stop_graph(session: SmokeSession, graph_id: str) -> bool:
    """Останавливает и удаляет graph-container, подтверждая cleanup."""
    removed = container.remove_container_verified(graph_id, context=session.context)
    if graph_id in session.graph_ids:
        session.graph_ids.remove(graph_id)
    if not removed:
        session.cleanup_ok = False
    return removed


@beartype
def close_session(session: SmokeSession) -> dict[str, object]:
    """Удаляет gateway, все graph-контейнеры и сеть. Возвращает отчёт cleanup."""
    for graph_id in list(session.graph_ids):
        stop_graph(session, graph_id)
    gateway_removed = container.remove_container_verified(
        session.gateway_id, context=session.context
    )
    container.network_remove(session.network, context=session.context)
    network_gone = not container.network_exists(session.network, context=session.context)
    cleanup_ok = gateway_removed and network_gone and session.cleanup_ok
    return {
        "gateway_removed": gateway_removed,
        "network_removed": network_gone,
        "graph_removed": len(session.graph_ids) == 0,
        "ok": cleanup_ok,
    }
