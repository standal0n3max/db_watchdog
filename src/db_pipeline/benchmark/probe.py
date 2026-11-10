"""Дешёвый E2E probe до дорогих прогонов.

Проверяет то, что ломается чаще всего и без модели:

- gateway отвечает на ``/health``, отдаёт allowlist через ``/v1/models``,
  отклоняет модель вне allowlist (403) и выполняет один completion;
- MCP-сервер графа проходит ``initialize``, отдаёт три инструмента, отвечает
  на по одному вызову каждого и отказывает по ``head_sha`` текущей задачи.

    10|MCP-проба запускается внутри самого graph-container через ``docker exec``: порт
во внутренней сети на хост не публикуется, а task-container ходит к серверу по
внутреннему DNS. Так проверяется ровно тот путь, что и в прогоне.

Проба запускает gateway и per-task graph-container в одноразовой smoke-сессии и
гарантированно удаляет их. Секреты не логируются.
"""

from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.request
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import container, convert
from db_pipeline.benchmark import session as smoke_session
from db_pipeline.benchmark.config import BenchmarkConfig


@beartype
def probe_gateway(cfg: BenchmarkConfig, host_port: str) -> dict[str, object]:
    """Проверяет health, allowlist, отказ вне allowlist и один completion."""
    base = f"http://127.0.0.1:{host_port}"
    report: dict[str, object] = {}
    report["health"] = _get_json(f"{base}/health")
    models = _get_json(f"{base}/v1/models")
    report["models"] = models
    denied = _post_json(
        f"{base}/v1/chat/completions",
        {"model": "not/allowed", "messages": [{"role": "user", "content": "hi"}]},
    )
    report["denied"] = denied
    completion = _post_json(
        f"{base}/v1/chat/completions",
        {
            "model": cfg.model,
            "messages": [{"role": "user", "content": "Reply with the single word: ok"}],
            "max_tokens": 5,
            "temperature": 0,
        },
    )
    report["completion"] = completion
    health = _as_dict(report["health"])
    ok = (
        bool(health.get("ok"))
        and len(_as_dict(models)) > 0
        and _status(denied) == 403
        and _status(completion) == 200
    )
    report["ok"] = ok
    return report


@beartype
def probe_graph(graph_id: str, base_sha: str, head_sha: str, context: str = "") -> dict[str, object]:
    """MCP-проба внутри graph-container: initialize, tools, вызовы и cutoff."""
    script = (
        "python -m db_pipeline.benchmark.graph.probe_client "
        f"http://{smoke_session.GRAPH_ALIAS}:{smoke_session.GRAPH_PORT}{smoke_session.GRAPH_PATH} "
        f"{smoke_session.GRAPH_DB_IN_CONTAINER} {base_sha} {head_sha}"
    )
    result = subprocess.run(
        container.docker_command(
            "exec", "-i", graph_id, "sh", "-lc", script, context=context
        ),
        capture_output=True,
        text=True,
        check=False,
    )
    report = _parse_json_line(result.stdout)
    report["returncode"] = result.returncode
    if result.returncode != 0 and not report.get("ok"):
        report["stderr"] = result.stderr[-1000:]
    return report


@beartype
def probe_task(cfg: BenchmarkConfig, data: Path | None = None, name: str = "") -> dict[str, object]:
    """Полная дешёвая проба: gateway и MCP на одной контрольной задаче."""
    task = name or "postgres__d764d61af316"
    oracle = convert.load_oracle(task, data)
    report: dict[str, object] = {"task": task}
    active = smoke_session.open_session(cfg, data)
    graph_id = ""
    try:
        gateway_port = container.published_port(active.gateway_id, smoke_session.GATEWAY_PORT)
        report["gateway"] = probe_gateway(cfg, gateway_port)
        graph_id = smoke_session.start_graph(
            active, str(oracle["base"]), str(oracle["head"]), data
        )
        report["graph"] = probe_graph(
            graph_id, str(oracle["base"]), str(oracle["head"]), active.context
        )
    finally:
        if graph_id != "":
            smoke_session.stop_graph(active, graph_id)
        report["cleanup"] = smoke_session.close_session(active)
    gateway_ok = isinstance(report.get("gateway"), dict) and bool(
        _as_dict(report["gateway"]).get("ok")
    )
    graph_ok = isinstance(report.get("graph"), dict) and bool(
        _as_dict(report["graph"]).get("ok")
    )
    report["ok"] = gateway_ok and graph_ok and bool(_as_dict(report["cleanup"]).get("ok"))
    return report


@beartype
def _parse_json_line(text: str) -> dict[str, object]:
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                loaded: object = json.loads(line)
            except json.JSONDecodeError:
                continue
            return _as_dict(loaded)
    return {}


@beartype
def _as_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return cast("dict[str, object]", value)
    return {}


@beartype
def _status(value: object) -> int:
    mapping = _as_dict(value)
    status = mapping.get("status")
    return int(status) if isinstance(status, int) else 0


@beartype
def _get_json(url: str) -> dict[str, object]:
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            loaded: object = json.loads(response.read().decode())
        return _as_dict(loaded)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return {"error": type(exc).__name__}


@beartype
def _post_json(url: str, payload: dict[str, object]) -> dict[str, object]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read().decode()
            loaded: object = json.loads(body)
            result = _as_dict(loaded)
            result["status"] = response.status
            return result
    except urllib.error.HTTPError as exc:
        return {"status": exc.code, "body": exc.read().decode()[:300]}
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return {"error": type(exc).__name__}
