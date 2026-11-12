"""Публикация трассы прогона в Langfuse v4 через OTLP HTTP.

Корневой run span несёт вердикт и metadata, дочерние generation spans — вызовы
модели, дочерние span — инструменты и шаги harness
(``prepare/prebuild/hide/sever/agent/restore/build/install/test/score``).
Отправка идёт на ``/api/public/otel/v1/traces`` с Basic auth и заголовком
``x-langfuse-ingestion-version: 4``.

Секреты и auth-заголовки в payload не попадают. ``doctor`` проверяет health и
ключи и выполняет probe trace. Конфигурация берётся из переменных окружения;
корневой ``.env`` загружается только на границе CLI.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

from beartype import beartype

INGESTION_VERSION = "4"


@beartype
@dataclass(frozen=True)
class OtelConfig:
    url: str
    public_key: str
    secret_key: str

    @property
    def enabled(self) -> bool:
        return self.public_key != "" and self.secret_key != ""

    @property
    def endpoint(self) -> str:
        return f"{self.url.rstrip('/')}/api/public/otel/v1/traces"


@beartype
@dataclass
class OtelSpan:
    name: str
    start_ns: int
    end_ns: int = 0
    attributes: dict[str, str] = field(default_factory=dict[str, str])
    parent_id: str = ""
    span_id: str = ""
    kind: str = "span"

    def __post_init__(self) -> None:
        if self.span_id == "":
            self.span_id = _hex(8)
        if self.end_ns == 0:
            self.end_ns = self.start_ns


@beartype
def new_trace_id() -> str:
    return _hex(16)


@beartype
def _hex(size: int) -> str:
    return uuid.uuid4().hex[: size * 2]


@beartype
def now_ns() -> int:
    return int(datetime.now(UTC).timestamp() * 1_000_000_000)


@beartype
def ms_to_ns(millis: int) -> int:
    if millis <= 0:
        return now_ns()
    return millis * 1_000_000


@beartype
def span_payload(span: OtelSpan) -> dict[str, object]:
    payload: dict[str, object] = {
        "traceId": span.attributes.pop("__traceId", ""),
        "spanId": span.span_id,
        "name": span.name,
        "startTimeUnixNano": str(span.start_ns),
        "endTimeUnixNano": str(span.end_ns),
        "attributes": [
            {"key": key, "value": {"stringValue": value}}
            for key, value in span.attributes.items()
        ],
    }
    if span.parent_id != "":
        payload["parentSpanId"] = span.parent_id
    return payload


@beartype
def build_otlp(
    trace_id: str,
    root: OtelSpan,
    children: list[OtelSpan],
) -> dict[str, object]:
    spans: list[dict[str, object]] = []
    root_attrs = dict(root.attributes)
    root_attrs["__traceId"] = trace_id
    root_payload: dict[str, object] = {
        "traceId": trace_id,
        "spanId": root.span_id,
        "name": root.name,
        "startTimeUnixNano": str(root.start_ns),
        "endTimeUnixNano": str(root.end_ns),
        "attributes": [
            {"key": key, "value": {"stringValue": value}}
            for key, value in root_attrs.items()
            if key != "__traceId"
        ],
    }
    spans.append(root_payload)
    for child in children:
        attrs = dict(child.attributes)
        attrs.pop("__traceId", None)
        payload: dict[str, object] = {
            "traceId": trace_id,
            "spanId": child.span_id,
            "name": child.name,
            "startTimeUnixNano": str(child.start_ns),
            "endTimeUnixNano": str(child.end_ns),
            "attributes": [
                {"key": key, "value": {"stringValue": value}}
                for key, value in attrs.items()
            ],
        }
        if child.parent_id != "":
            payload["parentSpanId"] = child.parent_id
        spans.append(payload)
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "db-pipeline-benchmark"}}
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "db_pipeline.benchmark"},
                        "spans": spans,
                    }
                ],
            }
        ]
    }


@beartype
def _auth_header(config: OtelConfig) -> str:
    raw = f"{config.public_key}:{config.secret_key}".encode()
    return "Basic " + base64.b64encode(raw).decode()


@beartype
def publish(config: OtelConfig, trace_id: str, payload: dict[str, object]) -> dict[str, object]:
    if not config.enabled:
        return {"sent": False, "reason": "LANGFUSE_PUBLIC_KEY/SECRET_KEY не заданы"}
    request = urllib.request.Request(
        config.endpoint,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": _auth_header(config),
            "x-langfuse-ingestion-version": INGESTION_VERSION,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode() or "{}"
    except urllib.error.HTTPError as exc:
        return {
            "sent": False,
            "error": f"HTTP {exc.code}",
            "detail": exc.read().decode()[:500],
            "url": config.url,
        }
    except Exception as exc:  # noqa: BLE001 - отчёт не должен ломать прогон
        return {
            "sent": False,
            "error": type(exc).__name__,
            "detail": str(exc)[:300],
            "url": config.url,
        }
    return {
        "sent": True,
        "trace_id": trace_id,
        "events": len(body),
        "url": f"{config.url.rstrip('/')}/trace/{trace_id}",
        "raw": body[:500],
    }


@beartype
def public_get(config: OtelConfig, path: str, query: str = "") -> dict[str, object]:
    """Читает публичный API Langfuse. ``path`` начинается с ``/api/public``."""
    if not config.enabled or config.url == "":
        return {"ok": False, "error": "ключи не заданы"}
    url = f"{config.url.rstrip('/')}{path}"
    if query != "":
        url = f"{url}?{query}"
    request = urllib.request.Request(url, headers={"Authorization": _auth_header(config)})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode()
            status = response.status
    except urllib.error.HTTPError as exc:
        return {"ok": False, "status": exc.code, "error": exc.read().decode()[:500]}
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return {"ok": False, "error": type(exc).__name__}
    try:
        loaded: object = json.loads(body or "{}")
    except json.JSONDecodeError:
        return {"ok": False, "status": status, "error": "не json"}
    if isinstance(loaded, dict):
        raw = cast("dict[object, object]", loaded)
        payload: dict[str, object] = {str(key): value for key, value in raw.items()}
        payload["ok"] = True
        payload["status"] = status
        return payload
    return {"ok": True, "status": status, "data": loaded}


@beartype
def health(config: OtelConfig) -> tuple[bool, str]:
    url = config.url.rstrip("/")
    try:
        with urllib.request.urlopen(f"{url}/api/public/health", timeout=10) as response:
            response.read()
    except Exception as exc:  # noqa: BLE001
        return False, f"{url} недоступен: {type(exc).__name__} {exc}"
    if not config.enabled:
        return False, "LANGFUSE_PUBLIC_KEY/SECRET_KEY не заданы"
    request = urllib.request.Request(
        f"{url}/api/public/projects", headers={"Authorization": _auth_header(config)}
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        return False, f"{url} отклонил ключи: HTTP {exc.code}"
    except Exception as exc:  # noqa: BLE001
        return False, f"{url}: {type(exc).__name__} {exc}"
    return True, url


@beartype
def list_projects(config: OtelConfig) -> list[dict[str, str]]:
    """Проекты ключа: ``GET /api/public/projects`` (Basic auth)."""
    if not config.enabled or config.url == "":
        return []
    request = urllib.request.Request(
        f"{config.url.rstrip('/')}/api/public/projects",
        headers={"Authorization": _auth_header(config)},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read().decode()
    except (urllib.error.HTTPError, urllib.error.URLError, OSError, ValueError):
        return []
    try:
        loaded: object = json.loads(body or "{}")
    except json.JSONDecodeError:
        return []
    rows: object = loaded
    if isinstance(loaded, dict):
        rows = cast("dict[str, object]", loaded).get("data", [])
    if not isinstance(rows, list):
        return []
    found: list[dict[str, str]] = []
    for item in cast("list[object]", rows):
        if not isinstance(item, dict):
            continue
        row = cast("dict[str, object]", item)
        found.append(
            {
                "id": str(row.get("id") or ""),
                "name": str(row.get("name") or ""),
            }
        )
    return found


@beartype
def project_matches(config: OtelConfig, expected: str) -> tuple[bool, str]:
    """Имя проекта ключа совпадает с ``expected``. Пустое имя — отказ."""
    if expected == "":
        return False, "BENCH_LANGFUSE_PROJECT не задан"
    projects = list_projects(config)
    if len(projects) == 0:
        return False, "список проектов пуст или недоступен"
    names = [item["name"] for item in projects]
    if expected in names:
        matched = next(item for item in projects if item["name"] == expected)
        return True, matched["id"]
    return False, "ключ ведёт в проект " + ", ".join(names)


@beartype
def probe(config: OtelConfig) -> dict[str, object]:
    trace_id = new_trace_id()
    start = now_ns()
    root = OtelSpan(
        name="doctor-probe",
        start_ns=start,
        end_ns=start + 1_000_000,
        attributes={"langfuse.trace.name": "doctor-probe"},
    )
    child = OtelSpan(
        name="probe:generation",
        start_ns=start,
        end_ns=start + 1_000_000,
        parent_id=root.span_id,
        attributes={
            "langfuse.observation.type": "generation",
            "langfuse.observation.model.name": "probe",
            "langfuse.observation.output": "probe",
        },
    )
    return publish(config, trace_id, build_otlp(trace_id, root, [child]))
