"""Выгрузка трасс Langfuse по записям прогона.

Только чтение. По ``trace_id`` забирается ``GET /api/public/traces/{id}`` и
страницы ``GET /api/public/v2/observations?traceId=``. Формат сверен с
документацией Langfuse: список observations заменил вложенное чтение трассы,
поэтому оба ответа сохраняются.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast
from urllib.parse import urlencode

from beartype import beartype

from db_pipeline.benchmark import eval as eval_module
from db_pipeline.benchmark import paths, telemetry
from db_pipeline.benchmark.config import BenchmarkConfig


@beartype
def fetch_trace(config: BenchmarkConfig, trace_id: str) -> dict[str, object]:
    """Трасса и её observations. Пустой ``trace_id`` не ходит в сеть."""
    if trace_id == "":
        return {"ok": False, "error": "нет trace_id"}
    trace = telemetry.public_get(config.otel, f"/api/public/traces/{trace_id}")
    observations = _observations(config, trace_id)
    return {"trace": trace, "observations": observations}


@beartype
def dump_experiment(
    config: BenchmarkConfig,
    data: Path | None,
    experiment_id: str,
    label: str,
) -> dict[str, object]:
    """Пишет по файлу на прогон в ``data/reports/<label>/traces``."""
    records = eval_module.load_records(data, experiment_id)
    folder = paths.reports_dir(data) / label / "traces"
    folder.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for record in records:
        trace_id = str(record.get("trace_id") or "")
        payload = fetch_trace(config, trace_id)
        payload["func"] = record.get("func")
        payload["mode"] = record.get("mode")
        payload["started"] = record.get("started")
        payload["verdict"] = record.get("verdict")
        payload["agent_mcp_calls"] = record.get("agent_mcp_calls")
        payload["briefing"] = record.get("briefing")
        name = (
            f"{record.get('func')}__{record.get('mode')}__{trace_id or record.get('started')}.json"
        )
        target = folder / name
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        written.append(str(target.name))
    return {"written": len(written), "dir": str(folder), "files": written}


@beartype
def _observations(config: BenchmarkConfig, trace_id: str) -> list[dict[str, object]]:
    found: list[dict[str, object]] = []
    cursor = ""
    for _page in range(20):
        query = urlencode(
            {
                "traceId": trace_id,
                "limit": "100",
                "fields": "core,basic,usage",
                **({"cursor": cursor} if cursor != "" else {}),
            }
        )
        page = telemetry.public_get(config.otel, "/api/public/v2/observations", query)
        rows = page.get("data")
        if isinstance(rows, list):
            for item in cast("list[object]", rows):
                if isinstance(item, dict):
                    found.append(cast("dict[str, object]", item))
        meta = page.get("meta")
        next_cursor = ""
        if isinstance(meta, dict):
            raw = cast("dict[str, object]", meta).get("cursor")
            next_cursor = raw if isinstance(raw, str) else ""
        if next_cursor == "" or next_cursor == cursor or page.get("ok") is not True:
            break
        cursor = next_cursor
    return found
