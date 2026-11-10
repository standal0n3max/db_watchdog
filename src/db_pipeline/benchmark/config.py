"""Конфигурация benchmark из переменных окружения.

``.env`` не читается: значения берутся только из окружения. Проверяется лишь
наличие нужных переменных, значения секретов не логируются.

Ключ OpenRouter остаётся в model-gateway. Task-container получает только
``POSTGRES_GYM_PROVIDER_KEY`` как ключ провайдера и никогда не видит
``OPENROUTER_API_KEY``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from beartype import beartype

from db_pipeline.benchmark.telemetry import OtelConfig


@beartype
def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@beartype
@dataclass(frozen=True)
class BenchmarkConfig:
    image: str
    graph_image: str
    model: str
    provider: str
    max_turns: int
    temperature: float
    agent_timeout: int
    container_timeout: int
    network: str
    internal_network: str
    opencode_version: str
    openrouter_key_present: bool
    otel: OtelConfig

    def to_manifest(self) -> dict[str, object]:
        return {
            "image": self.image,
            "graph_image": self.graph_image,
            "model": self.model,
            "provider": self.provider,
            "max_turns": self.max_turns,
            "temperature": self.temperature,
            "agent_timeout": self.agent_timeout,
            "opencode_version": self.opencode_version,
            "langfuse_url": self.otel.url,
        }


@beartype
def load_config() -> BenchmarkConfig:
    key = os.environ.get("OPENROUTER_API_KEY", "")
    otel = OtelConfig(
        url=_env("LANGFUSE_URL") or _env("LANGFUSE_BASE_URL") or _env("LANGFUSE_HOST"),
        public_key=_env("LANGFUSE_PUBLIC_KEY"),
        secret_key=_env("LANGFUSE_SECRET_KEY"),
    )
    return BenchmarkConfig(
        image=_env("BENCH_TASK_IMAGE", "db-pipeline-bench-postgres:local"),
        graph_image=_env("BENCH_GRAPH_IMAGE", "db-pipeline-graph-postgres:local"),
        model=_env("BENCH_MODEL", "deepseek/deepseek-v4-flash"),
        provider=_env("BENCH_PROVIDER", "pgpro"),
        max_turns=int(_env("BENCH_MAX_TURNS", "50")),
        temperature=float(_env("BENCH_TEMPERATURE", "0")),
        agent_timeout=int(_env("BENCH_AGENT_TIMEOUT", "1800")),
        container_timeout=int(_env("BENCH_CONTAINER_TIMEOUT", "3600")),
        network=_env("BENCH_NETWORK"),
        internal_network=_env("BENCH_INTERNAL_NETWORK", "bench-internal"),
        opencode_version=_env("BENCH_OPENCODE_VERSION", "v1.18.20"),
        openrouter_key_present=key != "",
        otel=otel,
    )


@beartype
def missing_langfuse_vars() -> list[str]:
    missing: list[str] = []
    for name in ("LANGFUSE_URL", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"):
        if os.environ.get(name, "") == "":
            missing.append(name)
    return missing
