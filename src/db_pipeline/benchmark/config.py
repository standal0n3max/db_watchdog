"""Конфигурация benchmark из переменных окружения.

Корневой ``.env`` загружается только на границе CLI через
``db_pipeline.environment.load_environment``; сам модуль значений не читает.
Проверяется лишь наличие нужных переменных, значения секретов не логируются.

Ключ OpenRouter остаётся в model-gateway. Task-container получает только
``POSTGRES_GYM_PROVIDER_KEY`` как ключ провайдера и никогда не видит
``OPENROUTER_API_KEY``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from beartype import beartype

from db_pipeline.benchmark.opencode import (
    DEFAULT_CONTEXT_LENGTH,
    DEFAULT_MAX_TOKENS,
    DEFAULT_STEP_LIMIT,
)
from db_pipeline.benchmark.telemetry import OtelConfig


@beartype
def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@beartype
@dataclass(frozen=True)
class BenchmarkConfig:
    image: str
    graph_image: str
    gateway_image: str
    model: str
    provider: str
    step_limit: int
    max_tokens: int
    temperature: float
    context_length: int
    reasoning_max_tokens: int
    empty_stop_nudges: int
    experiment_id: str
    langfuse_project: str
    agent_timeout: int
    container_timeout: int
    network: str
    internal_network: str
    opencode_version: str
    openrouter_key_present: bool
    otel: OtelConfig

    def to_manifest(self) -> dict[str, object]:
        """Фиксирует ровно те лимиты, что передаются агенту.

        ``step_limit`` — реальное ограничение числа ходов
        (``agent.build.steps`` у OpenCode), ``max_tokens`` — потолок ответа
        модели. Прежний ``max_turns`` не передавался в ``opencode run`` и потому
        удалён из manifest: он не описывал прогон.
        """
        return {
            "image": self.image,
            "graph_image": self.graph_image,
            "gateway_image": self.gateway_image,
            "model": self.model,
            "provider": self.provider,
            "step_limit": self.step_limit,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "context_length": self.context_length,
            "reasoning_max_tokens": self.reasoning_max_tokens,
            "empty_stop_nudges": self.empty_stop_nudges,
            "experiment_id": self.experiment_id,
            "langfuse_project": self.langfuse_project,
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
        gateway_image=_env("BENCH_GATEWAY_IMAGE", "db-pipeline-bench-gateway:local"),
        model=_env("BENCH_MODEL", "deepseek/deepseek-v4-flash"),
        provider=_env("BENCH_PROVIDER", "pgpro"),
        step_limit=int(_env("BENCH_STEP_LIMIT", str(DEFAULT_STEP_LIMIT))),
        max_tokens=int(_env("BENCH_MAX_TOKENS", str(DEFAULT_MAX_TOKENS))),
        temperature=float(_env("BENCH_TEMPERATURE", "0")),
        context_length=int(_env("BENCH_CONTEXT_LENGTH", str(DEFAULT_CONTEXT_LENGTH))),
        reasoning_max_tokens=int(_env("BENCH_REASONING_MAX_TOKENS", "1024")),
        empty_stop_nudges=int(_env("BENCH_EMPTY_STOP_NUDGES", "1")),
        experiment_id=_env("BENCH_EXPERIMENT_ID"),
        langfuse_project=_env("BENCH_LANGFUSE_PROJECT", "diagnostic"),
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
