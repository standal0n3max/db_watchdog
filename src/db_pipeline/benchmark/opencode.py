"""Обёртка над OpenCode в контейнере задачи.

OpenCode запускается непривилегированным пользователем в ``/workspace/postgres``
с ``--format json --standalone``. Модель и провайдер задаёт model-gateway:
ключ приходит только как ``POSTGRES_GYM_PROVIDER_KEY`` и не попадает в промпт.

JSONL-события группируются по ``messageID``: ``step_finish`` даёт tokens/cost/reason,
``tool_use`` — input/output/status/time, ``text`` — ответ модели. Разбор не
запускает процесс, он принимает уже снятый stdout, поэтому проверяется тестами.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import cast

from beartype import beartype

from db_pipeline.errors import PipelineError

# Реальный лимит шагов агента: попадает в ``agent.build.steps`` эффективного
# конфига. ``opencode run`` собственного флага лимита не имеет.
DEFAULT_STEP_LIMIT = 80

MODES = ("bare", "graph")


@beartype
@dataclass(frozen=True)
class AgentConfig:
    provider: str = "pgpro"
    model: str = ""
    temperature: float = 0.0
    context_length: int = 32768
    max_tokens: int = 4096
    step_limit: int = DEFAULT_STEP_LIMIT
    timeout: int = 1800


@beartype
@dataclass
class ToolCall:
    tool: str
    status: str
    started: int = 0
    ended: int = 0
    input: str = ""
    output: str = ""
    message_id: str = ""

    @property
    def seconds(self) -> float:
        if self.started and self.ended:
            return round((self.ended - self.started) / 1000, 3)
        return 0.0


@beartype
@dataclass
class Generation:
    message_id: str
    model: str = ""
    reason: str = ""
    cost: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0
    cache_read: int = 0
    cache_write: int = 0
    text: str = ""


@beartype
@dataclass
class AgentTranscript:
    generations: list[Generation] = field(default_factory=list[Generation])
    tools: list[ToolCall] = field(default_factory=list[ToolCall])
    errors: list[str] = field(default_factory=list[str])
    raw_events: int = 0

    def tool_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for tool in self.tools:
            counts[tool.tool] = counts.get(tool.tool, 0) + 1
        return dict(sorted(counts.items(), key=lambda kv: -kv[1]))

    def totals(self) -> dict[str, int | float]:
        return {
            "calls": len(self.generations),
            "tools": len(self.tools),
            "tokens_input": sum(g.input_tokens for g in self.generations),
            "tokens_output": sum(g.output_tokens for g in self.generations),
            "tokens_reasoning": sum(g.reasoning_tokens for g in self.generations),
            "cache_read": sum(g.cache_read for g in self.generations),
            "cost": round(sum(g.cost for g in self.generations), 6),
        }


@beartype
def parse_jsonl(text: str) -> AgentTranscript:
    transcript = AgentTranscript()
    by_message: dict[str, Generation] = {}
    for line in text.splitlines():
        line = line.strip()
        if line == "" or not line.startswith("{"):
            continue
        try:
            loaded: object = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(loaded, dict):
            continue
        event = _as_dict(cast("object", loaded))
        transcript.raw_events += 1
        if len(event) == 0:
            continue
        kind = event.get("type")
        part = _as_dict(event.get("part"))
        if len(part) == 0:
            continue
        if kind == "step_finish":
            _absorb_step(transcript, by_message, part)
        elif kind == "tool_use":
            _absorb_tool(transcript, part)
        elif kind == "text":
            _absorb_text(by_message, part)
        elif kind == "error":
            message = part.get("error") or part.get("message") or "error"
            transcript.errors.append(str(message)[:500])
    transcript.generations = list(by_message.values())
    return transcript


@beartype
def _absorb_step(
    transcript: AgentTranscript,
    by_message: dict[str, Generation],
    part: dict[str, object],
) -> None:
    message_id = str(part.get("messageID") or part.get("id") or "")
    generation = by_message.setdefault(message_id, Generation(message_id=message_id))
    generation.reason = str(part.get("reason") or "")
    cost = part.get("cost")
    if isinstance(cost, (int, float)):
        generation.cost = float(cost)
    tokens = _as_dict(part.get("tokens"))
    if len(tokens) > 0:
        generation.input_tokens = _int(tokens.get("input"))
        generation.output_tokens = _int(tokens.get("output"))
        generation.reasoning_tokens = _int(tokens.get("reasoning"))
        cache = _as_dict(tokens.get("cache"))
        generation.cache_read = _int(cache.get("read"))
        generation.cache_write = _int(cache.get("write"))
    transcript.generations = list(by_message.values())


@beartype
def _absorb_tool(transcript: AgentTranscript, part: dict[str, object]) -> None:
    state = _as_dict(part.get("state"))
    if len(state) == 0:
        return
    clock = _as_dict(state.get("time"))
    transcript.tools.append(
        ToolCall(
            tool=str(part.get("tool") or "?"),
            status=str(state.get("status") or ""),
            started=_int(clock.get("start")),
            ended=_int(clock.get("end")),
            input=_json_text(state.get("input")),
            output=_json_text(state.get("output")),
            message_id=str(part.get("messageID") or ""),
        )
    )


@beartype
def _absorb_text(by_message: dict[str, Generation], part: dict[str, object]) -> None:
    message_id = str(part.get("messageID") or part.get("id") or "")
    generation = by_message.setdefault(message_id, Generation(message_id=message_id))
    text = part.get("text")
    if isinstance(text, str):
        generation.text += text


@beartype
def _int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


@beartype
def _as_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        raw = cast("dict[object, object]", value)
        return {str(key): item for key, item in raw.items()}
    return {}


@beartype
def _json_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value[:4000]
    return json.dumps(value, ensure_ascii=False)[:4000]


@beartype
def run_command(config: AgentConfig, prompt: str) -> list[str]:
    return [
        "opencode",
        "run",
        "--pure",
        "--auto",
        "--format",
        "json",
        "--model",
        f"{config.provider}/{config.model}",
        prompt,
    ]


GRAPH_MCP_URL = "http://graph:8765/mcp"
CONFIG_PATH = "/home/bench/.config/opencode/opencode.jsonc"


@beartype
def build_config(
    mode: str,
    provider: str,
    model: str,
    context_length: int = 32768,
    max_tokens: int = 4096,
    step_limit: int = DEFAULT_STEP_LIMIT,
) -> dict[str, object]:
    """Строит эффективный OpenCode-конфиг: bare без MCP, graph с remote MCP.

    Секретов в конфиге нет: ключ провайдера подставляется из
    ``POSTGRES_GYM_PROVIDER_KEY`` на стороне контейнера. ``graph``-режим
    подключает три инструмента по Streamable HTTP с увеличенным таймаутом.

    ``agent.build.steps`` задаёт реальный лимит ходов агента по умолчанию
    (``build`` — primary-агент OpenCode); без него ``opencode run`` ходов не
    ограничивает.
    """
    if mode not in MODES:
        raise PipelineError("opencode", f"режим {mode} неизвестен")
    config: dict[str, object] = {
        "$schema": "https://opencode.ai/config.json",
        "autoupdate": False,
        "share": "disabled",
        "provider": {
            provider: {
                "npm": "@ai-sdk/openai-compatible",
                "name": "Model Gateway",
                "options": {
                    "baseURL": "http://model-gateway:8080/v1",
                    "apiKey": "{env:POSTGRES_GYM_PROVIDER_KEY}",
                },
                "models": {
                    model: {
                        "name": model,
                        "limit": {"context": context_length, "output": max_tokens},
                    }
                },
            }
        },
        "agent": {"build": {"steps": step_limit}},
        "permission": {"webfetch": "deny", "external_directory": "deny"},
        "tools": {"webfetch": False},
    }
    if mode == "graph":
        config["mcp"] = {
            "graph": {
                "type": "remote",
                "url": GRAPH_MCP_URL,
                "enabled": True,
                "oauth": False,
                "timeout": 120000,
            }
        }
    return config


@beartype
def config_text(config: dict[str, object]) -> str:
    return json.dumps(config, ensure_ascii=False, indent=2) + "\n"


@beartype
def config_sha256(config: dict[str, object]) -> str:
    import hashlib

    return hashlib.sha256(config_text(config).encode("utf-8")).hexdigest()
