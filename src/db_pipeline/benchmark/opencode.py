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

# Потолок ответа модели. 2048 вместе с reasoning часто давал пустой ответ:
# рассуждение съедало весь бюджет. 8192 оставляет место и ответу, и вызову
# инструмента; reasoning отдельно режется шлюзом.
DEFAULT_MAX_TOKENS = 8192

# Реальный лимит шагов агента: попадает в ``agent.build.steps`` эффективного
# конфига. ``opencode run`` собственного флага лимита не имеет.
DEFAULT_STEP_LIMIT = 80

# Длина контекста по умолчанию. Реальное значение берётся из
# ``BENCH_CONTEXT_LENGTH`` и сверяется с карточкой модели OpenRouter.
DEFAULT_CONTEXT_LENGTH = 32768

# Руки прогона. ``graph`` подключает MCP графа, ``bare`` — нет.
MODES = ("bare", "graph")

# Руки, которым нужен MCP-сервер графа в сети прогона.
GRAPH_MODES = ("graph",)


@beartype
@dataclass(frozen=True)
class AgentConfig:
    provider: str = "pgpro"
    model: str = ""
    temperature: float = 0.0
    context_length: int = DEFAULT_CONTEXT_LENGTH
    max_tokens: int = DEFAULT_MAX_TOKENS
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
    compaction: bool = False
    started: int = 0
    ended: int = 0


@beartype
@dataclass
class AgentTranscript:
    generations: list[Generation] = field(default_factory=list[Generation])
    tools: list[ToolCall] = field(default_factory=list[ToolCall])
    errors: list[str] = field(default_factory=list[str])
    raw_events: int = 0
    session_id: str = ""

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


# Синтетическое сообщение OpenCode v1.18 после сводки компакции.
# Сверено с session/compaction.ts: user-part с ``synthetic`` и этим текстом.
CONTINUE_MARKER = (
    "Continue if you have next steps, or stop and ask for clarification "
    "if you are unsure how to proceed."
)

# Инструменты, меняющие дерево. Совпадает с классификатором завершения.
_EDIT_TOOLS = frozenset(
    {"edit", "write", "patch", "multiedit", "apply_patch", "str_replace"}
)


@beartype
def parse_jsonl(text: str) -> AgentTranscript:
    """Разбирает JSONL ``opencode run --format json``.

    Пара «сводка + синтетическое Continue» помечает генерацию компакции:
    у неё нет ``step_finish`` у сообщения Continue, а предыдущая завершённая
    генерация — это сводка. Такое сообщение в список генераций не входит.
    """
    transcript = AgentTranscript()
    events = _load_events(text)
    transcript.raw_events = len(events)
    continue_ids = _continue_message_ids(events)
    by_message: dict[str, Generation] = {}
    last_finished = ""
    for event in events:
        if transcript.session_id == "":
            session_id = str(event.get("sessionID") or "")
            if session_id != "":
                transcript.session_id = session_id
        kind = event.get("type")
        part = _as_dict(event.get("part"))
        if len(part) == 0:
            if kind == "error":
                transcript.errors.append(str(event.get("error") or "error")[:500])
            continue
        message_id = str(part.get("messageID") or part.get("id") or "")
        if kind == "text" and message_id in continue_ids:
            if last_finished in by_message:
                by_message[last_finished].compaction = True
            continue
        stamp = _int(event.get("timestamp"))
        if kind == "step_finish":
            _absorb_step(transcript, by_message, part, stamp)
            if message_id != "":
                last_finished = message_id
        elif kind == "tool_use":
            _absorb_tool(transcript, part)
        elif kind == "text":
            _absorb_text(by_message, part, stamp)
        elif kind == "error":
            message = part.get("error") or part.get("message") or "error"
            transcript.errors.append(str(message)[:500])
    transcript.generations = list(by_message.values())
    return transcript


@beartype
def metrics(transcript: AgentTranscript) -> dict[str, int | str]:
    """Поля записи прогона: компакции, пик контекста и шаг первой правки.

    ``max_context`` — максимум ``input + cache_read`` по генерациям.
    ``first_edit_step`` — номер настоящего шага (без компакции), 1-based,
    либо ``-1``, если правки не было.
    """
    peak = 0
    compactions = 0
    for generation in transcript.generations:
        peak = max(peak, generation.input_tokens + generation.cache_read)
        if generation.compaction:
            compactions += 1
    return {
        "compactions": compactions,
        "max_context": peak,
        "first_edit_step": first_edit_step(transcript),
        "session_id": transcript.session_id,
    }


@beartype
def first_edit_step(transcript: AgentTranscript) -> int:
    """Номер шага агента с первой правкой. Компакции в счёт не идут."""
    edit_message = ""
    for tool in transcript.tools:
        short = tool.tool.rsplit("_", 1)[-1]
        if tool.tool in _EDIT_TOOLS or short in _EDIT_TOOLS:
            edit_message = tool.message_id
            break
    if edit_message == "":
        return -1
    step = 0
    for generation in transcript.generations:
        if generation.compaction:
            continue
        step += 1
        if generation.message_id == edit_message:
            return step
    return -1


@beartype
def agent_steps(transcript: AgentTranscript) -> int:
    """Число генераций без сводок компакции: по ним считается лимит шагов."""
    return sum(1 for generation in transcript.generations if not generation.compaction)


@beartype
def _load_events(text: str) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for line in text.splitlines():
        line = line.strip()
        if line == "" or not line.startswith("{"):
            continue
        try:
            loaded: object = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(loaded, dict):
            events.append(_as_dict(cast("object", loaded)))
    return events


@beartype
def _continue_message_ids(events: list[dict[str, object]]) -> set[str]:
    """Идентификаторы синтетического Continue: у них нет ``step_finish``."""
    finished: set[str] = set()
    candidates: set[str] = set()
    for event in events:
        part = _as_dict(event.get("part"))
        if len(part) == 0:
            continue
        message_id = str(part.get("messageID") or part.get("id") or "")
        if message_id == "":
            continue
        if event.get("type") == "step_finish":
            finished.add(message_id)
        elif event.get("type") == "text" and _is_continue_part(part):
            candidates.add(message_id)
    return candidates - finished


@beartype
def _is_continue_part(part: dict[str, object]) -> bool:
    text = part.get("text")
    if not isinstance(text, str) or CONTINUE_MARKER not in text:
        return False
    if part.get("synthetic") is True:
        return True
    metadata = _as_dict(part.get("metadata"))
    return metadata.get("compaction_continue") is True or text.strip().endswith(CONTINUE_MARKER)


@beartype
def _absorb_step(
    transcript: AgentTranscript,
    by_message: dict[str, Generation],
    part: dict[str, object],
    stamp: int = 0,
) -> None:
    message_id = str(part.get("messageID") or part.get("id") or "")
    generation = by_message.setdefault(message_id, Generation(message_id=message_id))
    _touch(generation, stamp)
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
def _absorb_text(
    by_message: dict[str, Generation], part: dict[str, object], stamp: int = 0
) -> None:
    message_id = str(part.get("messageID") or part.get("id") or "")
    generation = by_message.setdefault(message_id, Generation(message_id=message_id))
    _touch(generation, stamp)
    text = part.get("text")
    if isinstance(text, str):
        generation.text += text


@beartype
def _touch(generation: Generation, stamp: int) -> None:
    if stamp <= 0:
        return
    if generation.started == 0 or stamp < generation.started:
        generation.started = stamp
    if stamp > generation.ended:
        generation.ended = stamp


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


# Одинаковый для обеих рук: сессия продолжается, если diff ещё пустой.
NUDGE_PROMPT = "No files have been changed yet. Continue the task."


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


@beartype
def continue_command(config: AgentConfig, session_id: str, prompt: str) -> list[str]:
    """Продолжает ту же сессию OpenCode: флаг ``--session`` есть в v1.18."""
    command = run_command(config, prompt)
    prompt_text = command.pop()
    command.extend(["--session", session_id, prompt_text])
    return command


@beartype
def append_transcript(base: AgentTranscript, extra: AgentTranscript) -> None:
    """Дописывает события продолжения сессии, не затирая уже разобранные шаги."""
    base.generations.extend(extra.generations)
    base.tools.extend(extra.tools)
    base.errors.extend(extra.errors)
    base.raw_events += extra.raw_events
    if base.session_id == "":
        base.session_id = extra.session_id


@beartype
def should_nudge(
    transcript: AgentTranscript,
    step_limit: int,
    diff_empty: bool,
    timed_out: bool,
) -> bool:
    """Пустой stop и ещё есть шаги: можно один раз продолжить сессию."""
    if timed_out or not diff_empty or transcript.session_id == "":
        return False
    if len(transcript.generations) == 0:
        return False
    if step_limit > 0 and agent_steps(transcript) >= step_limit:
        return False
    return transcript.generations[-1].reason == "stop"


GRAPH_MCP_URL = "http://graph:8765/mcp"
CONFIG_PATH = "/home/bench/.config/opencode/opencode.jsonc"


@beartype
def build_config(
    mode: str,
    provider: str,
    model: str,
    context_length: int = DEFAULT_CONTEXT_LENGTH,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    step_limit: int = DEFAULT_STEP_LIMIT,
    temperature: float = 0.0,
) -> dict[str, object]:
    """Строит эффективный OpenCode-конфиг: bare без MCP, graph с remote MCP.

    Секретов в конфиге нет: ключ провайдера подставляется из
    ``POSTGRES_GYM_PROVIDER_KEY`` на стороне контейнера. ``graph`` подключает
    все семь инструментов по Streamable HTTP с увеличенным таймаутом.

    Формат сверен с OpenCode v1.18: ``agent.build.temperature`` задаёт
    температуру primary-агента, ``agent.build.steps`` — лимит ходов,
    ``agent.build.permission.task`` запрещает субагента ``task`` (его шаги и
    MCP-вызовы в запись прогона не попадают). ``limit.output`` — потолок
    ответа модели.
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
        "agent": {
            "build": {
                "steps": step_limit,
                "temperature": temperature,
                "permission": {"task": {"*": "deny"}},
            }
        },
        "permission": {"webfetch": "deny", "external_directory": "deny"},
        "tools": {"webfetch": False},
    }
    if mode in GRAPH_MODES:
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
