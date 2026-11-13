"""Классификатор завершения benchmark-прогона.

Один и тот же отказ ``statusCode:402`` маскирует два разных диагноза, поэтому
они разведены:

- ``infra_fail_truncated`` — генераций нет вовсе: ключ не «вмещал» запрос
  (``requires more credits, or fewer max_tokens``), отказ с первой секунды,
  лечится уменьшением ``max_tokens``;
- ``infra_fail_wall`` — работа шёл, потом упёрся в потолок ключа
  (``in_flight_budget_exhausted``), лечится ретраями и предохранителем бюджета.

Категории API-отказов не должны попадать в McNemar: иначе измеряется надёжность
шлюза, а не качество графа. В пары идут только ``ok`` и ``self_stop_empty``.
``length_partial`` (ответ обрезан, но текст ненулевой) считается отдельно и в
пары не входит: это не полный ответ модели. ``compaction_stop`` — финальная
генерация сразу после сводки компакции и пустой diff; в пары не входит.

Признаки берутся из run record: число генераций, ``reason`` последней генерации,
маркер бюджетной ошибки в ``agent_jsonl`` и наличие изменяющего инструмента в
``tool_events``. Оценка чистых прогонов уже записана, поэтому классификатор
работает на сохранённом JSON без Docker и сети.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from beartype import beartype

# Инструменты, меняющие код: по ним видно, что агент вообще что-то правил.
EDIT_TOOLS = frozenset(
    {"edit", "write", "patch", "multiedit", "apply_patch", "str_replace"}
)

# Маркеры исчерпания бюджета ключа OpenRouter в теле ответа upstream.
BUDGET_MARKERS = (
    "in_flight_budget_exhausted",
    "requires more credits",
    "exceed your available credits",
)

OK_OUTCOMES = frozenset({"ok", "self_stop_empty"})
INFRA_OUTCOMES = frozenset({"infra_fail_truncated", "infra_fail_wall"})
OUTCOMES = (
    "ok",
    "infra_fail_truncated",
    "infra_fail_wall",
    "length",
    "length_partial",
    "step_limit",
    "compaction_stop",
    "self_stop_empty",
    "timeout",
    "crash_unknown",
)


@beartype
@dataclass(frozen=True)
class Completion:
    """Итог классификации: категория и короткое пояснение для отчёта."""

    outcome: str
    generations: int = 0
    reason: str = ""
    detail: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "outcome": self.outcome,
            "generations": self.generations,
            "reason": self.reason,
            "detail": self.detail,
        }


@beartype
def classify(record: dict[str, object], step_limit: int = 0) -> Completion:
    """Определяет категорию завершения по run record.

    Порядок проверок: отсутствие генераций → потолок ответа → бюджет →
    лимит настоящих шагов (без генераций компакции) → остановка сразу после
    компакции → самоостановка без правки → успех.
    """
    if record.get("agent_timeout") is True:
        generations = _generations(record)
        return Completion(
            outcome="timeout",
            generations=len(generations),
            reason="timeout",
            detail="агент не уложился в таймаут",
        )
    generations = _generations(record)
    jsonl = str(record.get("agent_jsonl") or "")
    budget = _has_budget_error(jsonl)
    if len(generations) == 0:
        if budget:
            return Completion(
                outcome="infra_fail_truncated",
                detail="нет генераций: ключ не вместил запрос",
            )
        return Completion(
            outcome="crash_unknown",
            detail="нет генераций и нет маркера бюджета",
        )

    last_reason = _last_reason(generations)
    output_tokens = _int((generations[-1] or {}).get("output_tokens"))
    if last_reason == "length" and output_tokens == 0:
        return Completion(
            outcome="length",
            generations=len(generations),
            reason=last_reason,
            detail="последняя генерация обрезана потолком ответа",
        )
    if last_reason == "length" and output_tokens > 0:
        return Completion(
            outcome="length_partial",
            generations=len(generations),
            reason=last_reason,
            detail="ответ обрезан, но текст ненулевой",
        )
    if budget:
        return Completion(
            outcome="infra_fail_wall",
            generations=len(generations),
            reason=last_reason,
            detail="прогон упёрся в потолок бюджета ключа",
        )
    agent_steps = _agent_steps(generations)
    if step_limit > 0 and len(agent_steps) >= step_limit:
        return Completion(
            outcome="step_limit",
            generations=len(generations),
            reason=last_reason,
            detail=f"достигнут лимит шагов {step_limit}",
        )
    if _compaction_stop(generations, record):
        return Completion(
            outcome="compaction_stop",
            generations=len(generations),
            reason=last_reason,
            detail="остановка сразу после компакции, diff пустой",
        )
    if last_reason == "stop" and not _edited(record):
        return Completion(
            outcome="self_stop_empty",
            generations=len(generations),
            reason=last_reason,
            detail="агент остановился, не сделав правок",
        )
    return Completion(
        outcome="ok",
        generations=len(generations),
        reason=last_reason,
        detail="",
    )


@beartype
def outcome(record: dict[str, object], step_limit: int = 0) -> str:
    return classify(record, step_limit).outcome


@beartype
def countable(record: dict[str, object], step_limit: int = 0) -> bool:
    """Идёт ли прогон в пары McNemar: API-отказы и лимиты исключаются."""
    return classify(record, step_limit).outcome in OK_OUTCOMES


@beartype
def summarize(records: list[dict[str, object]], step_limit: int = 0) -> dict[str, int]:
    """Счётчик по категориям, включая нулевые: отчёт должен быть полным."""
    counts: dict[str, int] = {name: 0 for name in OUTCOMES}
    for record in records:
        result = classify(record, step_limit).outcome
        counts[result] = counts.get(result, 0) + 1
    return counts


@beartype
def _generations(record: dict[str, object]) -> list[dict[str, object]]:
    raw = record.get("generations")
    if not isinstance(raw, list):
        return []
    found: list[dict[str, object]] = []
    for item in cast("list[object]", raw):
        if isinstance(item, dict):
            found.append(cast("dict[str, object]", item))
    return found


@beartype
def _last_reason(generations: list[dict[str, object]]) -> str:
    return str(generations[-1].get("reason") or "")


@beartype
def _agent_steps(generations: list[dict[str, object]]) -> list[dict[str, object]]:
    """Генерации агента: сводка компакции шагом не считается."""
    return [item for item in generations if item.get("compaction") is not True]


@beartype
def _compaction_stop(
    generations: list[dict[str, object]], record: dict[str, object]
) -> bool:
    """Финальная генерация идёт сразу после компакции, правок нет."""
    if len(generations) < 2:
        return False
    if generations[-2].get("compaction") is not True:
        return False
    if _last_reason(generations) != "stop":
        return False
    if _edited(record):
        return False
    return _int(record.get("diff_size")) == 0


@beartype
def _has_budget_error(jsonl: str) -> bool:
    return any(marker in jsonl for marker in BUDGET_MARKERS)


@beartype
def _edited(record: dict[str, object]) -> bool:
    raw = record.get("tool_events")
    if not isinstance(raw, list):
        return False
    for item in cast("list[object]", raw):
        if not isinstance(item, dict):
            continue
        name = str(cast("dict[str, object]", item).get("tool") or "")
        if name in EDIT_TOOLS or name.rsplit("_", 1)[-1] in EDIT_TOOLS:
            return True
    return False


@beartype
def _int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0
