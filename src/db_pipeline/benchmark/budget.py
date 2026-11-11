"""Предохранитель бюджета OpenRouter перед парным прогоном.

Проверяет ключ и остаток кредитов до первого запуска: пустой бюджет — это
отказ с первой секунды на всех парах (``infra_fail_truncated``), а не сигнал о
графе. Проверка дешёвая и не заменяет ретраи в шлюзе, а лишь отсекает заведомо
провальный batch.

Сетевой сбой самой проверки не должен блокировать прогон: при недоступности
upstream статус бюджета помечается ``unknown``, и запуск продолжается — иначе
preflight стал бы новым источником простоев.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import cast

from beartype import beartype

OPENROUTER_KEY_URL = "https://openrouter.ai/api/v1/key"


@beartype
@dataclass(frozen=True)
class BudgetReport:
    """Итог проверки: пускать ли batch и что известно о бюджете."""

    ok: bool
    reason: str = ""
    status: int = 0
    limit: float = 0.0
    usage: float = 0.0
    limit_remaining: float = 0.0
    unknown: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "reason": self.reason,
            "status": self.status,
            "limit": self.limit,
            "usage": self.usage,
            "limit_remaining": self.limit_remaining,
            "unknown": self.unknown,
        }


@beartype
def check_budget(
    api_key: str,
    url: str = OPENROUTER_KEY_URL,
    timeout: float = 20.0,
) -> BudgetReport:
    """Остаток кредитов ключа: пустой бюджет — abort, недоступность — unknown."""
    if api_key == "":
        return BudgetReport(ok=False, reason="OPENROUTER_API_KEY не задан")
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode()
            status = response.status
    except urllib.error.HTTPError as exc:
        # 401/403 — ключ недействителен, это тоже повод не начинать batch.
        if exc.code in (401, 403):
            return BudgetReport(ok=False, reason="ключ отклонён", status=exc.code)
        return BudgetReport(
            ok=True, reason=f"upstream {exc.code}", status=exc.code, unknown=True
        )
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return BudgetReport(ok=True, reason=type(exc).__name__, unknown=True)
    data = _data(body)
    remaining = _float(data.get("limit_remaining"))
    limit = _float(data.get("limit"))
    usage = _float(data.get("usage"))
    if "limit_remaining" in data and remaining <= 0:
        return BudgetReport(
            ok=False,
            reason="бюджет исчерпан",
            status=status,
            limit=limit,
            usage=usage,
            limit_remaining=remaining,
        )
    return BudgetReport(
        ok=True,
        status=status,
        limit=limit,
        usage=usage,
        limit_remaining=remaining,
    )


@beartype
def _data(body: str) -> dict[str, object]:
    try:
        loaded: object = json.loads(body)
    except json.JSONDecodeError:
        return {}
    if not isinstance(loaded, dict):
        return {}
    mapping = cast("dict[str, object]", loaded)
    data = mapping.get("data")
    if isinstance(data, dict):
        return cast("dict[str, object]", data)
    return mapping


@beartype
def _float(value: object) -> float:
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0
