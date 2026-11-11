"""model-gateway: прокси к OpenRouter с allowlist моделей.

Ключ ``OPENROUTER_API_KEY`` читается только здесь. Task-container обращается к
шлюзу по внутренней сети и ключа не получает. Логируется только маршрут и
статус, без заголовков авторизации и тела.

Разрешены только нужные OpenAI-compatible endpoints: ``GET /v1/models`` и
``POST /v1/chat/completions``. Для чата поле ``model`` обязано входить в
allowlist ``BENCH_ALLOWED_MODELS``; иначе шлюз отвечает 403 и upstream не
вызывается. ``GET /health`` используется как readiness probe.
"""

from __future__ import annotations

import asyncio
import json
import os

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

UPSTREAM = os.environ.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
ALLOWED = {
    item.strip()
    for item in os.environ.get("BENCH_ALLOWED_MODELS", "").split(",")
    if item.strip()
}

# Локальные маршруты шлюза (клиент ходит сюда) и путь upstream OpenRouter.
CHAT_PATH = "/v1/chat/completions"
MODELS_PATH = "/v1/models"
UPSTREAM_CHAT_PATH = "/chat/completions"

# Повтор при исчерпании бюджета (402) и троттлинге (429). Ретрай на уровне
# шлюза сохраняет семантику трассы: один запрос — одна генерация. OpenCode сам
# такие ответы не повторяет (``isRetryable: false``).
RETRY_STATUSES = frozenset({402, 429})
MAX_ATTEMPTS = int(os.environ.get("BENCH_GATEWAY_MAX_ATTEMPTS", "4"))
MAX_RETRY_WAIT = float(os.environ.get("BENCH_GATEWAY_MAX_RETRY_WAIT", "120"))
BASE_BACKOFF = float(os.environ.get("BENCH_GATEWAY_BASE_BACKOFF", "2"))

# base_url не задаём: upstream URL собирается явно, иначе httpx склеивает
# base path и относительный путь и получается дубль ``/api/v1/v1/...``.
CLIENT = httpx.AsyncClient(timeout=600.0)


async def health(_request: Request) -> Response:
    """Readiness плюс остаток кредитов: оркестратор проверяет его до batch."""
    return JSONResponse(
        {
            "ok": True,
            "key_present": API_KEY != "",
            "models": sorted(ALLOWED),
            "budget_remaining": await _budget_remaining(),
        }
    )


async def _budget_remaining() -> float | None:
    """Остаток кредитов ключа; ``None``, если upstream недоступен или не отдал его."""
    if API_KEY == "":
        return None
    try:
        response = await CLIENT.get(
            f"{UPSTREAM.rstrip('/')}/key",
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=15.0,
        )
        loaded: object = json.loads(response.content.decode())
    except (httpx.HTTPError, ValueError):
        return None
    if not isinstance(loaded, dict):
        return None
    data = loaded.get("data")
    if not isinstance(data, dict):
        data = loaded
    value = data.get("limit_remaining")
    return float(value) if isinstance(value, (int, float)) else None


async def models(_request: Request) -> Response:
    return JSONResponse({"data": [{"id": name} for name in sorted(ALLOWED)]})


def _requested_model(body: bytes) -> str:
    try:
        loaded: object = json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return ""
    if not isinstance(loaded, dict):
        return ""
    model = loaded.get("model")
    return model if isinstance(model, str) else ""


async def chat(request: Request) -> Response:
    if API_KEY == "":
        return JSONResponse({"error": "gateway has no key"}, status_code=500)
    body = await request.body()
    model = _requested_model(body)
    if len(ALLOWED) > 0 and model not in ALLOWED:
        return JSONResponse(
            {"error": "model not allowed", "model": model},
            status_code=403,
        )
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": request.headers.get("content-type", "application/json"),
    }
    upstream_url = f"{UPSTREAM.rstrip('/')}{UPSTREAM_CHAT_PATH}"
    upstream = await _post_with_retry(upstream_url, body, headers)
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type", "application/json"),
    )


async def _post_with_retry(
    url: str, body: bytes, headers: dict[str, str]
) -> httpx.Response:
    """Повторяет запрос при 402/429 с учётом ``Retry-After``.

    Возвращается последний ответ upstream, даже если это отказ: вызывающий
    клиент получит тот же статус, что и без ретраев, но у шлюза будет шанс
    дождаться спада параллельных резервирований.
    """
    response = await CLIENT.post(url, content=body, headers=headers)
    for attempt in range(1, MAX_ATTEMPTS):
        if response.status_code not in RETRY_STATUSES:
            return response
        delay = _retry_delay(response, attempt)
        if delay > 0:
            await asyncio.sleep(delay)
        response = await CLIENT.post(url, content=body, headers=headers)
    return response


def _retry_delay(response: httpx.Response, attempt: int) -> float:
    """Пауза перед повтором: ``Retry-After`` либо экспоненциальный откат.

    ``Retry-After: 0`` означает немедленный повтор — это тоже повтор, поэтому
    нулевая пауза не отменяет попытку.
    """
    header = response.headers.get("retry-after", "")
    if header.strip() != "":
        try:
            return max(0.0, min(float(header), MAX_RETRY_WAIT))
        except ValueError:
            pass
    return max(0.0, min(BASE_BACKOFF**attempt, MAX_RETRY_WAIT))


async def not_found(_request: Request) -> Response:
    return JSONResponse({"error": "endpoint not allowed"}, status_code=404)


app = Starlette(
    routes=[
        Route("/health", health, methods=["GET"]),
        Route(MODELS_PATH, models, methods=["GET"]),
        Route(CHAT_PATH, chat, methods=["POST"]),
        Route("/{path:path}", not_found, methods=["GET", "POST", "PUT", "DELETE", "PATCH"]),
    ]
)
