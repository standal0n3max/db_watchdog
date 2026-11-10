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

# base_url не задаём: upstream URL собирается явно, иначе httpx склеивает
# base path и относительный путь и получается дубль ``/api/v1/v1/...``.
CLIENT = httpx.AsyncClient(timeout=600.0)


async def health(_request: Request) -> Response:
    return JSONResponse(
        {
            "ok": True,
            "key_present": API_KEY != "",
            "models": sorted(ALLOWED),
        }
    )


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
    upstream = await CLIENT.post(upstream_url, content=body, headers=headers)
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type", "application/json"),
    )


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
