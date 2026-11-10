"""model-gateway: прокси к OpenRouter с allowlist моделей.

Ключ ``OPENROUTER_API_KEY`` читается только здесь. Task-container обращается к
шлюзу по внутренней сети и ключа не получает. Логируется только маршрут и
статус, без заголовков авторизации и тела.
"""

from __future__ import annotations

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

CLIENT = httpx.AsyncClient(base_url=UPSTREAM, timeout=600.0)


async def proxy(request: Request) -> Response:
    if API_KEY == "":
        return JSONResponse({"error": "gateway has no key"}, status_code=500)
    path = request.url.path
    if path.endswith("/models") and request.method == "GET":
        return JSONResponse({"data": [{"id": name} for name in sorted(ALLOWED)]})
    body = await request.body()
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": request.headers.get("content-type", "application/json"),
    }
    upstream = await CLIENT.request(request.method, path, content=body, headers=headers)
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type", "application/json"),
    )


async def health(_request: Request) -> Response:
    return JSONResponse({"ok": True, "key_present": API_KEY != ""})


app = Starlette(
    routes=[
        Route("/health", health, methods=["GET"]),
        Route("/v1/{path:path}", proxy, methods=["GET", "POST"]),
    ]
)
