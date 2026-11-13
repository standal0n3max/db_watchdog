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

# Пустой ответ (finish stop, нет текста и нет tool_calls) повторяется отдельно:
# модель иногда возвращает его вместо вызова инструмента. Счётчик уходит
# заголовком и копится для записи прогона.
EMPTY_RETRIES = int(os.environ.get("BENCH_GATEWAY_EMPTY_RETRIES", "2"))

# Потолок reasoning OpenRouter. Поле ``reasoning.max_tokens`` (не вместе с
# ``effort``): иначе reasoning съедает ``max_tokens`` и агент получает пустой
# ответ. 0 отключает подстановку.
# Сверено с OpenRouter (reasoning tokens): параметр единый для chat completions.
# У DeepSeek это запрос, а не жёсткий clamp — reasoning входит в ``max_tokens``,
# и провайдер может выйти за cap. Такие генерации считает запись прогона.
REASONING_MAX_TOKENS = int(os.environ.get("BENCH_REASONING_MAX_TOKENS", "1024"))

# Последний успешный ответ: провайдер OpenRouter и сумма пустых повторов.
# Прогоны идут по одному шлюзу последовательно, оркестратор снимает дельту.
STATS: dict[str, object] = {
    "provider": "",
    "empty_retries_total": 0,
    "last_empty_retries": 0,
}

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
            "provider": STATS["provider"],
            "empty_retries_total": STATS["empty_retries_total"],
            "last_empty_retries": STATS["last_empty_retries"],
            "reasoning_max_tokens": REASONING_MAX_TOKENS,
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
    prepared = _inject_reasoning(body)
    upstream = await _post_with_retry(upstream_url, prepared, headers)
    empty_retries = 0
    while (
        empty_retries < EMPTY_RETRIES
        and upstream.status_code == 200
        and _is_empty_stop(upstream.content)
    ):
        empty_retries += 1
        upstream = await _post_with_retry(upstream_url, prepared, headers)
    provider = _provider_name(upstream.content)
    STATS["provider"] = provider
    STATS["last_empty_retries"] = empty_retries
    total = STATS["empty_retries_total"]
    STATS["empty_retries_total"] = int(total) + empty_retries if isinstance(total, int) else empty_retries
    response_headers = {
        "X-Bench-Empty-Retries": str(empty_retries),
        "X-OpenRouter-Provider": provider,
    }
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type", "application/json"),
        headers=response_headers,
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


def _inject_reasoning(body: bytes) -> bytes:
    """Кладёт ``reasoning.max_tokens`` в тело, если вызывающий его не задал."""
    if REASONING_MAX_TOKENS <= 0:
        return body
    try:
        loaded: object = json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return body
    if not isinstance(loaded, dict) or "reasoning" in loaded:
        return body
    loaded["reasoning"] = {"max_tokens": REASONING_MAX_TOKENS}
    return json.dumps(loaded).encode("utf-8")


def _is_empty_stop(content: bytes) -> bool:
    """Ответ без текста и без ``tool_calls`` при остановке, а не при обрезке.

    OpenCode ходит через ``@ai-sdk/openai-compatible`` со стримингом, поэтому
    тело часто SSE, а не один JSON. Для не-стримингового ответа остаётся
    прежняя ветка.
    """
    streamed = _sse_objects(content)
    if streamed is not None:
        return _sse_empty_stop(streamed)
    loaded = _json_object(content)
    if loaded is None:
        return False
    return _json_empty_stop(loaded)


def _provider_name(content: bytes) -> str:
    """Фактический провайдер OpenRouter из тела ответа или из чанка SSE."""
    streamed = _sse_objects(content)
    if streamed is not None:
        for item in streamed:
            name = _provider_from_mapping(item)
            if name != "":
                return name
        return ""
    loaded = _json_object(content)
    if loaded is None:
        return ""
    return _provider_from_mapping(loaded)


def _sse_objects(content: bytes) -> list[dict[str, object]] | None:
    """Чанки ``data:`` либо ``None``, если тело не SSE."""
    stripped = content.lstrip()
    if not stripped.startswith(b"data:"):
        return None
    found: list[dict[str, object]] = []
    for raw_line in stripped.decode("utf-8", "replace").splitlines():
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        data = line[len("data:") :].strip()
        if data == "" or data == "[DONE]":
            continue
        try:
            loaded: object = json.loads(data)
        except json.JSONDecodeError:
            continue
        if isinstance(loaded, dict):
            raw = loaded
            found.append({str(key): value for key, value in raw.items()})
    return found


def _json_object(content: bytes) -> dict[str, object] | None:
    try:
        loaded: object = json.loads(content.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(loaded, dict):
        return None
    raw = loaded
    return {str(key): value for key, value in raw.items()}


def _json_empty_stop(loaded: dict[str, object]) -> bool:
    choices = loaded.get("choices")
    if not isinstance(choices, list) or len(choices) == 0:
        return True
    first = choices[0]
    if not isinstance(first, dict):
        return False
    finish = str(first.get("finish_reason") or "")
    if finish not in ("stop", ""):
        return False
    message = first.get("message")
    if not isinstance(message, dict):
        return True
    return _message_is_empty(message)


def _sse_empty_stop(chunks: list[dict[str, object]]) -> bool:
    """Пустой stop: в дельтах нет текста и нет ``tool_calls``, финал — ``stop``."""
    text = ""
    tools = False
    finish = ""
    for chunk in chunks:
        choices = chunk.get("choices")
        if not isinstance(choices, list) or len(choices) == 0:
            continue
        first = choices[0]
        if not isinstance(first, dict):
            continue
        reason = str(first.get("finish_reason") or "")
        if reason not in ("", "null"):
            finish = reason
        payload = first.get("delta")
        if not isinstance(payload, dict):
            payload = first.get("message")
        if not isinstance(payload, dict):
            continue
        if not _message_is_empty(payload):
            content = payload.get("content")
            if isinstance(content, str):
                text += content
            else:
                return False
            calls = payload.get("tool_calls")
            if isinstance(calls, list) and len(calls) > 0:
                tools = True
    if finish != "stop":
        return False
    return text.strip() == "" and not tools


def _message_is_empty(message: dict[str, object]) -> bool:
    text = message.get("content")
    if isinstance(text, str) and text.strip() != "":
        return False
    if isinstance(text, list) and len(text) > 0:
        return False
    tools = message.get("tool_calls")
    return not isinstance(tools, list) or len(tools) == 0


def _provider_from_mapping(loaded: dict[str, object]) -> str:
    provider = loaded.get("provider")
    if isinstance(provider, str):
        return provider
    if isinstance(provider, dict):
        name = provider.get("name") or provider.get("id")
        return name if isinstance(name, str) else ""
    return ""


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
