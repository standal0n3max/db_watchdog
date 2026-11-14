"""Ретраи model-gateway на 402/429 без реального upstream.

Модуль шлюза лежит вне пакета (``docker/model-gateway/gateway.py``), поэтому
загружается по пути. Проверяется, что отказ ``in_flight_budget_exhausted`` и
троттлинг повторяются с учётом ``Retry-After``, а успешный ответ не повторяется.

Тест пропускается, если в окружении нет ``starlette`` или ``httpx``: в рантайме
шлюза они есть (устанавливаются его Dockerfile), в тестовом venv могут быть не
всегда.
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import types
import unittest
from collections.abc import Coroutine
from typing import Protocol, cast

import pytest

from db_pipeline.benchmark import paths

GATEWAY_PATH = paths.docker_dir() / "model-gateway" / "gateway.py"


class HttpResponse(Protocol):
    """Минимальный вид ответа httpx, нужный тесту (сам httpx в deps проекта нет)."""

    status_code: int


class GatewayModule(Protocol):
    """Вид загруженного модуля шлюза, нужный тесту."""

    CLIENT: object
    MAX_RETRY_WAIT: float
    BASE_BACKOFF: float

    # Функция асинхронная: тест оборачивает её в asyncio.run.
    def _post_with_retry(
        self, url: str, body: bytes, headers: dict[str, str]
    ) -> Coroutine[None, None, HttpResponse]: ...

    def _retry_delay(self, response: object, attempt: int) -> float: ...

    def _is_empty_stop(self, content: bytes) -> bool: ...

    def _provider_name(self, content: bytes) -> str: ...

    def _inject_reasoning(self, body: bytes) -> bytes: ...

    def _inject_provider(self, body: bytes) -> bytes: ...

    def _observe_provider(self, content: bytes) -> str: ...

    OPENROUTER_PROVIDER: str
    STATS: dict[str, object]


def _load_gateway() -> types.ModuleType | None:
    if not GATEWAY_PATH.is_file():
        return None
    if importlib.util.find_spec("httpx") is None:
        return None
    if importlib.util.find_spec("starlette") is None:
        return None
    spec = importlib.util.spec_from_file_location("bench_gateway_under_test", GATEWAY_PATH)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeClient:
    """Подменяет httpx.AsyncClient: отдаёт заранее заданную очередь ответов."""

    def __init__(self, statuses: list[int], retry_after: str = "") -> None:
        self._statuses = list(statuses)
        self._retry_after = retry_after
        self.calls = 0

    async def post(self, _url: str, content: bytes = b"", headers: object = None) -> object:
        import httpx

        index = min(self.calls, len(self._statuses) - 1)
        status = self._statuses[index]
        self.calls += 1
        headers_map = {"retry-after": self._retry_after} if self._retry_after else {}
        return httpx.Response(status, content=b"{}", headers=headers_map)


@pytest.mark.skipif(_load_gateway() is None, reason="нет starlette/httpx или файла шлюза")
class GatewayRetryTests(unittest.TestCase):
    module: GatewayModule

    @classmethod
    def setUpClass(cls) -> None:
        loaded = _load_gateway()
        if loaded is None:
            raise unittest.SkipTest("шлюз недоступен")
        cls.module = cast("GatewayModule", loaded)

    def setUp(self) -> None:
        os.environ["BENCH_GATEWAY_MAX_ATTEMPTS"] = "4"
        self.module.MAX_RETRY_WAIT = 0.0
        self.module.BASE_BACKOFF = 0.0
        self.module.OPENROUTER_PROVIDER = "deepinfra"
        self.module.STATS["provider"] = ""
        self.module.STATS["requests"] = 0
        self.module.STATS["provider_ok"] = True
        self.module.STATS["provider_mismatch"] = False

    def _retry(self, client: _FakeClient) -> HttpResponse:
        self.module.CLIENT = client
        return asyncio.run(self.module._post_with_retry("http://x", b"{}", {}))  # pyright: ignore[reportPrivateUsage]

    def test_retries_on_402_and_429(self) -> None:
        client = _FakeClient([402, 429, 200])
        response = self._retry(client)
        self.assertEqual(client.calls, 3)
        self.assertEqual(response.status_code, 200)

    def test_returns_error_after_exhausting_attempts(self) -> None:
        client = _FakeClient([402])
        response = self._retry(client)
        self.assertEqual(response.status_code, 402)
        self.assertEqual(client.calls, 4)

    def test_success_is_not_retried(self) -> None:
        client = _FakeClient([200])
        self._retry(client)
        self.assertEqual(client.calls, 1)

    def test_non_retryable_error_is_returned_once(self) -> None:
        client = _FakeClient([400])
        response = self._retry(client)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(client.calls, 1)

    def test_retry_after_header_is_honoured(self) -> None:
        import httpx

        self.module.MAX_RETRY_WAIT = 120.0
        response = httpx.Response(429, headers={"retry-after": "7"})
        delay = self.module._retry_delay(response, attempt=1)  # pyright: ignore[reportPrivateUsage]
        self.assertAlmostEqual(delay, 7.0)

    def test_retry_after_is_capped(self) -> None:
        import httpx

        self.module.MAX_RETRY_WAIT = 5.0
        response = httpx.Response(429, headers={"retry-after": "600"})
        delay = self.module._retry_delay(response, attempt=1)  # pyright: ignore[reportPrivateUsage]
        self.assertAlmostEqual(delay, 5.0)

    def test_missing_retry_after_uses_backoff(self) -> None:
        import httpx

        self.module.BASE_BACKOFF = 2.0
        self.module.MAX_RETRY_WAIT = 120.0
        response = httpx.Response(402)
        delay = self.module._retry_delay(response, attempt=2)  # pyright: ignore[reportPrivateUsage]
        self.assertAlmostEqual(delay, 4.0)

    def test_empty_stop_has_no_text_and_no_tools(self) -> None:
        empty = b'{"choices":[{"finish_reason":"stop","message":{"content":"","tool_calls":[]}}]}'
        filled = b'{"choices":[{"finish_reason":"stop","message":{"content":"ok"}}]}'
        tools = b'{"choices":[{"finish_reason":"stop","message":{"content":null,"tool_calls":[{"id":"1"}]}}]}'
        length = b'{"choices":[{"finish_reason":"length","message":{"content":""}}]}'
        self.assertTrue(self.module._is_empty_stop(empty))  # pyright: ignore[reportPrivateUsage]
        self.assertFalse(self.module._is_empty_stop(filled))  # pyright: ignore[reportPrivateUsage]
        self.assertFalse(self.module._is_empty_stop(tools))  # pyright: ignore[reportPrivateUsage]
        self.assertFalse(self.module._is_empty_stop(length))  # pyright: ignore[reportPrivateUsage]

    def test_sse_empty_stop_tool_calls_and_text(self) -> None:
        empty = (
            b'data: {"provider":"DeepInfra","choices":[{"delta":{"role":"assistant"},"finish_reason":null}]}\n'
            b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n'
            b"data: [DONE]\n"
        )
        text = (
            b'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":null}]}\n'
            b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n'
            b"data: [DONE]\n"
        )
        tools = (
            b'data: {"choices":[{"delta":{"tool_calls":[{"id":"1"}]},"finish_reason":"tool_calls"}]}\n'
            b"data: [DONE]\n"
        )
        self.assertTrue(self.module._is_empty_stop(empty))  # pyright: ignore[reportPrivateUsage]
        self.assertFalse(self.module._is_empty_stop(text))  # pyright: ignore[reportPrivateUsage]
        self.assertFalse(self.module._is_empty_stop(tools))  # pyright: ignore[reportPrivateUsage]
        self.assertEqual(self.module._provider_name(empty), "DeepInfra")  # pyright: ignore[reportPrivateUsage]

    def test_json_provider_still_read_from_body(self) -> None:
        body = b'{"provider":"Together","choices":[{"finish_reason":"stop","message":{"content":"x"}}]}'
        self.assertEqual(self.module._provider_name(body), "Together")  # pyright: ignore[reportPrivateUsage]
        self.assertFalse(self.module._is_empty_stop(body))  # pyright: ignore[reportPrivateUsage]

    def test_reasoning_cap_is_injected_once(self) -> None:
        import json

        self.module.REASONING_MAX_TOKENS = 1024  # type: ignore[attr-defined]
        body = self.module._inject_reasoning(b'{"model":"m"}')  # pyright: ignore[reportPrivateUsage]
        loaded = json.loads(body)
        self.assertEqual(loaded["reasoning"], {"max_tokens": 1024})
        again = self.module._inject_reasoning(body)  # pyright: ignore[reportPrivateUsage]
        self.assertEqual(json.loads(again)["reasoning"], {"max_tokens": 1024})

    def test_provider_pin_disables_fallbacks(self) -> None:
        import json

        body = self.module._inject_provider(  # pyright: ignore[reportPrivateUsage]
            b'{"model":"m","provider":{"order":["together"],"allow_fallbacks":true}}'
        )
        loaded = json.loads(body)
        self.assertEqual(loaded["provider"]["only"], ["deepinfra"])
        self.assertIs(loaded["provider"]["allow_fallbacks"], False)
        self.assertEqual(loaded["provider"]["order"], ["together"])

    def test_sse_and_json_provider_mismatch(self) -> None:
        sse = (
            b'data: {"provider":"DeepInfra","choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n'
            b"data: [DONE]\n"
        )
        self.assertEqual(self.module._observe_provider(sse), "DeepInfra")  # pyright: ignore[reportPrivateUsage]
        self.assertFalse(self.module.STATS["provider_mismatch"])
        body = b'{"provider":"Together","choices":[{"finish_reason":"stop","message":{"content":"x"}}]}'
        self.assertEqual(self.module._observe_provider(body), "Together")  # pyright: ignore[reportPrivateUsage]
        self.assertTrue(self.module.STATS["provider_mismatch"])
        self.assertFalse(self.module.STATS["provider_ok"])
        self.assertEqual(self.module.STATS["requests"], 2)
        self.assertEqual(self.module.STATS["provider"], "Together")


if __name__ == "__main__":
    unittest.main()
