"""Предохранитель бюджета OpenRouter: разбор ответа ``/key`` без сети.

Проверяется, что пустой бюджет и отклонённый ключ останавливают batch, а
недоступность upstream не блокирует прогон (помечается ``unknown``).
"""

from __future__ import annotations

import email.message
import io
import unittest
import urllib.error
import urllib.request

from db_pipeline.benchmark import budget


class _FakeResponse(io.BytesIO):
    """Минимальный ответ urlopen: тело и статус, контекстный менеджер."""

    def __init__(self, payload: str, status: int) -> None:
        super().__init__(payload.encode())
        self.status = status

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, _kind: object, _value: object, _traceback: object) -> None:
        return None


class BudgetRequestTests(unittest.TestCase):
    def _use_response(self, payload: str, status: int = 200) -> None:
        original = urllib.request.urlopen

        def opener(_request: object, timeout: float = 0) -> _FakeResponse:
            return _FakeResponse(payload, status)

        urllib.request.urlopen = opener  # type: ignore[assignment]
        self.addCleanup(setattr, urllib.request, "urlopen", original)

    def _use_error(self, error: Exception) -> None:
        original = urllib.request.urlopen

        def opener(_request: object, timeout: float = 0) -> None:
            raise error

        urllib.request.urlopen = opener  # type: ignore[assignment]
        self.addCleanup(setattr, urllib.request, "urlopen", original)

    def test_missing_key_is_blocked(self) -> None:
        report = budget.check_budget("")
        self.assertFalse(report.ok)
        self.assertEqual(report.reason, "OPENROUTER_API_KEY не задан")

    def test_empty_remaining_blocks_batch(self) -> None:
        self._use_response('{"data": {"limit": 10.0, "usage": 10.0, "limit_remaining": 0.0}}')
        report = budget.check_budget("key")
        self.assertFalse(report.ok)
        self.assertEqual(report.reason, "бюджет исчерпан")

    def test_positive_remaining_passes(self) -> None:
        self._use_response('{"data": {"limit": 10.0, "usage": 4.0, "limit_remaining": 6.0}}')
        report = budget.check_budget("key")
        self.assertTrue(report.ok)
        self.assertFalse(report.unknown)
        self.assertAlmostEqual(report.limit_remaining, 6.0)

    def test_unauthorized_key_is_blocked(self) -> None:
        self._use_error(
            urllib.error.HTTPError("u", 401, "denied", email.message.Message(), None)
        )
        report = budget.check_budget("key")
        self.assertFalse(report.ok)
        self.assertEqual(report.status, 401)

    def test_network_failure_is_unknown_but_allowed(self) -> None:
        self._use_error(urllib.error.URLError("no route"))
        report = budget.check_budget("key")
        self.assertTrue(report.ok)
        self.assertTrue(report.unknown)

    def test_report_to_dict_shape(self) -> None:
        mapping = budget.BudgetReport(ok=True).to_dict()
        self.assertIn("limit_remaining", mapping)
        self.assertIn("unknown", mapping)


if __name__ == "__main__":
    unittest.main()
