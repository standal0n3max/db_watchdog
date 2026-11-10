"""Gate-ы anti-leak, парсинг pg_regress, cutoff и OTLP payload.

Проверяются: непустой source diff, запрет правок тестов, пересозданные тесты,
разбор вывода pg_regress, предковство cutoff и структура OTLP-запроса к
Langfuse. Docker и сеть не нужны.
"""

from __future__ import annotations

import unittest
from typing import cast

from db_pipeline.benchmark import postgres, security, telemetry
from db_pipeline.benchmark.graph import cutoff as cutoff_module


class GateTests(unittest.TestCase):
    def test_source_diff_is_required(self) -> None:
        gates = security.gates([], [])
        self.assertFalse(gates["nonempty_diff"].ok)

    def test_test_edit_is_blocked(self) -> None:
        gates = security.gates(["src/test/regress/sql/x.sql"], [])
        self.assertFalse(gates["no_test_edits"].ok)

    def test_recreated_test_is_blocked(self) -> None:
        gates = security.gates(["src/backend/x.c"], ["src/test/regress/sql/x.sql"])
        self.assertFalse(gates["no_recreated_tests"].ok)

    def test_clean_source_passes(self) -> None:
        gates = security.gates(["src/backend/x.c"], [])
        self.assertTrue(security.verdict(gates, target_ok=True, noregress_ok=True))

    def test_verdict_needs_regression(self) -> None:
        gates = security.gates(["src/backend/x.c"], [])
        self.assertFalse(security.verdict(gates, target_ok=True, noregress_ok=False))


class RegressParseTests(unittest.TestCase):
    OUTPUT = (
        "test eager_aggregate ... ok\n"
        "test int4 ... FAILED\n"
        "1 of 2 tests failed\n"
    )

    def test_failed_names(self) -> None:
        self.assertEqual(postgres.parse_failed(self.OUTPUT), ["int4"])

    def test_total(self) -> None:
        self.assertEqual(postgres.parse_total(self.OUTPUT), 2)

    def test_statuses(self) -> None:
        statuses = postgres.parse_statuses(self.OUTPUT)
        self.assertEqual(statuses["eager_aggregate"], "passed")
        self.assertEqual(statuses["int4"], "failed")


class RegressSetupTests(unittest.TestCase):
    def test_setup_is_prepended(self) -> None:
        self.assertEqual(postgres._with_setup(["eager_aggregate"]), ["test_setup", "eager_aggregate"])

    def test_setup_is_not_duplicated(self) -> None:
        self.assertEqual(postgres._with_setup(["test_setup", "int4"]), ["test_setup", "int4"])


class CutoffTests(unittest.TestCase):
    def test_ancestor_is_allowed(self) -> None:
        cutoff = cutoff_module.build_cutoff("base", 1000, {"base", "older"})
        self.assertTrue(cutoff.allows("older", 900))
        self.assertTrue(cutoff.allows("base", 1000))

    def test_newer_commit_is_refused(self) -> None:
        cutoff = cutoff_module.build_cutoff("base", 1000, {"base", "older"})
        self.assertFalse(cutoff.allows("head", 1000))

    def test_time_after_cutoff_is_refused(self) -> None:
        cutoff = cutoff_module.build_cutoff("base", 1000, {"base", "future"})
        self.assertFalse(cutoff.allows("future", 2000))

    def test_head_never_reachable(self) -> None:
        cutoff = cutoff_module.build_cutoff("base", 1000, {"base", "head"}, forbidden_head="head")
        self.assertFalse(cutoff.allows("head", 500))


class TelemetryTests(unittest.TestCase):
    def test_otlp_shape(self) -> None:
        root = telemetry.OtelSpan(
            name="root", start_ns=1, end_ns=2, attributes={"langfuse.trace.name": "t"}
        )
        child = telemetry.OtelSpan(
            name="child",
            start_ns=1,
            end_ns=2,
            parent_id=root.span_id,
            attributes={"langfuse.observation.type": "generation"},
        )
        payload = telemetry.build_otlp("a" * 32, root, [child])
        resource_spans = cast("list[object]", payload["resourceSpans"])
        resource = cast("dict[str, object]", resource_spans[0])
        scope_spans = cast("list[object]", resource["scopeSpans"])
        scope = cast("dict[str, object]", scope_spans[0])
        spans = cast("list[dict[str, object]]", scope["spans"])
        self.assertEqual(len(spans), 2)
        self.assertEqual(spans[0]["traceId"], "a" * 32)
        self.assertEqual(spans[1]["parentSpanId"], root.span_id)

    def test_disabled_without_keys(self) -> None:
        config = telemetry.OtelConfig(url="http://x", public_key="", secret_key="")
        result = telemetry.publish(config, "t", {})
        self.assertFalse(result["sent"])


if __name__ == "__main__":
    unittest.main()
