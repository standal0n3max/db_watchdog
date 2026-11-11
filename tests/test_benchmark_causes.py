"""Пер-прогонный агрегатор причин: категория завершения, rule-based ``cause``.

Прогоны не запускаются: тесты работают на синтетических run record и oracle,
ровно так, как их пишет ``runner.run_one``.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import cast

from db_pipeline.benchmark import causes


def _record(
    mode: str,
    passed: bool,
    *,
    mcp_calls: int = 0,
    diff_size: int = 5,
    events: list[str] | None = None,
    gates: dict[str, bool] | None = None,
    target_ok: bool = True,
    noregress_ok: bool = True,
    outcome: str = "ok",
) -> dict[str, object]:
    generations = [{"reason": "tool-calls", "output_tokens": 10}]
    return {
        "func": "postgres__a",
        "mode": mode,
        "pass": passed,
        "mcp_calls": mcp_calls,
        "diff_size": diff_size,
        "target_ok": target_ok,
        "noregress_ok": noregress_ok,
        "tool_events": [{"tool": name} for name in (events or ["read", "edit"])],
        "gates": {name: {"ok": ok} for name, ok in (gates or {}).items()},
        "generations": generations,
        "check": {"failed": []},
        "manifest": {"step_limit": 80},
    }


class CauseTests(unittest.TestCase):
    def test_pass_is_not_a_failure_cause(self) -> None:
        result = causes.cause_of(_record("bare", True))
        self.assertEqual(result.cause, "pass")
        self.assertEqual(result.outcome, "ok")

    def test_infra_outcome_carries_its_own_cause(self) -> None:
        record = _record("bare", False)
        record["generations"] = []
        record["agent_jsonl"] = '{"error":"in_flight_budget_exhausted"}'
        result = causes.cause_of(record)
        self.assertEqual(result.cause, "infra_fail_truncated")

    def test_graph_not_called_is_distinct_from_not_localized(self) -> None:
        record = _record("graph", False, mcp_calls=0, events=["read", "edit"])
        self.assertEqual(causes.cause_of(record).cause, "graph_not_called")

    def test_graph_called_but_no_edit_is_graph_useless(self) -> None:
        record = _record(
            "graph",
            False,
            mcp_calls=3,
            events=["graph_symbol_history", "read"],
        )
        self.assertEqual(causes.cause_of(record).cause, "graph_useless")

    def test_empty_diff_is_reported(self) -> None:
        record = _record("bare", False, diff_size=0, events=["read", "edit"])
        self.assertEqual(causes.cause_of(record).cause, "empty_diff")

    def test_edited_tests_take_priority(self) -> None:
        record = _record(
            "bare",
            False,
            gates={"no_test_edits": False, "no_recreated_tests": True},
        )
        self.assertEqual(causes.cause_of(record).cause, "edited_tests")

    def test_broken_regress_needs_target_ok(self) -> None:
        record = _record(
            "bare", False, target_ok=True, noregress_ok=False, events=["edit"]
        )
        self.assertEqual(causes.cause_of(record).cause, "broke_regress")

    def test_integration_failure_outranks_graph_logic(self) -> None:
        record = _record("graph", False, mcp_calls=0)
        record["integration_failure"] = True
        record["error"] = "Traceback"
        self.assertEqual(causes.cause_of(record).cause, "integration_failure")

    def test_wrong_file_when_diff_misses_gold_source(self) -> None:
        record = _record("bare", False, events=["edit"])
        record["diff"] = (
            "diff --git a/src/backend/utils/adt/other.c "
            "b/src/backend/utils/adt/other.c\n@@ -1 +1 @@\n"
        )
        oracle: dict[str, object] = {
            "reverse_patch": (
                "diff --git a/src/backend/utils/adt/ri_triggers.c "
                "b/src/backend/utils/adt/ri_triggers.c\n@@ -1 +1 @@\n"
            )
        }
        self.assertEqual(causes.cause_of(record, oracle).cause, "wrong_file")

    def test_not_localized_when_diff_hits_gold_source(self) -> None:
        record = _record("bare", False, events=["edit"])
        record["diff"] = (
            "diff --git a/src/backend/utils/adt/ri_triggers.c "
            "b/src/backend/utils/adt/ri_triggers.c\n@@ -1 +1 @@\n"
        )
        oracle: dict[str, object] = {
            "reverse_patch": (
                "diff --git a/src/backend/utils/adt/ri_triggers.c "
                "b/src/backend/utils/adt/ri_triggers.c\n@@ -1 +1 @@\n"
            )
        }
        self.assertEqual(causes.cause_of(record, oracle).cause, "not_localized")


class AggregateTests(unittest.TestCase):
    def test_summary_counts_mode_outcome_cause(self) -> None:
        runs = causes.classify_records(
            [
                _record("bare", True, events=["read", "edit"]),
                _record("graph", False, mcp_calls=0, events=["read", "edit"]),
            ]
        )
        summary = causes.aggregate_causes(runs)
        self.assertEqual(summary["runs"], 2)
        by_cause = cast("dict[str, dict[str, int]]", summary["by_cause"])
        self.assertEqual(by_cause["bare"]["pass"], 1)
        self.assertEqual(by_cause["graph"]["graph_not_called"], 1)

    def test_build_report_filters_by_mode(self) -> None:
        records = [
            _record("bare", True, events=["read", "edit"]),
            _record("graph", False, mcp_calls=0, events=["read", "edit"]),
        ]
        report = causes.build_cause_report(records=records, modes=("graph",))
        summary = cast("dict[str, object]", report["summary"])
        by_cause = cast("dict[str, dict[str, int]]", summary["by_cause"])
        self.assertNotIn("bare", by_cause)


class WriteReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="causes-")
        self.data = Path(self.tmp.name)
        (self.data / "runs").mkdir(parents=True)
        (self.data / "reports").mkdir(parents=True)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_write_cause_report_creates_file(self) -> None:
        target = causes.write_cause_report(self.data, label="causes-tune")
        self.assertTrue(target.is_file())
        self.assertEqual(target.name, "causes-tune.json")


if __name__ == "__main__":
    unittest.main()
