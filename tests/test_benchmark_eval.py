"""Парная оценка: McNemar, порядок вызовов, повторы и страта prior_twin.

Прогоны не запускаются: тесты работают на синтетических run records, ровно так,
как их пишет ``runner.run_one``.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import cast

from db_pipeline.benchmark import eval as eval_module
from db_pipeline.benchmark import completion


def _record(
    task: str,
    mode: str,
    passed: bool,
    started: str,
    events: list[str] | None = None,
) -> dict[str, object]:
    tool_events = [{"tool": name} for name in (events or [])]
    return {
        "func": task,
        "mode": mode,
        "pass": passed,
        "verdict": "PASS" if passed else "FAIL",
        "started": started,
        "tool_events": tool_events,
        "mcp_calls": sum(1 for name in (events or []) if name.startswith("graph_")),
        "agent": {"tokens_input": 10, "tokens_output": 5},
        "gates": {"tests_unchanged": {"ok": True}},
        # Прогон пригоден только при наличии генераций: без них классификатор
        # относит запись к API-отказу и исключает из пар.
        "generations": [{"reason": "tool-calls", "output_tokens": 10}],
    }


class McNemarTests(unittest.TestCase):
    def test_no_discordant_pairs_gives_one(self) -> None:
        self.assertEqual(eval_module.exact_binomial(0, 0), 1.0)

    def test_symmetric_discordance_has_no_effect(self) -> None:
        self.assertAlmostEqual(eval_module.exact_binomial(5, 5), 1.0, places=6)

    def test_large_asymmetry_is_significant(self) -> None:
        self.assertLess(eval_module.exact_binomial(9, 1), 0.05)

    def test_moderate_asymmetry_is_not_significant(self) -> None:
        self.assertGreater(eval_module.exact_binomial(6, 3), 0.05)

    def test_single_discordance_is_not_significant(self) -> None:
        self.assertEqual(eval_module.exact_binomial(1, 0), 1.0)

    def test_two_sided_is_capped_at_one(self) -> None:
        self.assertLessEqual(eval_module.exact_binomial(3, 3), 1.0)


class PairBuildingTests(unittest.TestCase):
    def test_pairs_are_built_by_task_and_repetition(self) -> None:
        records = [
            _record("postgres__a", "bare", True, "2026-01-01T00:00:00+00:00"),
            _record("postgres__a", "graph", True, "2026-01-01T00:10:00+00:00"),
            _record("postgres__a", "bare", False, "2026-01-02T00:00:00+00:00"),
            _record("postgres__a", "graph", True, "2026-01-02T00:10:00+00:00"),
        ]
        eval_module.assign_repetitions(records)
        pairs = eval_module.build_pairs(records)
        self.assertEqual(len(pairs), 2)
        # Повтор 0 — первая по ``started`` запись каждой руки.
        self.assertEqual(pairs[0].repetition, 0)
        self.assertTrue(pairs[0].bare_pass)
        self.assertTrue(pairs[0].graph_pass)
        # Повтор 1: bare упал, graph прошёл — это и есть расхождение для McNemar.
        self.assertEqual(pairs[1].repetition, 1)
        self.assertFalse(pairs[1].bare_pass)
        self.assertTrue(pairs[1].graph_pass)
        self.assertTrue(pairs[1].discordant)

    def test_assign_repetitions_is_stable_by_started(self) -> None:
        later = _record("postgres__a", "bare", False, "2026-02-01T00:00:00+00:00")
        earlier = _record("postgres__a", "bare", True, "2026-01-01T00:00:00+00:00")
        records = [later, earlier]
        eval_module.assign_repetitions(records)
        self.assertEqual(earlier["repetition"], 0)
        self.assertEqual(later["repetition"], 1)

    def test_mcnemar_counts_discordance(self) -> None:
        pairs = [
            eval_module.PairOutcome("t1", 0, bare_pass=True, graph_pass=False),
            eval_module.PairOutcome("t2", 0, bare_pass=False, graph_pass=True),
            eval_module.PairOutcome("t3", 0, bare_pass=True, graph_pass=True),
        ]
        result = eval_module.mcnemar(pairs)
        self.assertEqual(result.bare_only, 1)
        self.assertEqual(result.graph_only, 1)
        self.assertEqual(result.both_pass, 1)
        self.assertEqual(result.both_fail, 0)

    def test_task_level_uses_majority_vote(self) -> None:
        pairs = [
            eval_module.PairOutcome("t1", 0, bare_pass=True, graph_pass=False),
            eval_module.PairOutcome("t1", 1, bare_pass=True, graph_pass=True),
            eval_module.PairOutcome("t1", 2, bare_pass=False, graph_pass=True),
        ]
        reduced = eval_module.mcnemar(pairs, task_level=True)
        self.assertEqual(reduced.pairs, 1)
        self.assertEqual(reduced.both_pass, 1)

    def test_majority_false_when_not_over_half(self) -> None:
        pairs = [
            eval_module.PairOutcome("t1", 0, bare_pass=True, graph_pass=False),
            eval_module.PairOutcome("t1", 1, bare_pass=False, graph_pass=False),
        ]
        reduced = eval_module.mcnemar(pairs, task_level=True)
        self.assertEqual(reduced.both_fail, 1)


class ToolOrderTests(unittest.TestCase):
    def test_history_before_edit_is_true(self) -> None:
        record = _record(
            "t", "graph", True, "2026-01-01",
            ["graph_find_code_symbols", "edit"],
        )
        self.assertTrue(eval_module.history_used_before_edit(record))
        self.assertEqual(eval_module.turns_to_first_mcp(record), 1)

    def test_edit_before_history_is_false(self) -> None:
        record = _record(
            "t", "graph", True, "2026-01-01",
            ["edit", "graph_symbol_history"],
        )
        self.assertFalse(eval_module.history_used_before_edit(record))
        self.assertEqual(eval_module.turns_to_first_mcp(record), 2)

    def test_no_mcp_means_not_used(self) -> None:
        record = _record("t", "graph", True, "2026-01-01", ["read", "edit"])
        self.assertFalse(eval_module.history_used_before_edit(record))
        self.assertEqual(eval_module.turns_to_first_mcp(record), -1)

    def test_mcp_without_edit_counts_as_used(self) -> None:
        record = _record("t", "graph", True, "2026-01-01", ["graph_search_git_history"])
        self.assertTrue(eval_module.history_used_before_edit(record))

    def test_bare_record_never_uses_history(self) -> None:
        record = _record("t", "bare", True, "2026-01-01", ["edit", "read"])
        self.assertFalse(eval_module.history_used_before_edit(record))

    def test_first_edit_index_detects_variants(self) -> None:
        record = _record("t", "bare", True, "2026-01-01", ["read", "str_replace"])
        self.assertEqual(eval_module.first_edit_index(record), 1)

    def test_first_edit_index_without_edit(self) -> None:
        record = _record("t", "bare", True, "2026-01-01", ["read", "bash"])
        self.assertEqual(eval_module.first_edit_index(record), -1)


class AggregateTests(unittest.TestCase):
    def test_aggregate_rates_and_tokens(self) -> None:
        records = [
            _record("a", "bare", True, "2026-01-01"),
            _record("b", "bare", False, "2026-01-02"),
        ]
        summary = eval_module.aggregate(records)
        self.assertEqual(summary["runs"], 2)
        self.assertEqual(summary["pass"], 1)
        self.assertEqual(summary["pass_rate"], 0.5)
        self.assertEqual(summary["tokens_input"], 20)

    def test_median_of_even_list(self) -> None:
        self.assertEqual(eval_module.median([1, 2, 3, 4]), 2.5)

    def test_median_of_empty_list(self) -> None:
        self.assertEqual(eval_module.median([]), -1.0)

    def test_gate_failures_are_counted(self) -> None:
        record = _record("a", "bare", False, "2026-01-01")
        record["gates"] = {"tests_unchanged": {"ok": False}, "no_new_files": {"ok": True}}
        summary = eval_module.aggregate([record])
        self.assertEqual(summary["gate_failures"], {"tests_unchanged": 1})


class ReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="eval-")
        self.data = Path(self.tmp.name)
        (self.data / "runs").mkdir(parents=True)
        (self.data / "reports").mkdir(parents=True)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write(self, record: dict[str, object]) -> None:
        name = f"{record['func']}__{record['mode']}__{record['started']}.json"
        (self.data / "runs" / name).write_text(
            json.dumps(record, ensure_ascii=False), encoding="utf-8"
        )

    def test_report_counts_pairs_and_repetitions(self) -> None:
        for day in ("01", "02"):
            for mode in ("bare", "graph"):
                self._write(_record("postgres__a", mode, True, f"2026-01-{day}T00:00:00+00:00"))
        report = eval_module.build_report(self.data)
        self.assertEqual(report["pairs"], 2)
        self.assertEqual(report["repetitions"], 2)
        self.assertTrue(report["repetitions_ok"])
        self.assertEqual(report["incomplete_pairs"], 0)

    def test_incomplete_pair_is_reported(self) -> None:
        self._write(_record("postgres__a", "bare", True, "2026-01-01T00:00:00+00:00"))
        report = eval_module.build_report(self.data)
        self.assertEqual(report["pairs"], 0)
        self.assertEqual(report["incomplete_pairs"], 1)

    def test_prior_twin_stratum_is_separated(self) -> None:
        for mode in ("bare", "graph"):
            self._write(_record("postgres__a", mode, True, "2026-01-01T00:00:00+00:00"))
            self._write(_record("postgres__b", mode, True, "2026-01-02T00:00:00+00:00"))
        (self.data / "reports" / "leak-audit-holdout.json").write_text(
            json.dumps({"prior_twin_tasks": ["postgres__a"]}), encoding="utf-8"
        )
        report = eval_module.build_report(self.data)
        self.assertEqual(report["prior_twin_pairs"], 1)
        normal = report["mcnemar_normal_only"]
        self.assertIsInstance(normal, dict)
        self.assertEqual(cast("dict[str, object]", normal)["pairs"], 1)
        self.assertEqual(report["prior_twin_tasks"], ["postgres__a"])

    def test_prism_missing_stratum_is_separated_from_covered(self) -> None:
        """База вне снимка PRISM не смешивается с покрытыми: у v2 там нет истории."""
        for mode in ("bare", "graph"):
            self._write(_record("postgres__a", mode, True, "2026-01-01T00:00:00+00:00"))
            self._write(_record("postgres__b", mode, True, "2026-01-02T00:00:00+00:00"))
        (self.data / "reports" / "leak-audit-holdout.json").write_text(
            json.dumps(
                {
                    "prior_twin_tasks": [],
                    "prism_missing_tasks": ["postgres__b"],
                }
            ),
            encoding="utf-8",
        )
        report = eval_module.build_report(self.data)
        self.assertEqual(report["prism_missing_tasks"], ["postgres__b"])
        self.assertEqual(report["prism_missing_pairs"], 1)
        covered = report["mcnemar_prism_covered"]
        self.assertIsInstance(covered, dict)
        self.assertEqual(cast("dict[str, object]", covered)["pairs"], 1)

    def test_write_report_creates_file(self) -> None:
        target = eval_module.write_report(self.data, "tune")
        self.assertTrue(target.is_file())
        self.assertEqual(target.name, "pairwise-tune.json")

    def test_infra_failures_are_excluded_from_pairs(self) -> None:
        """Отказ API не идёт в пары: иначе McNemar измеряет надёжность шлюза."""
        good = _record("postgres__a", "bare", True, "2026-01-01T00:00:00+00:00")
        failed = _record("postgres__a", "graph", False, "2026-01-01T00:10:00+00:00")
        failed["generations"] = []
        failed["agent_jsonl"] = '{"error":"in_flight_budget_exhausted"}'
        self._write(good)
        self._write(failed)
        report = eval_module.build_report(self.data)
        self.assertEqual(report["pairs"], 0)
        self.assertEqual(report["runs_excluded"], 1)
        completion_counts = report["completion"]
        self.assertIsInstance(completion_counts, dict)
        counted = cast("dict[str, int]", completion_counts)
        self.assertEqual(counted["infra_fail_truncated"], 1)
        self.assertEqual(counted["crash_unknown"], 0)

    def test_completion_counts_present_even_when_empty(self) -> None:
        self._write(_record("postgres__a", "bare", True, "2026-01-01T00:00:00+00:00"))
        report = eval_module.build_report(self.data)
        counted = cast("dict[str, int]", report["completion"])
        self.assertIn("ok", counted)
        self.assertIn("infra_fail_wall", counted)

    def test_load_records_skips_broken_json(self) -> None:
        (self.data / "runs" / "broken.json").write_text("{not json", encoding="utf-8")
        self._write(_record("postgres__a", "bare", True, "2026-01-01T00:00:00+00:00"))
        self.assertEqual(len(eval_module.load_records(self.data)), 1)


class StepLimitTests(unittest.TestCase):
    def test_record_step_limit_reads_manifest(self) -> None:
        record = _record("a", "bare", True, "2026-01-01")
        record["manifest"] = {"step_limit": 80}
        self.assertEqual(eval_module.record_step_limit(record), 80)

    def test_record_step_limit_defaults_to_zero(self) -> None:
        self.assertEqual(eval_module.record_step_limit(_record("a", "bare", True, "2026-01-01")), 0)

    def test_run_at_step_limit_is_excluded_from_pairs(self) -> None:
        """Прогон, дошедший до ``agent.build.steps``, не идёт в пары."""
        record = _record("postgres__a", "bare", False, "2026-01-01T00:00:00+00:00")
        record["manifest"] = {"step_limit": 2}
        record["generations"] = [
            {"reason": "tool-calls", "output_tokens": 10},
            {"reason": "tool-calls", "output_tokens": 10},
        ]
        step_limit = eval_module.record_step_limit(record)
        self.assertFalse(completion.countable(record, step_limit))


class AblationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="ablation-")
        self.data = Path(self.tmp.name)
        (self.data / "runs").mkdir(parents=True)
        (self.data / "reports").mkdir(parents=True)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write(self, record: dict[str, object]) -> None:
        name = f"{record['func']}__{record['mode']}__{record['started']}.json"
        (self.data / "runs" / name).write_text(
            json.dumps(record, ensure_ascii=False), encoding="utf-8"
        )

    def test_ablation_compares_graph_against_graph_v2(self) -> None:
        for day in ("01", "02"):
            self._write(_record("postgres__a", "graph", True, f"2026-01-{day}T00:00:00+00:00"))
            self._write(_record("postgres__a", "graph_v2", True, f"2026-01-{day}T00:10:00+00:00"))
        report = eval_module.build_report(self.data)
        ablation = cast("dict[str, object]", report["ablation"])
        self.assertEqual(ablation["left"], "graph")
        self.assertEqual(ablation["right"], "graph_v2")
        self.assertEqual(ablation["pairs"], 2)

    def test_bare_vs_graph_pairs_unaffected_by_graph_v2(self) -> None:
        """Абляционная рука не попадает в основное сравнение ``bare`` vs ``graph``."""
        for mode in ("bare", "graph"):
            self._write(_record("postgres__a", mode, True, "2026-01-01T00:00:00+00:00"))
        self._write(_record("postgres__a", "graph_v2", False, "2026-01-01T00:10:00+00:00"))
        report = eval_module.build_report(self.data)
        self.assertEqual(report["pairs"], 1)
        graph_v2 = cast("dict[str, object]", report["graph_v2"])
        self.assertEqual(graph_v2["runs"], 1)

    def test_ablation_hand_mismatch_is_incomplete(self) -> None:
        self._write(_record("postgres__a", "graph", True, "2026-01-01T00:00:00+00:00"))
        report = eval_module.build_report(self.data)
        ablation = cast("dict[str, object]", report["ablation"])
        self.assertEqual(ablation["pairs"], 0)
        self.assertEqual(ablation["incomplete_pairs"], 1)

    def test_empty_diff_and_seconds_are_counted(self) -> None:
        record = _record("a", "bare", False, "2026-01-01")
        record["diff_empty"] = True
        record["seconds"] = 120
        summary = eval_module.aggregate([record])
        self.assertEqual(summary["empty_diff"], 1)
        self.assertEqual(summary["seconds_median"], 120.0)


if __name__ == "__main__":
    unittest.main()
