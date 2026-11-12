"""Классификатор завершения: разведение truncated и wall и счёт категорий.

Тесты работают на синтетических run records: Docker, сеть и модель не нужны.
Каждая категория проверяется на минимальном наборе полей, которые реально
пишет ``runner._run_in_container``.
"""

from __future__ import annotations

import unittest

from db_pipeline.benchmark import completion


def _generations(*reasons: str, output_tokens: int = 5) -> list[dict[str, object]]:
    return [
        {"reason": reason, "output_tokens": output_tokens} for reason in reasons
    ]


def _record(
    generations: list[dict[str, object]] | None = None,
    jsonl: str = "",
    events: list[str] | None = None,
) -> dict[str, object]:
    return {
        "func": "postgres__test",
        "mode": "bare",
        "generations": generations if generations is not None else [],
        "agent_jsonl": jsonl,
        "tool_events": [{"tool": name} for name in (events or [])],
        "pass": False,
    }


class ClassifyTests(unittest.TestCase):
    def test_zero_generations_with_budget_marker_is_truncated(self) -> None:
        record = _record(jsonl="This request requires more credits, or fewer max_tokens")
        self.assertEqual(completion.outcome(record), "infra_fail_truncated")

    def test_zero_generations_without_marker_is_crash_unknown(self) -> None:
        self.assertEqual(completion.outcome(_record()), "crash_unknown")

    def test_generations_with_inflight_marker_is_wall(self) -> None:
        record = _record(
            generations=_generations("tool-calls"),
            jsonl='{"reason":"in_flight_budget_exhausted"}',
        )
        self.assertEqual(completion.outcome(record), "infra_fail_wall")

    def test_length_with_zero_output_is_length(self) -> None:
        record = _record(generations=_generations("length", output_tokens=0))
        self.assertEqual(completion.outcome(record), "length")

    def test_length_with_output_is_partial(self) -> None:
        record = _record(generations=_generations("length"))
        self.assertEqual(completion.outcome(record), "length_partial")
        self.assertFalse(completion.countable(record))

    def test_timeout_flag_is_timeout(self) -> None:
        record = _record(generations=_generations("stop"))
        record["agent_timeout"] = True
        self.assertEqual(completion.outcome(record), "timeout")
        self.assertFalse(completion.countable(record))

    def test_step_limit_detected_at_boundary(self) -> None:
        generations = _generations("tool-calls", "tool-calls", "tool-calls")
        self.assertEqual(completion.outcome(_record(generations), step_limit=3), "step_limit")

    def test_step_limit_not_triggered_below_boundary(self) -> None:
        generations = _generations("tool-calls", "tool-calls", "tool-calls")
        self.assertEqual(completion.outcome(_record(generations), step_limit=80), "ok")

    def test_self_stop_without_edit(self) -> None:
        record = _record(generations=_generations("stop"))
        self.assertEqual(completion.outcome(record), "self_stop_empty")

    def test_self_stop_with_edit_is_ok(self) -> None:
        record = _record(
            generations=_generations("stop"),
            events=["read", "edit"],
        )
        self.assertEqual(completion.outcome(record), "ok")

    def test_edit_tool_variant_counts(self) -> None:
        record = _record(
            generations=_generations("stop"),
            events=["str_replace"],
        )
        self.assertEqual(completion.outcome(record), "ok")

    def test_prefixed_edit_tool_counts(self) -> None:
        record = _record(
            generations=_generations("stop"),
            events=["graph_edit"],
        )
        self.assertEqual(completion.outcome(record), "ok")


class CountableTests(unittest.TestCase):
    def test_ok_is_countable(self) -> None:
        record = _record(generations=_generations("tool-calls"))
        self.assertTrue(completion.countable(record))

    def test_self_stop_empty_is_countable(self) -> None:
        record = _record(generations=_generations("stop"))
        self.assertTrue(completion.countable(record))

    def test_infra_and_limits_are_not_countable(self) -> None:
        wall = _record(
            generations=_generations("tool-calls"),
            jsonl="in_flight_budget_exhausted",
        )
        truncated = _record(jsonl="requires more credits")
        length = _record(generations=_generations("length", output_tokens=0))
        for record in (wall, truncated, length):
            self.assertFalse(completion.countable(record))


class SummarizeTests(unittest.TestCase):
    def test_summary_contains_all_categories(self) -> None:
        records = [
            _record(generations=_generations("tool-calls")),
            _record(jsonl="requires more credits"),
        ]
        summary = completion.summarize(records)
        for name in completion.OUTCOMES:
            self.assertIn(name, summary)
        self.assertEqual(summary["ok"], 1)
        self.assertEqual(summary["infra_fail_truncated"], 1)
        self.assertEqual(summary["crash_unknown"], 0)


if __name__ == "__main__":
    unittest.main()
