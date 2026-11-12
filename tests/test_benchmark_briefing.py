"""Сводка графа: бюджет токенов и выбор пилотных задач без базы и Docker."""

from __future__ import annotations

import unittest

from db_pipeline.benchmark.graph import briefing, briefing_audit


class BriefingBudgetTests(unittest.TestCase):
    def test_long_text_is_cut_to_budget(self) -> None:
        text = "\n".join(f"line {index} " + ("x" * 80) for index in range(200))
        fitted = briefing._fit(text)  # pyright: ignore[reportPrivateUsage]
        self.assertLessEqual(briefing.estimate_tokens(fitted), briefing.TOKEN_BUDGET)
        self.assertTrue(fitted.startswith("line 0"))

    def test_pilot_seed_is_stable(self) -> None:
        names = [f"postgres__{index:012d}" for index in range(20)]
        first = briefing_audit.choose_pilot(names)
        second = briefing_audit.choose_pilot(names)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 5)
        self.assertEqual(len(set(first)), 5)
        self.assertEqual(briefing_audit.pilot_seed(), briefing_audit.pilot_seed())


if __name__ == "__main__":
    unittest.main()
