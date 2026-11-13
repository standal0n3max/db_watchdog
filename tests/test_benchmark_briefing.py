"""Сводка графа: бюджет токенов и выбор пилотных задач без базы и Docker."""

from __future__ import annotations

import unittest

from db_pipeline.benchmark.graph import briefing, briefing_audit


class BriefingSymbolTests(unittest.TestCase):
    def test_goal_identifiers_cover_camel_snake_and_oid(self) -> None:
        goal = (
            "Extend getBaseType() for JsonIsPredicate and expr_base_type. "
            "Store INT4OID. The word JSON stays out."
        )
        names = briefing.goal_identifiers(goal)
        self.assertIn("getBaseType", names)
        self.assertIn("JsonIsPredicate", names)
        self.assertIn("expr_base_type", names)
        self.assertIn("INT4OID", names)
        self.assertNotIn("JSON", names)

    def test_expected_and_sql_collapse_to_one_stem(self) -> None:
        graded = [
            "src/test/regress/expected/sqljson.out",
            "src/test/regress/sql/sqljson.sql",
            "src/test/regress/sql/other.sql",
        ]
        chosen = briefing._unique_tests(graded)  # pyright: ignore[reportPrivateUsage]
        self.assertEqual(
            chosen,
            [
                "src/test/regress/sql/sqljson.sql",
                "src/test/regress/sql/other.sql",
            ],
        )


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
