"""Аудит недоопределённых задач: эталон только для пометки, не для агента."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import cast

from db_pipeline.benchmark import eval as eval_module
from db_pipeline.benchmark import spec_audit


def _patch(hunk: str) -> str:
    return (
        "diff --git a/src/test/regress/expected/t.out "
        "b/src/test/regress/expected/t.out\n"
        "--- a/src/test/regress/expected/t.out\n"
        "+++ b/src/test/regress/expected/t.out\n"
        "@@ -1,2 +1,4 @@\n"
        + hunk
    )


class SpecAuditTests(unittest.TestCase):
    def test_existing_query_with_line_marker_is_underspecified(self) -> None:
        patch = _patch(
            " SELECT NULL::int IS JSON;\n"
            " ERROR:  cannot use type integer in IS JSON predicate\n"
            "+LINE 1: SELECT NULL::int IS JSON;\n"
            "+               ^\n"
        )
        reasons = spec_audit.reasons_for(patch, "Show the original type name.", [])
        self.assertIn("changed_existing_output", reasons)
        self.assertTrue(any(item.startswith("hidden_error_position") for item in reasons))

    def test_new_query_whose_error_is_in_the_goal_is_specified(self) -> None:
        patch = _patch(
            "+SELECT NULL::jd5 IS JSON;\n"
            "+ERROR:  cannot use type jd5 in IS JSON predicate\n"
        )
        goal = "ERROR:  cannot use type jd5 in IS JSON predicate"
        self.assertEqual(spec_audit.reasons_for(patch, goal, []), [])

    def test_error_absent_from_goal_is_hidden(self) -> None:
        patch = _patch("+SELECT 1;\n+ERROR:  cache lookup failed for type 1\n")
        reasons = spec_audit.reasons_for(patch, "Fix the cache.", [])
        self.assertTrue(any(item.startswith("hidden_error_text") for item in reasons))


class PrimaryMetricTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="spec-")
        self.data = Path(self.tmp.name)
        (self.data / "runs").mkdir(parents=True)
        (self.data / "reports").mkdir(parents=True)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_underspecified_task_leaves_the_primary_metric(self) -> None:
        for mode in ("bare", "graph"):
            for name, passed in (("postgres__a", True), ("postgres__b", False)):
                record = {
                    "func": name,
                    "mode": mode,
                    "pass": passed,
                    "started": "2026-01-01T00:00:00+00:00",
                    "generations": [{"reason": "stop", "output_tokens": 5}],
                    "tool_events": [{"tool": "edit"}],
                    "agent_jsonl": "",
                }
                path = self.data / "runs" / f"{name}__{mode}.json"
                path.write_text(json.dumps(record), encoding="utf-8")
        (self.data / "reports" / "spec-audit.json").write_text(
            json.dumps({"underspecified": ["postgres__b"], "tasks": []}),
            encoding="utf-8",
        )
        report = eval_module.build_report(self.data)
        self.assertEqual(report["pairs"], 1)
        self.assertEqual(report["underspecified_pairs"], 1)
        self.assertEqual(report["underspecified_tasks"], ["postgres__b"])
        itt = cast("dict[str, object]", report["itt"])
        self.assertEqual(itt["bare_runs"], 1)
        other = cast("dict[str, object]", report["itt_underspecified"])
        self.assertEqual(other["bare_runs"], 1)


if __name__ == "__main__":
    unittest.main()
