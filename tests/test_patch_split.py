from __future__ import annotations

import unittest
from pathlib import Path

from db_pipeline.config import load_cores, require_core
from db_pipeline.patch_split import paths_in_diff, split_unified_diff


class PatchSplitTests(unittest.TestCase):
    def test_saved_diff_keeps_tests_out_of_gold(self) -> None:
        diff_path = Path(__file__).resolve().parent / "fixtures" / "pr_26419.diff"
        diff_text = diff_path.read_text(encoding="utf-8")
        core = require_core(load_cores(), "duckdb")
        test_patch, gold_patch = split_unified_diff(diff_text, core.code_prefixes, core.test_prefixes)
        test_paths = paths_in_diff(test_patch)
        gold_paths = paths_in_diff(gold_patch)
        self.assertIn("test/sql/subquery/exists/test_issue_26401.test", test_paths)
        self.assertIn("src/optimizer/statistics/operator/propagate_aggregate.cpp", gold_paths)
        for path in gold_paths:
            self.assertFalse(path.startswith("test/"))
        for path in test_paths:
            self.assertFalse(path.startswith("src/"))
            self.assertFalse(path.startswith("extension/core_functions/"))


if __name__ == "__main__":
    unittest.main()
