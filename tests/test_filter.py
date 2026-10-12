from __future__ import annotations

import unittest

from db_pipeline.config import load_cores, require_core
from db_pipeline.filter_report import write_filter_report
from db_pipeline.filter_rules import filter_candidate
from db_pipeline.models import ChangeView, FileChange


class FilterTests(unittest.TestCase):
    def test_labeled_sample_matches_manual_marks(self) -> None:
        matched, total = write_filter_report()
        self.assertEqual(total, 20)
        self.assertGreaterEqual(matched, 16)

    def test_merge_commit_is_rejected(self) -> None:
        core = require_core(load_cores(), "duckdb")
        change = ChangeView(
            sha="merge",
            repository_id="duckdb",
            subject="Merge pull request #1 from someone/branch",
            body="",
            parent_count=2,
            files=[
                FileChange("src/main/config.cpp", 3, 1, "modified"),
                FileChange("test/sql/sample.test", 10, 0, "added"),
            ],
            pr_number=None,
            base_sha="",
        )
        decision = filter_candidate(change, core, set())
        self.assertFalse(decision.accept)
        self.assertEqual(decision.reason_code, "merge_commit")

    def test_pull_request_merge_with_normal_subject_is_not_junk(self) -> None:
        core = require_core(load_cores(), "duckdb")
        change = ChangeView(
            sha="pr-merge",
            repository_id="duckdb",
            subject="Fix something (#1)",
            body="",
            parent_count=2,
            files=[
                FileChange("src/main/config.cpp", 3, 1, "modified"),
                FileChange("test/sql/sample.test", 10, 0, "added"),
            ],
            pr_number=1,
            base_sha="base",
        )
        decision = filter_candidate(change, core, set())
        self.assertTrue(decision.accept)

    def test_known_sha_is_rejected(self) -> None:
        core = require_core(load_cores(), "duckdb")
        change = ChangeView(
            sha="already",
            repository_id="duckdb",
            subject="Fix something",
            body="",
            parent_count=1,
            files=[
                FileChange("src/main/config.cpp", 3, 1, "modified"),
                FileChange("test/sql/sample.test", 10, 0, "added"),
            ],
            pr_number=1,
            base_sha="",
        )
        decision = filter_candidate(change, core, {"already"})
        self.assertFalse(decision.accept)
        self.assertEqual(decision.reason_code, "already_in_journal")


if __name__ == "__main__":
    unittest.main()
