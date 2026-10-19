"""Правила SQLite: префиксы ядра, амальгама и разрез патча.

Двадцатка ``labeled_sqlite.json`` сверяется через ``write_sqlite_filter_report``. Отчёт уходит в переданный временный каталог.
"""

from __future__ import annotations

import unittest

import tempfile
from pathlib import Path

from db_pipeline.compile_task import compile_task
from db_pipeline.config import load_cores, require_core
from db_pipeline.filter_rules import filter_candidate
from db_pipeline.models import ChangeView, FileChange
from db_pipeline.patch_split import paths_in_diff
from db_pipeline.sqlite_report import write_sqlite_filter_report


def _change(sha: str, subject: str, files: list[FileChange]) -> ChangeView:
    return ChangeView(
        sha=sha,
        repository_id="sqlite",
        subject=subject,
        body="",
        parent_count=1,
        files=files,
        pr_number=None,
        base_sha="base",
    )


class SqliteRuleTests(unittest.TestCase):
    def test_labeled_sample_matches_frozen_marks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            matched, total = write_sqlite_filter_report(data=Path(tmp))
        self.assertEqual(total, 20)
        self.assertGreaterEqual(matched, 16)

    def test_prefixes_are_fixed_before_the_sample(self) -> None:
        core = require_core(load_cores(), "sqlite")
        self.assertEqual(core.code_prefixes, ["src/"])
        self.assertEqual(core.test_prefixes, ["test/"])
        self.assertEqual(core.test_suffixes, [".test"])
        self.assertEqual(core.generated_names, ["sqlite3.c", "sqlite3.h"])
        self.assertTrue(core.enabled)

    def test_code_and_test_file_is_accepted(self) -> None:
        core = require_core(load_cores(), "sqlite")
        change = _change(
            "good",
            "Fix a crash in the parser",
            [
                FileChange("src/parse.c", 8, 1, "modified"),
                FileChange("test/parse.test", 12, 0, "modified"),
            ],
        )
        decision = filter_candidate(change, core, set())
        self.assertTrue(decision.accept)

    def test_amalgamation_is_not_core_code(self) -> None:
        core = require_core(load_cores(), "sqlite")
        change = _change(
            "amalgam",
            "Update the amalgamation",
            [
                FileChange("sqlite3.c", 400, 20, "modified"),
                FileChange("sqlite3.h", 10, 1, "modified"),
                FileChange("test/select1.test", 4, 0, "modified"),
            ],
        )
        decision = filter_candidate(change, core, set())
        self.assertFalse(decision.accept)
        self.assertEqual(decision.reason_code, "generated_files")

    def test_c_file_under_test_is_not_a_test(self) -> None:
        core = require_core(load_cores(), "sqlite")
        change = _change(
            "harness",
            "Adjust the test harness",
            [
                FileChange("src/vdbe.c", 3, 1, "modified"),
                FileChange("test/tester.c", 6, 0, "modified"),
            ],
        )
        decision = filter_candidate(change, core, set())
        self.assertFalse(decision.accept)
        self.assertEqual(decision.reason_code, "missing_code_and_tests")

    def test_split_drops_amalgamation_and_keeps_test_suffix(self) -> None:
        core = require_core(load_cores(), "sqlite")
        diff = "\n".join(
            [
                "diff --git a/src/parse.c b/src/parse.c",
                "--- a/src/parse.c",
                "+++ b/src/parse.c",
                "@@ -1 +1 @@",
                "-old",
                "+new",
                "diff --git a/sqlite3.c b/sqlite3.c",
                "--- a/sqlite3.c",
                "+++ b/sqlite3.c",
                "@@ -1 +1 @@",
                "-old",
                "+new",
                "diff --git a/test/parse.test b/test/parse.test",
                "--- a/test/parse.test",
                "+++ b/test/parse.test",
                "@@ -1 +1 @@",
                "-old",
                "+new",
                "diff --git a/test/tester.c b/test/tester.c",
                "--- a/test/tester.c",
                "+++ b/test/tester.c",
                "@@ -1 +1 @@",
                "-old",
                "+new",
                "",
            ]
        )
        change = _change("split", "Fix parser", [FileChange("src/parse.c", 1, 1, "modified")])
        task = compile_task(change, core, diff, "Fix parser", "commit_message")
        self.assertEqual(paths_in_diff(task.gold_patch), ["src/parse.c"])
        self.assertEqual(paths_in_diff(task.test_patch), ["test/parse.test"])
        self.assertEqual(task.fail_to_pass, ["test/parse.test"])
        self.assertNotIn("sqlite3.c", task.gold_patch)


if __name__ == "__main__":
    unittest.main()
