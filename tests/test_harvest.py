from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from db_pipeline.config import load_cores, require_core
from db_pipeline.filter_rules import filter_candidate
from db_pipeline.git_ops import select_harvest_rows
from db_pipeline.models import ChangeView, FileChange
from db_pipeline.sql_report import write_sql_labeled_report
from db_pipeline.stages import read_cursor, write_cursor


def _change(core_id: str, subject: str, files: list[FileChange]) -> ChangeView:
    return ChangeView(
        sha="sha",
        repository_id=core_id,
        subject=subject,
        body="",
        parent_count=1,
        files=files,
        pr_number=None,
        base_sha="base",
    )


class HarvestTests(unittest.TestCase):
    def test_sql_prefixes_are_fixed(self) -> None:
        cores = {core.id: core for core in load_cores()}
        self.assertEqual(cores["duckdb"].sql_code_prefixes, ["extension/core_functions/"])
        self.assertEqual(cores["duckdb"].sql_test_prefixes, ["test/sql/"])
        self.assertEqual(cores["postgres"].sql_code_prefixes, ["src/backend/utils/adt/"])
        self.assertEqual(cores["postgres"].sql_test_prefixes, ["src/test/regress/"])
        self.assertEqual(
            cores["sqlite"].sql_code_prefixes,
            ["src/func.c", "src/date.c", "src/printf.c"],
        )
        self.assertEqual(cores["sqlite"].sql_test_suffixes, [".test"])
        self.assertEqual(cores["duckdb"].code_prefixes, ["src/", "extension/core_functions/"])

    def test_postgres_without_expected_out_is_rejected(self) -> None:
        core = require_core(load_cores(), "postgres")
        change = _change(
            "postgres",
            "Fix int4 overflow",
            [
                FileChange("src/backend/utils/adt/int.c", 4, 1, "modified"),
                FileChange("src/test/regress/sql/int4.sql", 3, 0, "modified"),
            ],
        )
        decision = filter_candidate(change, core, set(), sql_only=True)
        self.assertFalse(decision.accept)
        self.assertEqual(decision.reason_code, "no_oracle_test")

    def test_postgres_sql_pair_is_accepted(self) -> None:
        core = require_core(load_cores(), "postgres")
        change = _change(
            "postgres",
            "Fix int4 overflow",
            [
                FileChange("src/backend/utils/adt/int.c", 4, 1, "modified"),
                FileChange("src/test/regress/expected/int4.out", 8, 0, "modified"),
            ],
        )
        decision = filter_candidate(change, core, set(), sql_only=True)
        self.assertTrue(decision.accept)

    def test_sqlite_exact_file_is_required(self) -> None:
        core = require_core(load_cores(), "sqlite")
        rejected = _change(
            "sqlite",
            "Fix parser",
            [
                FileChange("src/parse.c", 2, 0, "modified"),
                FileChange("test/parse.test", 2, 0, "modified"),
            ],
        )
        accepted = _change(
            "sqlite",
            "Fix date function",
            [
                FileChange("src/date.c", 2, 0, "modified"),
                FileChange("test/date.test", 2, 0, "modified"),
            ],
        )
        self.assertFalse(filter_candidate(rejected, core, set(), sql_only=True).accept)
        self.assertTrue(filter_candidate(accepted, core, set(), sql_only=True).accept)

    def test_window_starts_after_cursor_and_skips_known(self) -> None:
        rows = [
            ("ccc", "Fix newest (#3)"),
            ("bbb", "Fix middle (#2)"),
            ("aaa", "Fix oldest (#1)"),
            ("zzz", "Documentation only"),
        ]
        after = select_harvest_rows(rows, set(), "bbb", 5, True)
        self.assertEqual([sha for sha, _subject in after], ["aaa"])
        skipped = select_harvest_rows(rows, {"ccc"}, "", 1, True)
        self.assertEqual(skipped[0][0], "bbb")
        no_pr = select_harvest_rows(rows, {"ccc", "bbb", "aaa"}, "", 5, True)
        self.assertEqual(no_pr, [])

    def test_labeled_sql_twenties_match_frozen_marks(self) -> None:
        for core_id in ("duckdb", "postgres", "sqlite"):
            with tempfile.TemporaryDirectory() as tmp:
                matched, total = write_sql_labeled_report(core_id, data=Path(tmp))
            self.assertEqual(total, 20)
            self.assertGreaterEqual(matched, 16)

    def test_cursor_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(read_cursor(root, "sqlite"), "")
            write_cursor(root, "sqlite", "abc")
            self.assertEqual(read_cursor(root, "sqlite"), "abc")


if __name__ == "__main__":
    unittest.main()
