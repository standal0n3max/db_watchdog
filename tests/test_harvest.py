"""Обход истории: окно, курсор и повторный разбор пропуска «нет правки SQL-кода».

SQL-двадцатки трёх ядер (``labeled_sql_duckdb.json``, ``labeled_sql_postgres.json``, ``labeled_sql_sqlite.json``) сверяются с ``sql_only``. Отчёты пишутся во временный каталог.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from db_pipeline.config import load_cores, require_core
from db_pipeline.filter_rules import MISSING_SQL_CODE_REASON, filter_candidate
from db_pipeline.git_ops import select_harvest_rows
from db_pipeline.journal import create_journal, harvest_known_shas, read_journal, write_json
from db_pipeline.models import ChangeView, FileChange, change_to_dict
from db_pipeline.sql_report import write_sql_labeled_report
from db_pipeline.stages import StageResult, filter_stage, read_cursor, write_cursor


def _ready(sha: str) -> StageResult:
    return {
        "status": "ready",
        "repository_id": "sqlite",
        "sha": sha,
        "base_sha": "base",
        "pr_number": "",
        "instance_id": "",
        "reason_code": "",
        "reason": "",
        "statement_source": "",
        "task_path": "",
        "oracle_status": "",
        "run_path": "",
        "detail": "harvest",
    }


def _change(core_id: str, subject: str, files: list[FileChange], sha: str = "sha") -> ChangeView:
    return ChangeView(
        sha=sha,
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
        self.assertEqual(cores["duckdb"].sql_code_prefixes, ["src/", "extension/core_functions/"])
        self.assertEqual(cores["duckdb"].sql_test_prefixes, ["test/sql/"])
        self.assertEqual(cores["postgres"].sql_code_prefixes, ["src/backend/"])
        self.assertEqual(cores["postgres"].sql_test_prefixes, ["src/test/regress/"])
        self.assertEqual(cores["sqlite"].sql_code_prefixes, ["src/"])
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

    def test_sqlite_core_with_test_is_accepted(self) -> None:
        core = require_core(load_cores(), "sqlite")
        select_change = _change(
            "sqlite",
            "Fix union",
            [
                FileChange("src/select.c", 2, 0, "modified"),
                FileChange("test/unionall2.test", 2, 0, "modified"),
            ],
        )
        func_change = _change(
            "sqlite",
            "Fix date function",
            [
                FileChange("src/func.c", 2, 0, "modified"),
                FileChange("test/date.test", 2, 0, "modified"),
            ],
        )
        extension = _change(
            "sqlite",
            "Fix fts",
            [
                FileChange("ext/fts5/fts5.c", 2, 0, "modified"),
                FileChange("test/fts5.test", 2, 0, "modified"),
            ],
        )
        no_test = _change(
            "sqlite",
            "Fix parser",
            [FileChange("src/parse.c", 2, 0, "modified")],
        )
        amalgam = _change(
            "sqlite",
            "Regenerate amalgamation",
            [
                FileChange("src/sqlite3.c", 20, 0, "modified"),
                FileChange("test/select1.test", 2, 0, "modified"),
            ],
        )
        self.assertTrue(filter_candidate(select_change, core, set(), sql_only=True).accept)
        self.assertTrue(filter_candidate(func_change, core, set(), sql_only=True).accept)
        self.assertFalse(filter_candidate(extension, core, set(), sql_only=True).accept)
        self.assertFalse(filter_candidate(no_test, core, set(), sql_only=True).accept)
        self.assertFalse(filter_candidate(amalgam, core, set(), sql_only=True).accept)

    def test_duckdb_src_passes_and_optimizer_does_not(self) -> None:
        core = require_core(load_cores(), "duckdb")
        accepted = _change(
            "duckdb",
            "Fix planner (#1)",
            [
                FileChange("src/optimizer/rule/list_comprehension_rewrite.cpp", 4, 1, "modified"),
                FileChange("test/sql/optimizer/list_comprehension_casts.test", 8, 0, "modified"),
            ],
        )
        optimizer_only = _change(
            "duckdb",
            "Fix optimizer test (#2)",
            [FileChange("test/optimizer/aggregate_reuse.test", 3, 0, "modified")],
        )
        src_and_optimizer = _change(
            "duckdb",
            "Fix optimizer (#3)",
            [
                FileChange("src/optimizer/optimizer.cpp", 2, 0, "modified"),
                FileChange("test/optimizer/aggregate_reuse.test", 3, 0, "modified"),
            ],
        )
        self.assertTrue(filter_candidate(accepted, core, set(), sql_only=True).accept)
        self.assertFalse(filter_candidate(optimizer_only, core, set(), sql_only=True).accept)
        self.assertFalse(filter_candidate(src_and_optimizer, core, set(), sql_only=True).accept)

    def test_postgres_backend_outside_adt_passes_and_contrib_does_not(self) -> None:
        core = require_core(load_cores(), "postgres")
        accepted = _change(
            "postgres",
            "Fix heap scan",
            [
                FileChange("src/backend/access/heap/heapam.c", 4, 1, "modified"),
                FileChange("src/test/regress/expected/tidrangescan.out", 8, 0, "modified"),
            ],
        )
        no_expected = _change(
            "postgres",
            "Fix heap scan",
            [
                FileChange("src/backend/access/heap/heapam.c", 4, 1, "modified"),
                FileChange("src/test/regress/sql/tidrangescan.sql", 3, 0, "modified"),
            ],
        )
        contrib = _change(
            "postgres",
            "Fix visibility",
            [
                FileChange("contrib/pg_visibility/pg_visibility.c", 4, 1, "modified"),
                FileChange("contrib/pg_visibility/expected/pg_visibility.out", 8, 0, "modified"),
            ],
        )
        accepted_decision = filter_candidate(accepted, core, set(), sql_only=True)
        no_expected_decision = filter_candidate(no_expected, core, set(), sql_only=True)
        self.assertTrue(accepted_decision.accept)
        self.assertFalse(no_expected_decision.accept)
        self.assertEqual(no_expected_decision.reason_code, "no_oracle_test")
        self.assertFalse(filter_candidate(contrib, core, set(), sql_only=True).accept)

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

    def test_missing_sql_code_without_oracle_is_not_known(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            skipped = "skip-sql"
            tested = "no-test"
            accepted = "accepted-sha"
            create_journal(
                root,
                {
                    "sha": skipped,
                    "reason": MISSING_SQL_CODE_REASON,
                    "oracle_status": "skipped",
                },
            )
            create_journal(
                root,
                {
                    "sha": tested,
                    "reason": "нет правки SQL-тестов",
                    "oracle_status": "skipped",
                },
            )
            create_journal(
                root,
                {
                    "sha": accepted,
                    "reason": "есть правка SQL-кода и правка SQL-тестов",
                    "oracle_status": "accepted",
                },
            )
            known = harvest_known_shas(root)
            self.assertNotIn(skipped, known)
            self.assertIn(tested, known)
            self.assertIn(accepted, known)

    def test_accepted_sha_is_not_filtered_again(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sha = "already-accepted"
            create_journal(
                root,
                {
                    "sha": sha,
                    "repository_id": "sqlite",
                    "decision": "accept",
                    "reason_code": "accepted",
                    "reason": "есть правка SQL-кода и правка SQL-тестов",
                    "instance_id": "sqlite__sqlite-already",
                    "pr_number": "",
                    "recorded_at": "2026-10-18T00:00:00+00:00",
                    "oracle_status": "accepted",
                    "oracle_reason": "",
                },
            )
            state = filter_stage(_ready(sha), root)
            self.assertEqual(state["status"], "duplicate")
            self.assertEqual(len(list((root / "journal").glob("*.json"))), 1)
            self.assertEqual(len(list((root / "tasks").glob("*.json"))), 0)

    def test_missing_sql_code_skip_is_reopened(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sha = "reopen-sha"
            create_journal(
                root,
                {
                    "sha": sha,
                    "repository_id": "sqlite",
                    "decision": "reject",
                    "reason_code": "missing_code_and_tests",
                    "reason": MISSING_SQL_CODE_REASON,
                    "instance_id": "",
                    "pr_number": "",
                    "recorded_at": "2026-10-18T00:00:00+00:00",
                    "oracle_status": "skipped",
                    "oracle_reason": "",
                },
            )
            write_json(
                root / "changes" / sha / "change.json",
                change_to_dict(
                    _change(
                        "sqlite",
                        "Fix union",
                        [
                            FileChange("src/select.c", 2, 0, "modified"),
                            FileChange("test/unionall2.test", 2, 0, "modified"),
                        ],
                        sha,
                    )
                ),
            )
            state = filter_stage(_ready(sha), root)
            self.assertEqual(state["status"], "accepted")
            self.assertEqual(len(list((root / "journal").glob("*.json"))), 1)
            record = read_journal(root, sha)
            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.get("decision"), "accept")
            self.assertEqual(record.get("oracle_status"), "pending")

    def test_cursor_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(read_cursor(root, "sqlite"), "")
            write_cursor(root, "sqlite", "abc")
            self.assertEqual(read_cursor(root, "sqlite"), "abc")


if __name__ == "__main__":
    unittest.main()
