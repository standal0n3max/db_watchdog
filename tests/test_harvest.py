"""Обход истории: окно, курсор и повторный разбор пропуска «нет правки SQL-кода».

SQL-двадцатки трёх ядер (``labeled_sql_duckdb.json``, ``labeled_sql_postgres.json``, ``labeled_sql_sqlite.json``) сверяются с ``sql_only``. Отчёты пишутся во временный каталог.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from db_pipeline.config import load_cores, require_core
from db_pipeline.filter_rules import MISSING_SQL_CODE_REASON, MISSING_SQL_TEST_REASON, filter_candidate
from db_pipeline.git_ops import select_harvest_rows
from db_pipeline.journal import create_journal, harvest_known_shas, read_journal, write_json
from db_pipeline.models import ChangeView, FileChange, TaskInstance, change_to_dict
from db_pipeline.sql_report import write_sql_labeled_report
from db_pipeline.stages import (
    StageResult,
    compile_stage,
    discard_workdir,
    filter_stage,
    read_cursor,
    verify_stage,
    write_cursor,
)
from db_pipeline.verify_environment import OracleResult


def _ready(sha: str, core_id: str = "sqlite") -> StageResult:
    return {
        "status": "ready",
        "repository_id": core_id,
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
        self.assertEqual(cores["duckdb"].sql_test_prefixes, cores["duckdb"].test_prefixes)
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

    def test_duckdb_src_passes_with_sql_optimizer_or_api_test(self) -> None:
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
        src_and_api = _change(
            "duckdb",
            "Fix API (#4)",
            [
                FileChange("src/main/client_context.cpp", 2, 0, "modified"),
                FileChange("test/api/test_relation_api.cpp", 3, 0, "modified"),
            ],
        )
        self.assertTrue(filter_candidate(accepted, core, set(), sql_only=True).accept)
        self.assertFalse(filter_candidate(optimizer_only, core, set(), sql_only=True).accept)
        self.assertTrue(filter_candidate(src_and_optimizer, core, set(), sql_only=True).accept)
        self.assertTrue(filter_candidate(src_and_api, core, set(), sql_only=True).accept)

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

    def test_only_reopenable_harvest_skips_are_not_known(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            skipped = "skip-sql"
            duckdb_no_test = "duckdb-no-test"
            duckdb_rejected = "duckdb-rejected"
            sqlite_no_test = "sqlite-no-test"
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
                    "sha": duckdb_no_test,
                    "repository_id": "duckdb",
                    "reason": MISSING_SQL_TEST_REASON,
                    "oracle_status": "skipped",
                },
            )
            create_journal(
                root,
                {
                    "sha": duckdb_rejected,
                    "repository_id": "duckdb",
                    "reason": MISSING_SQL_TEST_REASON,
                    "oracle_status": "rejected",
                },
            )
            create_journal(
                root,
                {
                    "sha": sqlite_no_test,
                    "repository_id": "sqlite",
                    "reason": MISSING_SQL_TEST_REASON,
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
            self.assertNotIn(duckdb_no_test, known)
            self.assertIn(duckdb_rejected, known)
            self.assertIn(sqlite_no_test, known)
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

    def test_duckdb_missing_sql_test_skip_is_reopened(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sha = "reopen-duckdb-test"
            create_journal(
                root,
                {
                    "sha": sha,
                    "repository_id": "duckdb",
                    "decision": "reject",
                    "reason_code": "missing_code_and_tests",
                    "reason": MISSING_SQL_TEST_REASON,
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
                        "duckdb",
                        "Fix optimizer (#4)",
                        [
                            FileChange("src/optimizer/optimizer.cpp", 2, 0, "modified"),
                            FileChange("test/optimizer/aggregate_reuse.test", 3, 0, "modified"),
                        ],
                        sha,
                    )
                ),
            )
            state = filter_stage(_ready(sha, "duckdb"), root)
            self.assertEqual(state["status"], "accepted")
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

    def test_harvest_verify_removes_workdir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sha = "harvest-sha"
            state = _accepted_oracle_state(root, sha, "harvest")
            marker = root / "work" / sha / "build.o"
            marker.parent.mkdir(parents=True)
            marker.write_text("draft", encoding="utf-8")
            with patch("db_pipeline.stages.verify_task", return_value=_accepted_oracle()):
                result = verify_stage(state, root)
            record = read_journal(root, sha)
            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.get("oracle_status"), "accepted")
            self.assertEqual(result["oracle_status"], "accepted")
            self.assertFalse(marker.parent.exists())

    def test_pinned_verify_keeps_workdir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sha = "pinned-sha"
            state = _accepted_oracle_state(root, sha, "pinned")
            marker = root / "work" / sha / "build.o"
            marker.parent.mkdir(parents=True)
            marker.write_text("draft", encoding="utf-8")
            with patch("db_pipeline.stages.verify_task", return_value=_accepted_oracle()):
                verify_stage(state, root)
            record = read_journal(root, sha)
            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.get("oracle_status"), "accepted")
            self.assertEqual(marker.read_text(encoding="utf-8"), "draft")

    def test_missing_workdir_is_not_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sha = "missing-sha"
            state = _accepted_oracle_state(root, sha, "harvest")
            sibling = root / "work" / "other" / "keep.txt"
            sibling.parent.mkdir(parents=True)
            sibling.write_text("stay", encoding="utf-8")
            discard_workdir(root, "")
            discard_workdir(root, "absent")
            with patch("db_pipeline.stages.verify_task", return_value=_accepted_oracle()):
                verify_stage(state, root)
            record = read_journal(root, sha)
            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.get("oracle_status"), "accepted")
            self.assertFalse((root / "work" / sha).exists())
            self.assertEqual(sibling.read_text(encoding="utf-8"), "stay")

    def test_vanished_file_during_cleanup_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sha = "race-sha"
            work = root / "work" / sha
            work.mkdir(parents=True)
            (work / "gc.pid").write_text("1", encoding="utf-8")
            (work / "object.o").write_text("draft", encoding="utf-8")
            sibling = root / "work" / "other" / "keep.txt"
            sibling.parent.mkdir(parents=True)
            sibling.write_text("stay", encoding="utf-8")
            real_unlink = os.unlink

            def unlink(path: str | bytes, dir_fd: int | None = None) -> None:
                name = os.path.basename(os.fsdecode(path))
                if name == "gc.pid":
                    try:
                        real_unlink(path, dir_fd=dir_fd)
                    except FileNotFoundError:
                        pass
                    raise FileNotFoundError(2, "No such file or directory", "gc.pid")
                real_unlink(path, dir_fd=dir_fd)

            with patch("os.unlink", side_effect=unlink):
                discard_workdir(root, sha)
            self.assertFalse(work.exists())
            self.assertEqual(sibling.read_text(encoding="utf-8"), "stay")

    def test_pending_without_oracle_is_not_known_and_passes_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pending = "pending-sha"
            postgres_pending = "postgres-pending"
            with_log = "pending-with-log"
            accepted = "accepted-sha"
            rejected = "rejected-sha"
            create_journal(
                root,
                {
                    "sha": pending,
                    "repository_id": "sqlite",
                    "reason": "есть правка SQL-кода и правка SQL-тестов",
                    "oracle_status": "pending",
                    "oracle_reason": "",
                },
            )
            create_journal(
                root,
                {
                    "sha": postgres_pending,
                    "repository_id": "postgres",
                    "oracle_status": "pending",
                    "oracle_reason": "",
                },
            )
            create_journal(
                root,
                {
                    "sha": with_log,
                    "repository_id": "duckdb",
                    "oracle_status": "pending",
                    "oracle_reason": "",
                    "before_log": "/tmp/before.log",
                },
            )
            create_journal(
                root,
                {
                    "sha": accepted,
                    "repository_id": "sqlite",
                    "oracle_status": "accepted",
                    "oracle_reason": "тесты упали до эталона и прошли после",
                },
            )
            create_journal(
                root,
                {
                    "sha": rejected,
                    "repository_id": "duckdb",
                    "oracle_status": "rejected",
                    "oracle_reason": "тесты из test_patch прошли до эталона",
                },
            )
            known = harvest_known_shas(root)
            self.assertNotIn(pending, known)
            self.assertNotIn(postgres_pending, known)
            self.assertIn(with_log, known)
            self.assertIn(accepted, known)
            self.assertIn(rejected, known)
            write_json(
                root / "changes" / pending / "change.json",
                change_to_dict(
                    _change(
                        "sqlite",
                        "Fix union",
                        [
                            FileChange("src/select.c", 2, 0, "modified"),
                            FileChange("test/unionall2.test", 2, 0, "modified"),
                        ],
                        pending,
                    )
                ),
            )
            state = filter_stage(_ready(pending), root)
            self.assertEqual(state["status"], "accepted")
            record = read_journal(root, pending)
            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.get("oracle_status"), "pending")
            self.assertEqual(record.get("decision"), "accept")

    def test_pending_task_json_stays_open_for_oracle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sha = "pending-task"
            instance_id = "sqlite__sqlite-pending-tas"
            task_path = root / "tasks" / f"{instance_id}.json"
            write_json(task_path, {"instance_id": instance_id, "head_sha": sha})
            create_journal(
                root,
                {
                    "sha": sha,
                    "repository_id": "sqlite",
                    "instance_id": instance_id,
                    "oracle_status": "pending",
                    "oracle_reason": "",
                },
            )
            folder = root / "changes" / sha
            folder.mkdir(parents=True)
            write_json(
                folder / "change.json",
                change_to_dict(
                    _change(
                        "sqlite",
                        "Fix date",
                        [FileChange("src/date.c", 1, 0, "modified")],
                        sha,
                    )
                ),
            )
            (folder / "statement.txt").write_text("Fix date", encoding="utf-8")
            (folder / "statement_source.txt").write_text("commit_message", encoding="utf-8")
            (folder / "diff.patch").write_text("", encoding="utf-8")
            state = _ready(sha)
            state["status"] = "accepted"
            task = TaskInstance(
                instance_id=instance_id,
                repository="sqlite/sqlite",
                license="public domain",
                base_sha="base",
                statement="Fix date",
                statement_source="commit_message",
                test_patch="",
                gold_patch="",
                fail_to_pass=["date"],
                pass_to_pass=[],
                oracle="testfixture",
                reject_reason="",
                head_sha=sha,
                pr_number=None,
            )
            with patch("db_pipeline.stages.compile_task", return_value=task):
                result = compile_stage(state, root)
            self.assertEqual(result["status"], "accepted")
            self.assertEqual(result["task_path"], str(task_path))
            self.assertEqual(len(list((root / "tasks").glob("*.json"))), 1)


def _accepted_oracle() -> OracleResult:
    return OracleResult("accepted", "accepted", "тесты упали до эталона и прошли после", "", "", "")


def _accepted_oracle_state(root: Path, sha: str, detail: str) -> StageResult:
    instance_id = f"sqlite__sqlite-{sha}"
    task_path = root / "tasks" / f"{instance_id}.json"
    write_json(
        task_path,
        {
            "instance_id": instance_id,
            "repository": "sqlite",
            "license": "public domain",
            "base_sha": "base",
            "statement": "Fix date",
            "statement_source": "commit_message",
            "test_patch": "",
            "gold_patch": "",
            "fail_to_pass": ["date"],
            "pass_to_pass": [],
            "oracle": "testfixture",
            "reject_reason": "",
            "head_sha": sha,
            "pr_number": None,
            "oracle_command": "",
        },
    )
    create_journal(
        root,
        {
            "sha": sha,
            "repository_id": "sqlite",
            "decision": "accept",
            "reason_code": "accepted",
            "reason": "есть правка SQL-кода и правка SQL-тестов",
            "instance_id": instance_id,
            "pr_number": "",
            "recorded_at": "2026-10-20T00:00:00+00:00",
            "oracle_status": "pending",
            "oracle_reason": "",
        },
    )
    state = _ready(sha)
    state["status"] = "accepted"
    state["instance_id"] = instance_id
    state["task_path"] = str(task_path)
    state["detail"] = detail
    return state


if __name__ == "__main__":
    unittest.main()
