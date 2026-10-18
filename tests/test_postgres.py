from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from db_pipeline.compile_task import compile_task
from db_pipeline.config import load_cores, require_core
from db_pipeline.filter_rules import filter_candidate
from db_pipeline.journal import create_journal, read_journal, write_json, write_json_once
from db_pipeline.models import ChangeView, FileChange, TaskInstance, task_from_dict, task_to_dict
from db_pipeline.parsing import load_json_file
from db_pipeline.patch_split import paths_in_diff, split_by_paths
from db_pipeline.postgres_report import write_postgres_corpus_report, write_postgres_labeled_report
from db_pipeline.samples import limit_regress_command, sample_by_name
from db_pipeline.stages import filter_stage, reverify_targets, save_oracle_result, watch_repos
from db_pipeline.verify_environment import (
    OracleResult,
    classify_failed_run,
    host_user_script,
    incremental_build_command,
    oracle_shell_command,
)


class PostgresFilterTests(unittest.TestCase):
    def test_labeled_twenty_matches_frozen_marks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            matched, total = write_postgres_labeled_report(data=Path(tmp))
        self.assertEqual(total, 20)
        self.assertGreaterEqual(matched, 16)

    def test_every_sample_gets_a_decision_without_docker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            matched, total = write_postgres_corpus_report(data=Path(tmp))
        self.assertEqual(total, 89)
        self.assertGreaterEqual(matched, 16)

    def test_path_outside_sample_lists_is_rejected(self) -> None:
        core = require_core(load_cores(), "postgres")
        change = ChangeView(
            sha="outside",
            repository_id="postgres",
            subject="Fix integer overflow",
            body="",
            parent_count=1,
            files=[
                FileChange("src/backend/utils/adt/formatting.c", 4, 1, "modified"),
                FileChange("src/test/regress/sql/int4.sql", 3, 0, "modified"),
                FileChange("doc/src/sgml/func.sgml", 2, 0, "modified"),
            ],
            pr_number=None,
            base_sha="parent",
        )
        decision = filter_candidate(
            change,
            core,
            set(),
            ["src/backend/utils/adt/formatting.c"],
            ["src/test/regress/sql/int4.sql", "src/test/regress/expected/int4.out"],
        )
        self.assertFalse(decision.accept)
        self.assertEqual(decision.reason_code, "paths_outside_sample")

    def test_saved_diff_keeps_regress_out_of_gold(self) -> None:
        diff_path = Path(__file__).resolve().parent / "fixtures" / "pg_int4.diff"
        diff_text = diff_path.read_text(encoding="utf-8")
        solution = ["src/backend/utils/adt/formatting.c"]
        tests = ["src/test/regress/sql/int4.sql", "src/test/regress/expected/int4.out"]
        test_patch, gold_patch = split_by_paths(diff_text, solution, tests)
        gold_paths = paths_in_diff(gold_patch)
        test_paths = paths_in_diff(test_patch)
        self.assertEqual(gold_paths, solution)
        self.assertEqual(sorted(test_paths), sorted(tests))
        for path in gold_paths:
            self.assertNotIn(path, tests)
        core = require_core(load_cores(), "postgres")
        change = ChangeView(
            sha="04c25af21c02bf2c5152c3c8ab2ba877c231a598",
            repository_id="postgres",
            subject="Fix integer to_char() overflow with V format",
            body="",
            parent_count=1,
            files=[FileChange(path, 1, 0, "modified") for path in solution + tests],
            pr_number=None,
            base_sha="e96b27296bec1c6212874eceb394b4f8b7d06dcc",
        )
        task = compile_task(
            change,
            core,
            diff_text,
            "goal",
            "commit_message",
            gold_paths=solution,
            test_paths=tests,
            fail_to_pass=["int4"],
            oracle_command="make -C src/test/regress check TESTS=int4",
        )
        self.assertEqual(task.fail_to_pass, ["int4"])
        self.assertEqual(task.statement_source, "commit_message")
        self.assertNotIn("int4.out", task.gold_patch)
        self.assertIn("formatting.c", task.gold_patch)

    def test_harvest_split_does_not_need_sample_lists(self) -> None:
        diff_path = Path(__file__).resolve().parent / "fixtures" / "pg_int4.diff"
        diff_text = diff_path.read_text(encoding="utf-8")
        core = require_core(load_cores(), "postgres")
        change = ChangeView(
            sha="04c25af21c02bf2c5152c3c8ab2ba877c231a598",
            repository_id="postgres",
            subject="Fix integer to_char() overflow with V format",
            body="",
            parent_count=1,
            files=[],
            pr_number=None,
            base_sha="e96b27296bec1c6212874eceb394b4f8b7d06dcc",
        )
        task = compile_task(
            change,
            core,
            diff_text,
            "сообщение коммита",
            "commit_message",
            regress_split=True,
            oracle_command="make -C src/test/regress check-tests TESTS={test}",
        )
        gold_paths = paths_in_diff(task.gold_patch)
        test_paths = paths_in_diff(task.test_patch)
        self.assertEqual(task.fail_to_pass, ["int4"])
        self.assertIn("src/backend/utils/adt/formatting.c", gold_paths)
        self.assertNotIn("src/test/regress/expected/int4.out", gold_paths)
        self.assertNotIn("src/test/regress/sql/int4.sql", gold_paths)
        self.assertIn("src/test/regress/expected/int4.out", test_paths)
        self.assertIn("src/test/regress/sql/int4.sql", test_paths)
        self.assertNotIn("src/backend/utils/adt/formatting.c", test_paths)

    def test_regress_tests_variable_uses_check_tests_target(self) -> None:
        command = "make -C src/test/regress check TESTS=union"
        self.assertEqual(
            limit_regress_command(command),
            "make -C src/test/regress check-tests TESTS=union",
        )
        self.assertEqual(limit_regress_command("make check"), "make check")

    def test_task_union_command_is_what_the_container_runs(self) -> None:
        core = require_core(load_cores(), "postgres")
        task = _postgres_task("make -C src/test/regress check TESTS=union")
        command = oracle_shell_command(core, task, "make -C src/test/regress check TESTS={test}")
        self.assertEqual(command, 'make -C src/test/regress check-tests TESTS="test_setup union"')
        script = host_user_script(command)
        self.assertIn('make -C src/test/regress check-tests TESTS="test_setup union"', script)
        self.assertNotIn("check TESTS=union", script)
        self.assertNotIn("parallel_schedule", script)

    def test_repos_template_is_limited_the_same_way(self) -> None:
        core = require_core(load_cores(), "postgres")
        stored = replace(core, oracle_command="make -C src/test/regress check TESTS={test}")
        task = _postgres_task("")
        command = oracle_shell_command(stored, task, "make check")
        self.assertEqual(command, 'make -C src/test/regress check-tests TESTS="test_setup {test}"')

    def test_low_uid_keeps_host_id_and_clears_old_data_dir(self) -> None:
        script = host_user_script("make -j1")
        self.assertIn('if [ "$HOST_UID" -lt 1000 ]; then', script)
        self.assertIn('groupadd -o -g "$HOST_GID" hostgroup', script)
        self.assertIn('useradd -o -u "$HOST_UID" -g "$HOST_GID"', script)
        self.assertLess(script.index('chown -R "$HOST_UID:$HOST_GID" /src'), script.index("exec runuser"))
        cleanup = "rm -rf tmp_install src/test/regress/tmp_check src/test/regress/log"
        self.assertGreater(script.index(cleanup), script.index("exec runuser"))
        self.assertIn("EXTRA_REGRESS_OPTS=--temp-instance=/tmp/pg-regress-instance", script)
        self.assertGreater(script.index("EXTRA_REGRESS_OPTS"), script.index("exec runuser"))
        self.assertIn(')" -- "$@"', script)

    def test_postmaster_failure_is_environment_and_keeps_startup_logs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            log = work / "after.log"
            log.write_text(
                '# postmaster failed, examine "/src/src/test/regress/log/postmaster.log" for the reason\n',
                encoding="utf-8",
            )
            postmaster = work / "src/test/regress/log"
            postmaster.mkdir(parents=True)
            (postmaster / "postmaster.log").write_text("data directory has wrong ownership\n", encoding="utf-8")
            initdb = work / "tmp_install/log"
            initdb.mkdir(parents=True)
            (initdb / "initdb-template.log").write_text("initdb: ok\n", encoding="utf-8")
            code, reason = classify_failed_run("postgres", work, log)
            text = log.read_text(encoding="utf-8")
        self.assertEqual(code, "postmaster_failed")
        self.assertEqual(reason, "postmaster не стартовал")
        self.assertIn("# src/test/regress/log/postmaster.log", text)
        self.assertIn("data directory has wrong ownership", text)
        self.assertIn("# tmp_install/log/initdb-template.log", text)
        self.assertIn("initdb: ok", text)

    def test_ordinary_test_failure_stays_a_gold_miss(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            log = work / "after.log"
            log.write_text("not ok 1 union\n", encoding="utf-8")
            code, reason = classify_failed_run("postgres", work, log)
        self.assertEqual((code, reason), ("gold_did_not_fix", "эталон не починил падение тестов"))

    def test_reverify_updates_the_same_json_and_stops_at_three(self) -> None:
        core = require_core(load_cores(), "postgres")
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            expected: list[str] = []
            for name in core.sample_candidates:
                sample = sample_by_name(core, name)
                expected.append(sample.sha)
                instance_id = f"postgres__postgres-{sample.sha[:12]}"
                _store_saved_task(data, sample.sha, instance_id)
            _store_saved_task(data, "a" * 40, "postgres__postgres-extra")
            targets = reverify_targets(data, core)
            self.assertEqual([item[0] for item in targets], expected)
            self.assertEqual(len(list((data / "tasks").glob("*.json"))), 4)
            sha, task_path, task = targets[0]
            save_oracle_result(
                data,
                sha,
                task_path,
                task,
                OracleResult("rejected", "postmaster_failed", "postmaster не стартовал", "", "", ""),
            )
            self.assertEqual(len(list((data / "tasks").glob("*.json"))), 4)
            self.assertEqual(len(list((data / "journal").glob("*.json"))), 4)
            saved = task_from_dict(load_json_file(task_path))
            self.assertEqual(saved.reject_reason, "postmaster не стартовал")
            self.assertEqual(saved.oracle_command, task.oracle_command)
            journal = read_journal(data, sha)
            self.assertIsNotNone(journal)
            assert journal is not None
            self.assertEqual(journal.get("oracle_status"), "rejected")
            self.assertEqual(journal.get("oracle_reason"), "postmaster не стартовал")
            save_oracle_result(
                data,
                sha,
                task_path,
                task,
                OracleResult("accepted", "accepted", "тесты упали до эталона и прошли после", "", "", ""),
            )
            accepted = task_from_dict(load_json_file(task_path))
            self.assertEqual(accepted.reject_reason, "")
            self.assertEqual(len(list((data / "tasks").glob("*.json"))), 4)

    def test_three_rejections_still_stop_the_search(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            for index in range(3):
                sha = f"{index}" * 40
                create_journal(
                    data,
                    {
                        "sha": sha,
                        "repository_id": "postgres",
                        "decision": "accept",
                        "reason_code": "accepted",
                        "reason": "есть",
                        "instance_id": f"postgres__postgres-{index}",
                        "pr_number": "",
                        "recorded_at": "2026-10-12T00:00:00+00:00",
                        "oracle_status": "rejected",
                        "oracle_reason": "старая причина",
                    },
                )
            state = watch_repos(data, core_id="postgres")
        self.assertEqual(state["status"], "rejected")
        self.assertEqual(state["reason_code"], "watch_failed")
        self.assertIn("три отказа оракула", state["reason"])

    def test_stub_makefile_does_not_skip_configure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            (work / "Makefile").write_text("all:\n\t@echo run configure\n", encoding="utf-8")
            command = "./configure --without-readline && make -j1"
            self.assertEqual(incremental_build_command(work, command), command)
            (work / "config.status").write_text("configured\n", encoding="utf-8")
            self.assertEqual(incremental_build_command(work, command), "make -j1")

    def test_repeat_sha_does_not_create_a_second_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            sha = "04c25af21c02bf2c5152c3c8ab2ba877c231a598"
            self.assertTrue(
                create_journal(
                    data,
                    {
                        "sha": sha,
                        "repository_id": "postgres",
                        "decision": "accept",
                        "reason_code": "accepted",
                        "reason": "есть solution и test_files, diff внутри списков",
                        "instance_id": "postgres__postgres-04c25af21c02",
                        "pr_number": "",
                        "recorded_at": "2026-10-10T00:00:00+00:00",
                        "oracle_status": "pending",
                        "oracle_reason": "",
                    },
                )
            )
            task = data / "tasks" / "postgres__postgres-04c25af21c02.json"
            self.assertTrue(write_json_once(task, {"instance_id": "postgres__postgres-04c25af21c02"}))
            state = filter_stage(
                {
                    "status": "ready",
                    "repository_id": "postgres",
                    "sha": sha,
                    "base_sha": "parent",
                    "pr_number": "",
                    "instance_id": "",
                    "reason_code": "",
                    "reason": "",
                    "statement_source": "commit_message",
                    "task_path": "",
                    "oracle_status": "",
                    "run_path": "",
                    "detail": "pg_int4_integer_char_overflow",
                },
                data,
            )
            self.assertEqual(state["status"], "duplicate")
            self.assertEqual(len(list((data / "journal").glob("*.json"))), 1)
            self.assertEqual(len(list((data / "tasks").glob("*.json"))), 1)


def _postgres_task(command: str) -> TaskInstance:
    return TaskInstance(
        instance_id="postgres__postgres-93ed18720105",
        repository="postgres/postgres",
        license="PostgreSQL License",
        base_sha="parent",
        statement="goal",
        statement_source="commit_message",
        test_patch="",
        gold_patch="",
        fail_to_pass=["union"],
        pass_to_pass=[],
        oracle="pg_regress",
        reject_reason="эталон не починил падение тестов",
        head_sha="93ed18720105ebacfd2a2bdeb55bf33feaf2ec80",
        pr_number=None,
        oracle_command=command,
    )


def _store_saved_task(data: Path, sha: str, instance_id: str) -> None:
    create_journal(
        data,
        {
            "sha": sha,
            "repository_id": "postgres",
            "decision": "accept",
            "reason_code": "accepted",
            "reason": "есть solution и test_files, diff внутри списков",
            "instance_id": instance_id,
            "pr_number": "",
            "recorded_at": "2026-10-10T00:00:00+00:00",
            "oracle_status": "rejected",
            "oracle_reason": "старая причина",
        },
    )
    task = _postgres_task("make -C src/test/regress check TESTS=union")
    stored = task_to_dict(task)
    stored["instance_id"] = instance_id
    stored["head_sha"] = sha
    write_json(data / "tasks" / f"{instance_id}.json", stored)


if __name__ == "__main__":
    unittest.main()
