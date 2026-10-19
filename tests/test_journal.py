from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from db_pipeline.journal import accepted_count, create_journal, known_shas, write_json_once
from db_pipeline.stages import filter_stage


class JournalTests(unittest.TestCase):
    def test_second_pass_does_not_create_another_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            sha = "abc123"
            created = create_journal(
                data,
                {
                    "sha": sha,
                    "repository_id": "duckdb",
                    "decision": "accept",
                    "reason_code": "accepted",
                    "reason": "есть правка кода ядра и правка тестов",
                    "instance_id": "duckdb__duckdb-1",
                    "pr_number": "1",
                    "recorded_at": "2026-10-10T00:00:00+00:00",
                    "oracle_status": "pending",
                    "oracle_reason": "",
                },
            )
            self.assertTrue(created)
            task = data / "tasks" / "duckdb__duckdb-1.json"
            self.assertTrue(write_json_once(task, {"instance_id": "duckdb__duckdb-1"}))
            state = filter_stage(
                {
                    "status": "ready",
                    "repository_id": "duckdb",
                    "sha": sha,
                    "base_sha": "",
                    "pr_number": "1",
                    "instance_id": "",
                    "reason_code": "",
                    "reason": "",
                    "statement_source": "",
                    "task_path": "",
                    "oracle_status": "",
                    "run_path": "",
                    "detail": "",
                },
                data,
            )
            self.assertEqual(state["status"], "duplicate")
            self.assertEqual(len(list((data / "journal").glob("*.json"))), 1)
            self.assertEqual(len(list((data / "tasks").glob("*.json"))), 1)
            self.assertEqual(known_shas(data), {sha})
            again = create_journal(data, {"sha": sha})
            self.assertFalse(again)

    def test_accepted_count_reads_oracle_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            create_journal(
                data,
                {
                    "sha": "aa",
                    "repository_id": "sqlite",
                    "oracle_status": "accepted",
                },
            )
            create_journal(
                data,
                {
                    "sha": "bb",
                    "repository_id": "sqlite",
                    "oracle_status": "rejected",
                },
            )
            create_journal(
                data,
                {
                    "sha": "cc",
                    "repository_id": "duckdb",
                    "oracle_status": "accepted",
                },
            )
            self.assertEqual(accepted_count(data, "sqlite"), 1)
            self.assertEqual(accepted_count(data, "postgres"), 0)


if __name__ == "__main__":
    unittest.main()
