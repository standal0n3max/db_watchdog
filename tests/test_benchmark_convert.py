"""Конвертер, contract extraction и разбор JSONL OpenCode.

Проверяются чистка statement, вывод area, извлечение contract по точному тексту
ошибок, соответствие graded_by тестовым файлам и разбор JSONL-событий OpenCode.
Сеть и Docker не нужны.
"""

from __future__ import annotations

import json
import unittest

from db_pipeline.benchmark import convert, tasks
from db_pipeline.benchmark.models import PublicTask
from db_pipeline.benchmark.opencode import parse_jsonl
from db_pipeline.models import TaskInstance


def _instance(**overrides: object) -> TaskInstance:
    base: dict[str, object] = {
        "instance_id": "postgres__postgres-abcdef123456",
        "repository": "postgres/postgres",
        "license": "PostgreSQL License",
        "base_sha": "b" * 40,
        "statement": "Fix the bug\n\nBody line.\n\nReported-by: Someone <a@b.c>\nDiscussion: https://example.com\n",
        "statement_source": "commit_message",
        "test_patch": "",
        "gold_patch": "",
        "fail_to_pass": ["some_test"],
        "pass_to_pass": [],
        "oracle": "pg_regress",
        "reject_reason": "",
        "head_sha": "abcdef123456" + "0" * 28,
        "pr_number": None,
        "oracle_command": "make check",
    }
    base.update(overrides)
    return TaskInstance(**base)  # type: ignore[arg-type]


class StatementTests(unittest.TestCase):
    def test_trailers_are_removed(self) -> None:
        cleaned = tasks.clean_statement(
            "Fix bug\n\nBody.\n\nReported-by: X <x@y.z>\nDiscussion: https://x\nBackpatch-through: 17\n"
        )
        self.assertNotIn("Reported-by", cleaned)
        self.assertNotIn("Discussion", cleaned)
        self.assertNotIn("Backpatch-through", cleaned)
        self.assertIn("Body.", cleaned)

    def test_subject_is_first_line(self) -> None:
        self.assertEqual(tasks.statement_subject("Fix bug\n\nBody."), "Fix bug")

    def test_area_from_path(self) -> None:
        area = tasks.derive_area("Fix planner bug", ["src/backend/optimizer/util/relnode.c"])
        self.assertIn("query planner", area)
        self.assertIn("Fix planner bug", area)


class ContractTests(unittest.TestCase):
    def test_contract_requires_test_anchor(self) -> None:
        reference = (
            "diff --git a/src/backend/x.c b/src/backend/x.c\n"
            "--- a/src/backend/x.c\n+++ b/src/backend/x.c\n"
            "+ errmsg(\"label \\\"%s\\\" already exists\", label));\n"
        )
        present = 'ERROR:  label "l1" already exists\n'
        absent = "no match here\n"
        self.assertEqual(
            tasks.extract_contract(reference, present),
            ['label "%s" already exists'],
        )
        self.assertEqual(tasks.extract_contract(reference, absent), [])


class ConvertTests(unittest.TestCase):
    def test_public_has_no_reference(self) -> None:
        gold = (
            "diff --git a/src/backend/x.c b/src/backend/x.c\n"
            "--- a/src/backend/x.c\n+++ b/src/backend/x.c\n"
            "+ return 0;\n"
        )
        test = (
            "diff --git a/src/test/regress/expected/t.out b/src/test/regress/expected/t.out\n"
            "--- a/src/test/regress/expected/t.out\n+++ b/src/test/regress/expected/t.out\n"
            "+ ok\n"
        )
        payload = convert.convert_one(
            _instance(gold_patch=gold, test_patch=test), data=None
        )
        public = payload["public"]
        self.assertIsInstance(public, PublicTask)
        assert isinstance(public, PublicTask)
        serialized = json.dumps(public.to_dict())
        self.assertNotIn("return 0", serialized)
        self.assertNotIn("diff --git", serialized)
        self.assertEqual(public.graded_by, ["src/test/regress/expected/t.out"])

    def test_func_name_uses_head(self) -> None:
        self.assertEqual(convert.func_name(_instance()), "postgres__abcdef123456")


class OpenCodeJsonlTests(unittest.TestCase):
    def test_events_are_grouped(self) -> None:
        lines = [
            json.dumps(
                {
                    "type": "tool_use",
                    "part": {
                        "tool": "read",
                        "messageID": "m1",
                        "state": {
                            "status": "completed",
                            "input": {"path": "x.c"},
                            "output": "bytes",
                            "time": {"start": 1000, "end": 2000},
                        },
                    },
                }
            ),
            json.dumps(
                {
                    "type": "step_finish",
                    "part": {
                        "messageID": "m1",
                        "reason": "stop",
                        "cost": 0.01,
                        "tokens": {
                            "input": 100,
                            "output": 20,
                            "reasoning": 5,
                            "cache": {"read": 3, "write": 1},
                        },
                    },
                }
            ),
            json.dumps({"type": "text", "part": {"messageID": "m1", "text": "done"}}),
        ]
        transcript = parse_jsonl("\n".join(lines))
        self.assertEqual(len(transcript.tools), 1)
        self.assertEqual(transcript.tools[0].tool, "read")
        self.assertEqual(transcript.tools[0].seconds, 1.0)
        self.assertEqual(len(transcript.generations), 1)
        self.assertEqual(transcript.generations[0].input_tokens, 100)
        self.assertEqual(transcript.generations[0].text, "done")
        self.assertEqual(transcript.tool_counts(), {"read": 1})

    def test_bad_lines_are_ignored(self) -> None:
        transcript = parse_jsonl("not json\n\n{}\n")
        self.assertEqual(transcript.raw_events, 1)

    def test_compaction_pair_marks_summary_and_drops_continue(self) -> None:
        from db_pipeline.benchmark import opencode

        marker = opencode.CONTINUE_MARKER
        lines = [
            json.dumps(
                {
                    "type": "text",
                    "timestamp": 1000,
                    "sessionID": "ses_1",
                    "part": {"messageID": "sum", "text": "## Objective\nkeep going"},
                }
            ),
            json.dumps(
                {
                    "type": "step_finish",
                    "timestamp": 1100,
                    "sessionID": "ses_1",
                    "part": {
                        "messageID": "sum",
                        "reason": "stop",
                        "tokens": {
                            "input": 24000,
                            "output": 400,
                            "reasoning": 10,
                            "cache": {"read": 100},
                        },
                    },
                }
            ),
            json.dumps(
                {
                    "type": "text",
                    "timestamp": 1200,
                    "sessionID": "ses_1",
                    "part": {
                        "messageID": "cont",
                        "synthetic": True,
                        "metadata": {"compaction_continue": True},
                        "text": marker,
                    },
                }
            ),
            json.dumps(
                {
                    "type": "step_finish",
                    "timestamp": 1400,
                    "sessionID": "ses_1",
                    "part": {
                        "messageID": "next",
                        "reason": "stop",
                        "tokens": {"input": 15000, "output": 80, "cache": {"read": 50}},
                    },
                }
            ),
            json.dumps(
                {
                    "type": "text",
                    "timestamp": 1300,
                    "sessionID": "ses_1",
                    "part": {"messageID": "next", "text": "## Combined Summary"},
                }
            ),
        ]
        transcript = parse_jsonl("\n".join(lines))
        self.assertEqual(transcript.session_id, "ses_1")
        self.assertEqual(len(transcript.generations), 2)
        self.assertTrue(transcript.generations[0].compaction)
        self.assertFalse(transcript.generations[1].compaction)
        self.assertEqual(transcript.generations[1].text, "## Combined Summary")
        self.assertEqual(opencode.metrics(transcript)["compactions"], 1)
        self.assertEqual(opencode.metrics(transcript)["max_context"], 24100)
        self.assertEqual(opencode.metrics(transcript)["first_edit_step"], -1)


if __name__ == "__main__":
    unittest.main()
