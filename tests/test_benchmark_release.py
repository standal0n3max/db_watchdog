"""Выгрузка датасета: стабильные id, лицензии, манифест и его детерминизм.

Docker, сеть и клон ядра не нужны: тесты работают на синтетическом датасете в
temp-каталоге, ровно в том формате, какой пишет конвертер.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import cast

from db_pipeline.benchmark import release
from db_pipeline.errors import PipelineError

SHORTS = ("aaaa11112222", "bbbb33334444", "cccc55556666")
IDS = tuple(f"postgres__{short}" for short in SHORTS)


def _head(short: str) -> str:
    return f"{short}{'0' * 28}"


def _public(name: str, area: str = "query planner, Fix bug") -> dict[str, object]:
    return {
        "func": name,
        "area": area,
        "goal": f"Fix the bug in {name}",
        "contract": [],
        "graded_by": ["src/test/regress/sql/t.sql"],
    }


def _oracle(name: str, head: str, base: str) -> dict[str, object]:
    return {
        "func": name,
        "head": head,
        "base": base,
        "source": "postgres/postgres",
        "patch": f"prep/{name}.patch",
        "reverse_patch": "diff --git a/src/backend/x.c b/src/backend/x.c\n+ return 0;\n",
        "test": "diff --git a/src/test/regress/sql/t.sql b/src/test/regress/sql/t.sql\n+ ok\n",
        "test_files": ["src/test/regress/sql/t.sql"],
        "reference": "diff --git a/src/backend/x.c b/src/backend/x.c\n- return 1;\n",
        "oracle_command": "make -C src/test/regress check-tests TESTS={test}",
        "expected_tests": ["t"],
    }


def _card(head: str) -> dict[str, object]:
    return {
        "instance_id": f"postgres__postgres-{head[:12]}",
        "repository": "postgres/postgres",
        "license": "PostgreSQL License",
        "base_sha": "1" * 40,
        "statement": "Fix the bug",
        "statement_source": "commit_message",
        "test_patch": "",
        "gold_patch": "",
        "fail_to_pass": ["t"],
        "pass_to_pass": [],
        "oracle": "pg_regress",
        "reject_reason": "",
        "head_sha": head,
        "pr_number": None,
        "oracle_command": "",
    }


class ReleaseFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="release-")
        self.data = Path(self.tmp.name)
        (self.data / "benchmark" / "postgres" / "tasks").mkdir(parents=True)
        (self.data / "benchmark" / "postgres" / "oracle").mkdir(parents=True)
        (self.data / "tasks").mkdir(parents=True)
        for short in SHORTS:
            self._add(f"postgres__{short}", _head(short))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _add(self, name: str, head: str) -> None:
        base = "1" * 40
        tasks = self.data / "benchmark" / "postgres" / "tasks"
        oracle = self.data / "benchmark" / "postgres" / "oracle"
        (tasks / f"{name}.json").write_text(
            json.dumps(_public(name), ensure_ascii=False), encoding="utf-8"
        )
        (oracle / f"{name}.json").write_text(
            json.dumps(_oracle(name, head, base), ensure_ascii=False), encoding="utf-8"
        )
        card = f"postgres__postgres-{head[:12]}.json"
        (self.data / "tasks" / card).write_text(
            json.dumps(_card(head), ensure_ascii=False), encoding="utf-8"
        )


class CollectTests(ReleaseFixture):
    def _split_map(self) -> dict[str, str]:
        return {
            "postgres__aaaa11112222": "dev",
            "postgres__bbbb33334444": "tune",
            "postgres__cccc55556666": "holdout",
        }

    def test_ids_and_metadata(self) -> None:
        records = release.collect(self.data, split_map=self._split_map())
        self.assertEqual([r.id for r in records], sorted(IDS))
        first = records[0]
        self.assertEqual(first.license, "PostgreSQL License")
        self.assertEqual(first.source, "postgres/postgres")
        self.assertEqual(first.oracle_name, "pg_regress")
        self.assertEqual(len(first.head), 40)
        self.assertEqual(first.split, "dev")

    def test_id_is_derived_from_head(self) -> None:
        records = release.collect(self.data, split_map=self._split_map())
        for record in records:
            self.assertEqual(record.id.split("__")[1], record.head[:12])

    def test_public_dict_has_no_answer(self) -> None:
        records = release.collect(self.data, split_map=self._split_map())
        text = json.dumps(records[0].public_dict(), ensure_ascii=False)
        self.assertNotIn("diff --git", text)
        self.assertNotIn("reverse_patch", text)
        self.assertNotIn("return 0", text)

    def test_split_falls_back_to_unassigned(self) -> None:
        records = release.collect(self.data, split_map={"postgres__aaaa11112222": "dev"})
        others = [r.split for r in records if r.id != "postgres__aaaa11112222"]
        self.assertEqual(set(others), {release.UNASSIGNED})

    def test_missing_source_card_is_an_error(self) -> None:
        (self.data / "tasks" / f"postgres__postgres-{SHORTS[0]}.json").unlink()
        with self.assertRaises(PipelineError):
            release.collect(self.data, split_map={})


class BuildTests(ReleaseFixture):
    def setUp(self) -> None:
        super().setUp()
        self.out = self.data / "releases" / "0.1.0"

    def _split_map(self) -> dict[str, str]:
        return {
            "postgres__aaaa11112222": "dev",
            "postgres__bbbb33334444": "tune",
            "postgres__cccc55556666": "holdout",
        }

    def _build(self, with_oracle: bool = False, force: bool = False) -> dict[str, object]:
        return release.build(
            data=self.data,
            target=self.out,
            version="0.1.0",
            with_oracle=with_oracle,
            force=force,
            split_map=self._split_map(),
        )

    def test_build_writes_expected_files(self) -> None:
        manifest = self._build()
        self.assertTrue((self.out / "manifest.json").is_file())
        self.assertTrue((self.out / "split.json").is_file())
        self.assertTrue((self.out / "licenses.md").is_file())
        self.assertTrue((self.out / "README.md").is_file())
        self.assertEqual(len(list((self.out / "tasks").glob("*.json"))), 3)
        self.assertEqual(manifest["tasks"], 3)
        self.assertEqual(manifest["format"], release.FORMAT)

    def test_oracle_layer_is_optional(self) -> None:
        self._build(with_oracle=False)
        self.assertFalse((self.out / "oracle").exists())
        self._build(with_oracle=True, force=True)
        self.assertEqual(len(list((self.out / "oracle").glob("*.json"))), 3)

    def test_manifest_is_deterministic(self) -> None:
        first = self._build()
        second = self._build(force=True)
        self.assertEqual(first["content_sha256"], second["content_sha256"])
        self.assertEqual(first["files"], second["files"])

    def test_manifest_has_no_build_time(self) -> None:
        manifest = self._build()
        text = (self.out / "manifest.json").read_text(encoding="utf-8")
        for forbidden in ("timestamp", "generated_at", "built_at", str(self.data)):
            self.assertNotIn(forbidden, text)
        self.assertIn("content_sha256", manifest)

    def test_nonempty_target_needs_force(self) -> None:
        self._build()
        with self.assertRaises(PipelineError):
            self._build()

    def test_splits_counts_match(self) -> None:
        manifest = self._build()
        splits = cast("dict[str, int]", manifest["splits"])
        self.assertEqual(splits["dev"], 1)
        self.assertEqual(splits["tune"], 1)
        self.assertEqual(splits["holdout"], 1)


class VerifyTests(ReleaseFixture):
    def setUp(self) -> None:
        super().setUp()
        self.out = self.data / "releases" / "0.1.0"
        release.build(
            data=self.data,
            target=self.out,
            version="0.1.0",
            split_map={
                "postgres__aaaa11112222": "dev",
                "postgres__bbbb33334444": "tune",
                "postgres__cccc55556666": "holdout",
            },
        )

    def test_clean_release_verifies(self) -> None:
        report = release.verify(self.out)
        self.assertTrue(report["ok"])
        self.assertEqual(report["tasks"], 3)
        self.assertEqual(report["missing"], [])
        self.assertEqual(report["mismatched"], [])
        self.assertEqual(report["extra"], [])
        self.assertTrue(report["content_sha256_ok"])

    def test_tampered_task_is_detected(self) -> None:
        path = self.out / "tasks" / "postgres__aaaa11112222.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["goal"] = "tampered"
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        report = release.verify(self.out)
        self.assertFalse(report["ok"])
        self.assertEqual(report["mismatched"], ["tasks/postgres__aaaa11112222.json"])

    def test_missing_file_is_detected(self) -> None:
        (self.out / "split.json").unlink()
        report = release.verify(self.out)
        self.assertFalse(report["ok"])
        self.assertEqual(report["missing"], ["split.json"])

    def test_extra_file_is_detected(self) -> None:
        (self.out / "tasks" / "stray.json").write_text("{}", encoding="utf-8")
        report = release.verify(self.out)
        self.assertFalse(report["ok"])
        self.assertEqual(report["extra"], ["tasks/stray.json"])

    def test_public_leak_is_detected(self) -> None:
        path = self.out / "tasks" / "postgres__bbbb33334444.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["goal"] = "see diff --git a/x b/x"
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        report = release.verify(self.out)
        self.assertFalse(report["ok"])
        leaks = cast("list[str]", report["public_leaks"])
        self.assertIn("postgres__bbbb33334444.json", leaks)

    def test_missing_manifest_is_an_error(self) -> None:
        (self.out / "manifest.json").unlink()
        with self.assertRaises(PipelineError):
            release.verify(self.out)


if __name__ == "__main__":
    unittest.main()
