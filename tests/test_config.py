from __future__ import annotations

import unittest

from db_pipeline.airflow_local import WEB_PORT, local_airflow_env
from db_pipeline.config import enabled_cores, load_cores


class ConfigTests(unittest.TestCase):
    def test_only_duckdb_is_enabled(self) -> None:
        cores = load_cores()
        ids = [core.id for core in cores]
        self.assertEqual(ids, ["duckdb", "postgres", "sqlite"])
        enabled = [core.id for core in enabled_cores(cores)]
        self.assertEqual(enabled, ["duckdb", "postgres", "sqlite"])
        by_id = {core.id: core for core in cores}
        self.assertEqual(by_id["duckdb"].license, "MIT")
        self.assertIn("CMAKE_BUILD_PARALLEL_LEVEL=1", by_id["duckdb"].build_command)
        self.assertEqual(by_id["postgres"].oracle, "pg_regress")
        self.assertTrue(by_id["postgres"].enabled)
        self.assertEqual(
            by_id["postgres"].clone_url,
            "https://git.postgresql.org/git/postgresql.git",
        )
        self.assertEqual(by_id["postgres"].sample_candidates[0], "pg_int4_integer_char_overflow")
        self.assertEqual(by_id["sqlite"].oracle, "testfixture")
        self.assertTrue(by_id["sqlite"].enabled)
        self.assertTrue(by_id["sqlite"].pinned_sha.startswith("bfb858c596f6"))
        self.assertEqual(by_id["sqlite"].test_suffixes, [".test"])
        self.assertEqual(by_id["sqlite"].code_exclude, ["sqlite3.c", "sqlite3.h"])

    def test_airflow_uses_sqlite_and_free_port(self) -> None:
        env = local_airflow_env()
        self.assertEqual(env["AIRFLOW__CORE__EXECUTOR"], "SequentialExecutor")
        self.assertEqual(env["AIRFLOW__WEBSERVER__WEB_SERVER_PORT"], WEB_PORT)
        self.assertEqual(WEB_PORT, "8088")
        self.assertTrue(env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].startswith("sqlite:///"))
        self.assertNotIn(":5432", env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"])
        self.assertNotIn("8123", env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"])


if __name__ == "__main__":
    unittest.main()
