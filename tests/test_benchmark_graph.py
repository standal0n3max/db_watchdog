"""Индекс графа и три MCP-инструмента с отсечкой.

Проверяется, что инструменты возвращают только предковые ревизии, отказывают
на ``head_sha`` и потомках, а нулевой результат пишется в audit log. Индекс
строится в памяти SQLite, git и Docker не нужны.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import cast

from db_pipeline.benchmark.graph import cutoff as cutoff_module
from db_pipeline.benchmark.graph import store, tools


class GraphToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="graph-test-")
        self.db = Path(self.tmp.name) / "graph.sqlite"
        conn = store.open_store(self.db)
        store.add_commit(conn, "base", ["older"], "base commit", 1000)
        store.add_commit(conn, "older", [], "older commit", 900)
        store.add_commit(conn, "head", ["base"], "the fix commit", 1100)
        store.add_file(conn, "older", "src/backend/x.c", "blob1", "hash1", 100, "older")
        store.add_symbol(conn, "older", "src/backend/x.c", "do_thing", "function", 10)
        store.add_symbol(conn, "head", "src/backend/x.c", "do_fix", "function", 20)
        store.add_hunk(conn, "older", "src/backend/x.c", "added line", "removed line")
        conn.commit()
        conn.close()
        self.cutoff = cutoff_module.build_cutoff(
            "base", 1000, {"base", "older", "head"}, forbidden_head="head"
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _conn(self) -> sqlite3.Connection:
        return store.open_store(self.db)

    def _results(self, payload: dict[str, object]) -> list[dict[str, object]]:
        rows = payload["results"]
        if not isinstance(rows, list):
            return []
        items = cast("list[object]", rows)
        found: list[dict[str, object]] = []
        for row in items:
            if isinstance(row, dict):
                found.append(cast("dict[str, object]", row))
        return found

    def test_symbols_before_cutoff(self) -> None:
        conn = self._conn()
        try:
            found = tools.find_code_symbols(conn, self.cutoff, "do_")
            names = [str(row["name"]) for row in self._results(found)]
            self.assertIn("do_thing", names)
            self.assertNotIn("do_fix", names)
        finally:
            conn.close()

    def test_head_symbol_is_refused(self) -> None:
        conn = self._conn()
        try:
            found = tools.find_code_symbols(conn, self.cutoff, "do_fix")
            self.assertEqual(self._results(found), [])
        finally:
            conn.close()

    def test_read_versioned_source_ancestor(self) -> None:
        conn = self._conn()
        try:
            result = tools.read_versioned_source(conn, self.cutoff, "older", "src/backend/x.c")
            rows = self._results(result)
            self.assertEqual(str(rows[0]["sha256"]), "hash1")
        finally:
            conn.close()

    def test_read_versioned_source_head_refused(self) -> None:
        conn = self._conn()
        try:
            result = tools.read_versioned_source(conn, self.cutoff, "head", "src/backend/x.c")
            self.assertEqual(result["error"], "cutoff")
        finally:
            conn.close()

    def test_history_search_respects_cutoff(self) -> None:
        conn = self._conn()
        try:
            found = tools.search_git_history(conn, self.cutoff, "commit")
            revisions = {str(row["revision"]) for row in self._results(found)}
            self.assertNotIn("head", revisions)
        finally:
            conn.close()

    def test_empty_result_is_audited(self) -> None:
        conn = self._conn()
        try:
            tools.find_code_symbols(conn, self.cutoff, "no_such_symbol")
            rows = conn.execute("SELECT count(*) FROM audit WHERE empty_result = 1").fetchone()
            self.assertGreaterEqual(int(rows[0]), 1)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
