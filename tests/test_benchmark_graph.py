"""Единый git-native граф: сканер, отсечка, семь инструментов, инкремент.

Репозиторий синтетический, в ``tmp_path``-аналоге. Docker не нужен.
"""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import cast

from db_pipeline.benchmark.graph import coverage, gitgraph, view
from db_pipeline.benchmark.graph.symbols import parse_hunk_header, scan_symbols


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _init_repo(root: Path) -> tuple[Path, str, str, str]:
    repo = root / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "tester")
    source = repo / "src" / "backend"
    tests = repo / "src" / "test" / "regress" / "sql"
    source.mkdir(parents=True)
    tests.mkdir(parents=True)
    (source / "x.c").write_text(_source("return 1;"), encoding="utf-8")
    (tests / "t.sql").write_text("SELECT 1;\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "older commit introduces do_thing")
    older = _git(repo, "rev-parse", "HEAD")
    (source / "x.c").write_text(_source("return 2;"), encoding="utf-8")
    (tests / "t.sql").write_text("SELECT 2;\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "base commit keeps do_thing")
    base = _git(repo, "rev-parse", "HEAD")
    (source / "x.c").write_text(
        _source("return 3;") + "/* " + ("CANARY_" * 8) + " */\n",
        encoding="utf-8",
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "head the secret fix")
    head = _git(repo, "rev-parse", "HEAD")
    return repo, older, base, head


def _source(body: str) -> str:
    return (
        "static int do_thing(void);\n"
        "\n"
        "int\n"
        "do_thing(void)\n"
        "{\n"
        f"\t{body}\n"
        "}\n"
        "\n"
        "void\n"
        "report_missing(void)\n"
        "{\n"
        '\terrmsg("could not open file %s", path);\n'
        "}\n"
    )


class SymbolScannerTests(unittest.TestCase):
    def test_prototype_and_definition_are_distinguished(self) -> None:
        lines = _source("return 0;").splitlines()
        found = {item.name: item for item in scan_symbols("src/backend/x.c", lines)}
        self.assertTrue(found["do_thing"].definition)
        self.assertGreater(found["do_thing"].end_line, found["do_thing"].line)

    def test_hunk_header_is_parsed(self) -> None:
        self.assertEqual(parse_hunk_header("@@ -3310,4 +3311,6 @@ static Index"), (3310, 4, 3311, 6))
        self.assertEqual(parse_hunk_header("@@ -10 +20 @@"), (10, 1, 20, 1))
        self.assertEqual(parse_hunk_header("not a header"), (0, 0, 0, 0))


class GraphViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="gitgraph-")
        root = Path(self.tmp.name)
        self.repo, self.older, self.base, self.head = _init_repo(root)
        self.db = root / "graph.sqlite"
        gitgraph.build(db=self.db, repo=self.repo, bases=[self.base])
        self.view = view.open_view(self.db, self.base, self.head)

    def tearDown(self) -> None:
        view.close_view(self.view)
        self.tmp.cleanup()

    def _rows(self, payload: dict[str, object]) -> list[dict[str, object]]:
        rows = payload.get("results")
        if not isinstance(rows, list):
            return []
        items = cast("list[object]", rows)
        found: list[dict[str, object]] = []
        for item in items:
            if isinstance(item, dict):
                found.append(cast("dict[str, object]", item))
        return found

    def test_head_is_outside_visible_and_refused(self) -> None:
        self.assertNotIn(self.head, self.view.visible)
        self.assertIn(self.base, self.view.visible)
        self.assertIn(self.older, self.view.visible)
        refused = view.read_versioned_source(self.view, self.head, "src/backend/x.c")
        self.assertEqual(refused["error"], "outside_cutoff")
        self.assertEqual(refused["results"], [])

    def test_visible_matches_rev_list(self) -> None:
        count = int(_git(self.repo, "rev-list", "--count", self.base))
        self.assertEqual(len(self.view.visible), count)

    def test_symbols_and_source_are_at_base(self) -> None:
        found = view.find_code_symbols(self.view, "do_thing")
        rows = self._rows(found)
        self.assertGreater(len(rows), 0)
        self.assertTrue(all(self.base.startswith(str(row["revision"])) for row in rows))
        self.assertTrue(all("sha256" not in row for row in rows))
        self.assertNotIn("\n", view.compact_text(found))
        source = view.read_versioned_source(self.view, str(rows[0]["revision"]), "src/backend/x.c")
        text = str(self._rows(source)[0]["content"])
        self.assertIn("do_thing", text)
        self.assertNotIn("CANARY_", text)

    def test_seven_tools_stay_inside_cutoff(self) -> None:
        payloads = [
            view.find_code_symbols(self.view, "do_thing"),
            view.read_versioned_source(self.view, self.base, "src/backend/x.c", limit=80),
            view.search_git_history(self.view, "do_thing"),
            view.symbol_history(self.view, "src/backend/x.c", "do_thing"),
            view.tests_to_code(self.view, "src/test/regress/sql/t.sql"),
            view.similar_fixes(self.view, "do_thing"),
            view.find_error_origin(self.view, "could not open file"),
        ]
        for payload in payloads:
            rows = self._rows(payload)
            self.assertGreater(len(rows), 0, payload)
            self.assertIn("cutoff", payload)
            for row in rows:
                self.assertNotIn("cutoff", row)
                self.assertNotIn("sha256", row)
                revision = str(row.get("revision") or row.get("sample_revision") or "")
                if revision != "":
                    self.assertTrue(
                        any(full.startswith(revision) for full in self.view.visible)
                    )

    def test_cochange_weight_and_symbol_change(self) -> None:
        payload = view.tests_to_code(self.view, "src/test/regress/sql/t.sql")
        paths = {str(row["code_path"]) for row in self._rows(payload)}
        self.assertIn("src/backend/x.c", paths)
        history = view.symbol_history(self.view, "src/backend/x.c", "do_thing")
        self.assertGreater(len(self._rows(history)), 0)
        self.assertNotIn(self.head, {str(row["revision"]) for row in self._rows(history)})

    def test_short_revision_is_inside_cutoff(self) -> None:
        prefix = self.base[:12]
        self.assertTrue(coverage.revision_visible(self.view, prefix))
        payload: dict[str, object] = {}
        payload["results"] = [{"revision": prefix}]
        self.assertEqual(
            coverage._expect(self.view, "read_versioned_source", payload),  # pyright: ignore[reportPrivateUsage]
            [],
        )

    def test_unknown_sha_is_outside_cutoff(self) -> None:
        unknown = "zzzzzzzzzzzz"
        self.assertFalse(coverage.revision_visible(self.view, unknown))
        payload: dict[str, object] = {}
        payload["results"] = [{"revision": unknown}]
        problems = coverage._expect(self.view, "search_git_history", payload)  # pyright: ignore[reportPrivateUsage]
        self.assertIn("search_git_history: revision вне visible", problems)

    def test_head_is_outside_cutoff(self) -> None:
        self.assertFalse(coverage.revision_visible(self.view, self.head))
        self.assertFalse(coverage.revision_visible(self.view, self.head[:12]))
        payload: dict[str, object] = {}
        payload["results"] = [{"revision": self.head}]
        problems = coverage._expect(self.view, "symbol_history", payload)  # pyright: ignore[reportPrivateUsage]
        self.assertIn("symbol_history: revision вне visible", problems)

    def test_blank_query_is_empty(self) -> None:
        payload = view.search_git_history(self.view, "   ")
        self.assertEqual(payload["reason"], "not_found")

    def test_double_build_hashes_match_and_incremental_adds_commit(self) -> None:
        first = gitgraph.table_hashes(self.db)
        other = Path(self.tmp.name) / "other.sqlite"
        gitgraph.build(db=other, repo=self.repo, bases=[self.base])
        self.assertEqual(first, gitgraph.table_hashes(other))
        view.close_view(self.view)
        gitgraph.build(db=self.db, repo=self.repo, bases=[self.base, self.head])
        self.view = view.open_view(self.db, self.base, self.head)
        self.assertNotIn(self.head, self.view.visible)
        import sqlite3

        conn = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)
        try:
            counted = int(conn.execute("SELECT count(*) FROM commits").fetchone()[0])
        finally:
            conn.close()
        self.assertEqual(counted, 3)


if __name__ == "__main__":
    unittest.main()
