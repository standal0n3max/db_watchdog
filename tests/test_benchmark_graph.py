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
from typing import Sequence, cast

from db_pipeline.benchmark.graph import cutoff as cutoff_module
from db_pipeline.benchmark.graph import index as index_module
from db_pipeline.benchmark.graph import store, tools


class GraphToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="graph-test-")
        self.db = Path(self.tmp.name) / "graph.sqlite"
        conn = store.open_store(self.db)
        store.add_commit(conn, "base", ["older"], "base commit", 1000)
        store.add_commit(conn, "older", [], "older commit", 900)
        store.add_commit(conn, "head", ["base"], "the fix commit", 1100)
        content = b"int do_thing(void) { return 1; }\n"
        store.add_revision(conn, "older", "src/backend/x.c", "hash1")
        store.add_blob(conn, "hash1", content)
        store.add_symbol(conn, "src/backend/x.c", "hash1", "do_thing", "function", 10, "older")
        store.add_symbol(conn, "src/backend/x.c", "hash2", "do_fix", "function", 20, "head")
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
            self.assertIn("do_thing", str(rows[0]["content"]))
            span = cast("list[object]", rows[0]["byte_span"])
            self.assertEqual(span, [0, len(b"int do_thing(void) { return 1; }\n")])
        finally:
            conn.close()

    def test_read_versioned_source_span_is_bounded(self) -> None:
        conn = self._conn()
        try:
            result = tools.read_versioned_source(
                conn, self.cutoff, "older", "src/backend/x.c", offset=4, limit=9
            )
            rows = self._results(result)
            self.assertEqual(str(rows[0]["content"]), "do_thing(")
            self.assertEqual(cast("list[object]", rows[0]["byte_span"]), [4, 13])
        finally:
            conn.close()

    def test_read_versioned_source_without_bytes_is_missing(self) -> None:
        conn = self._conn()
        try:
            result = tools.read_versioned_source(conn, self.cutoff, "base", "src/backend/missing.c")
            self.assertEqual(result["error"], "not_found")
        finally:
            conn.close()

    def test_read_versioned_source_resolves_from_ancestor(self) -> None:
        conn = self._conn()
        try:
            result = tools.read_versioned_source(conn, self.cutoff, "base", "src/backend/x.c")
            rows = self._results(result)
            self.assertEqual(str(rows[0]["sha256"]), "hash1")
            self.assertIn("do_thing", str(rows[0]["content"]))
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


def _symbol_lines(lines: Sequence[str]) -> list[dict[str, object]]:
    """Прогоняет сканер по исходнику и возвращает словари символов."""
    found = index_module.scan_symbols("src/backend/x.c", list(lines))
    return [
        {
            "name": symbol.name,
            "kind": symbol.kind,
            "line": symbol.line,
            "end_line": symbol.end_line,
            "definition": symbol.definition,
        }
        for symbol in found
    ]


class SymbolScannerTests(unittest.TestCase):
    """Дефект 1 и 4: определения против прототипов и не-function символы."""

    def test_prototype_and_definition_are_distinguished(self) -> None:
        source = (
            "static Index get_expression_sortgroupref(PlannerInfo *root, Expr *expr);\n"
            "\n"
            "static Index\n"
            "get_expression_sortgroupref(PlannerInfo *root, Expr *expr)\n"
            "{\n"
            "\treturn 0;\n"
            "}\n"
        )
        found = _symbol_lines(source.splitlines())
        matches = [row for row in found if row["name"] == "get_expression_sortgroupref"]
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["line"], 4)
        self.assertEqual(matches[0]["end_line"], 7)
        self.assertTrue(matches[0]["definition"])

    def test_late_definition_is_indexed(self) -> None:
        lines = [f"/* filler line {n} */" for n in range(3000)]
        lines += [
            "static Index",
            "get_expression_sortgroupref(PlannerInfo *root, Expr *expr)",
            "{",
            "\treturn 0;",
            "}",
        ]
        found = _symbol_lines(lines)
        names = {row["name"]: row for row in found}
        self.assertIn("get_expression_sortgroupref", names)
        self.assertEqual(names["get_expression_sortgroupref"]["line"], 3002)

    def test_struct_enum_typedef_and_macro_are_indexed(self) -> None:
        source = (
            "typedef struct JsonConstructorExpr\n"
            "{\n"
            "\tNodeTag type;\n"
            "} JsonConstructorExpr;\n"
            "\n"
            "typedef enum OverridingKind\n"
            "{\n"
            "\tOVERRIDING_NOT_SET,\n"
            "} OverridingKind;\n"
            "\n"
            "typedef struct Alias AliasPtr;\n"
            "\n"
            "struct RangeVar\n"
            "{\n"
            "\tchar *catalogname;\n"
            "};\n"
            "\n"
            "#define PG_MAX_CASEMAP_MBCHARS \\\n"
            "\tUNICODE_MAX_CASEMAP_CODEPOINTS\n"
        )
        found = {row["name"]: row for row in _symbol_lines(source.splitlines())}
        self.assertEqual(found["JsonConstructorExpr"]["kind"], "struct")
        self.assertEqual(found["JsonConstructorExpr"]["line"], 1)
        self.assertEqual(found["JsonConstructorExpr"]["end_line"], 4)
        self.assertEqual(found["OverridingKind"]["kind"], "enum")
        self.assertEqual(found["AliasPtr"]["kind"], "typedef")
        self.assertEqual(found["RangeVar"]["kind"], "struct")
        self.assertEqual(found["PG_MAX_CASEMAP_MBCHARS"]["kind"], "macro")
        self.assertEqual(found["PG_MAX_CASEMAP_MBCHARS"]["end_line"], 19)

    def test_multi_line_macro_body(self) -> None:
        source = "#define FOO \\\n\tbar \\\n\tbaz\n"
        found = {row["name"]: row for row in _symbol_lines(source.splitlines())}
        self.assertEqual(found["FOO"]["end_line"], 3)


class SymbolToolTests(unittest.TestCase):
    """Дефект 2: определение предпочтительнее прототипа, диапазон строк, точный поиск."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="graph-symbol-")
        self.db = Path(self.tmp.name) / "graph.sqlite"
        conn = store.open_store(self.db)
        store.add_commit(conn, "base", ["older"], "base", 1000)
        store.add_commit(conn, "older", [], "older", 900)
        store.add_symbol(
            conn, "src/backend/x.c", "h1", "do_thing", "function", 10, "older",
            end_line=10, body="static int do_thing(void);",
        )
        store.add_symbol(
            conn, "src/backend/x.c", "h2", "do_thing", "function", 40, "base",
            end_line=50, body="int do_thing(void) { return 1; }",
        )
        store.add_symbol(
            conn, "src/include/nodes/primnodes.h", "h3", "JsonConstructorExpr", "struct",
            1730, "older", end_line=1743, body="typedef struct JsonConstructorExpr { }",
        )
        conn.commit()
        conn.close()
        self.cutoff = cutoff_module.build_cutoff("base", 1000, {"base", "older"})

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _conn(self) -> sqlite3.Connection:
        return store.open_store(self.db)

    def test_prefers_definition_over_prototype(self) -> None:
        conn = self._conn()
        try:
            payload = tools.find_code_symbols(conn, self.cutoff, "do_thing")
            rows = cast("list[dict[str, object]]", payload["results"])
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["line"], 40)
            self.assertEqual(rows[0]["end_line"], 50)
        finally:
            conn.close()

    def test_substring_search_finds_type(self) -> None:
        conn = self._conn()
        try:
            payload = tools.find_code_symbols(conn, self.cutoff, "Json")
            rows = cast("list[dict[str, object]]", payload["results"])
            names = {str(row["name"]) for row in rows}
            self.assertIn("JsonConstructorExpr", names)
            kinds = {str(row["kind"]) for row in rows}
            self.assertIn("struct", kinds)
        finally:
            conn.close()

    def test_exact_match_has_priority(self) -> None:
        conn = self._conn()
        try:
            payload = tools.find_code_symbols(conn, self.cutoff, "do_thing")
            rows = cast("list[dict[str, object]]", payload["results"])
            self.assertEqual(str(rows[0]["name"]), "do_thing")
        finally:
            conn.close()


class ReadByLinesTests(unittest.TestCase):
    """Дефект 3: чтение исходника по номерам строк."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="graph-lines-")
        self.db = Path(self.tmp.name) / "graph.sqlite"
        conn = store.open_store(self.db)
        store.add_commit(conn, "base", [], "base", 1000)
        self.text = "alpha\nbeta\ngamma\ndelta\n"
        store.add_revision(conn, "base", "src/backend/x.c", "h1")
        store.add_blob(conn, "h1", self.text.encode())
        conn.commit()
        conn.close()
        self.cutoff = cutoff_module.build_cutoff("base", 1000, {"base"})

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _conn(self) -> sqlite3.Connection:
        return store.open_store(self.db)

    def test_read_by_line_range(self) -> None:
        conn = self._conn()
        try:
            payload = tools.read_versioned_source(
                conn, self.cutoff, "base", "src/backend/x.c", start_line=2, end_line=3
            )
            rows = cast("list[dict[str, object]]", payload["results"])
            self.assertEqual(str(rows[0]["content"]), "beta\ngamma\n")
            self.assertEqual(rows[0]["start_line"], 2)
            self.assertEqual(rows[0]["end_line"], 3)
        finally:
            conn.close()

    def test_byte_offset_still_supported(self) -> None:
        conn = self._conn()
        try:
            payload = tools.read_versioned_source(
                conn, self.cutoff, "base", "src/backend/x.c", offset=0, limit=5
            )
            rows = cast("list[dict[str, object]]", payload["results"])
            self.assertEqual(str(rows[0]["content"]), "alpha")
        finally:
            conn.close()


class FtsSearchTests(unittest.TestCase):
    """Дефект 5: терпимый FTS по естественному языку и телам кода."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="graph-fts-")
        self.db = Path(self.tmp.name) / "graph.sqlite"
        conn = store.open_store(self.db)
        store.add_commit(conn, "base", [], "base commit", 1000)
        store.add_commit(
            conn, "older", [], "Fix eager aggregation grouping cross-type handling", 900
        )
        store.add_symbol(
            conn, "src/backend/optimizer/util/relnode.c", "h1",
            "get_expression_sortgroupref", "function", 3311, "older",
            end_line=3350, body="int get_expression_sortgroupref(void) { return 0; }",
        )
        conn.commit()
        conn.close()
        self.cutoff = cutoff_module.build_cutoff("base", 1000, {"base", "older"})

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _conn(self) -> sqlite3.Connection:
        return store.open_store(self.db)

    def test_natural_language_query_returns_rows(self) -> None:
        conn = self._conn()
        try:
            payload = tools.search_git_history(
                conn, self.cutoff, "eager aggregation grouping cross-type"
            )
            rows = cast("list[dict[str, object]]", payload["results"])
            self.assertGreater(len(rows), 0)
        finally:
            conn.close()

    def test_partial_token_query_returns_rows(self) -> None:
        conn = self._conn()
        try:
            payload = tools.search_git_history(conn, self.cutoff, "sortgroupref")
            rows = cast("list[dict[str, object]]", payload["results"])
            self.assertGreater(len(rows), 0)
            kinds = {str(row["kind"]) for row in rows}
            self.assertIn("code", kinds)
        finally:
            conn.close()

    def test_query_without_tokens_returns_nothing(self) -> None:
        conn = self._conn()
        try:
            payload = tools.search_git_history(conn, self.cutoff, "   ")
            self.assertEqual(payload["results"], [])
        finally:
            conn.close()


class HunkRangeTests(unittest.TestCase):
    """Дефект 6: реальные hunks с диапазонами строк."""

    def test_hunk_header_is_parsed(self) -> None:
        parsed = index_module.parse_hunk_header("@@ -3310,4 +3311,6 @@ static Index")
        self.assertEqual(parsed, (3310, 4, 3311, 6))

    def test_hunk_header_without_counts(self) -> None:
        parsed = index_module.parse_hunk_header("@@ -10 +20 @@")
        self.assertEqual(parsed, (10, 1, 20, 1))

    def test_hunk_header_invalid(self) -> None:
        parsed = index_module.parse_hunk_header("not a header")
        self.assertEqual(parsed, (0, 0, 0, 0))


if __name__ == "__main__":
    unittest.main()
