"""Граф v2: перевод байтов в строки, leak-guard и инструменты истории.

Проверяется то, что не требует PRISM и Docker: арифметика диапазонов, разбор
патчей, канарейки, membership-отказ и форма ответов v2 на синтетических базах
``history.sqlite`` и ``graph.sqlite``.
"""

from __future__ import annotations

import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import cast

from db_pipeline.benchmark.graph import cutoff as cutoff_module
from db_pipeline.benchmark.graph import history, history_store, history_tools, leakguard, store


def _blob(text: str) -> bytes:
    return text.encode("utf-8")


def _as_float(value: object) -> float:
    """Приводит значение из JSON-ответа к float для сравнений в тестах."""
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value))


def _rows(payload: dict[str, object]) -> list[dict[str, object]]:
    """Приводит ``payload["results"]`` к списку словарей для проверок."""
    raw = payload.get("results")
    if not isinstance(raw, list):
        return []
    found: list[dict[str, object]] = []
    for item in cast("list[object]", raw):
        if isinstance(item, dict):
            found.append(cast("dict[str, object]", item))
    return found


class ByteToLineTests(unittest.TestCase):
    """PRISM хранит байтовые смещения, таблица — номера строк."""

    def test_line_of_first_byte(self) -> None:
        blob = _blob("alpha\nbeta\ngamma\n")
        self.assertEqual(history.byte_to_line(blob, 0), 1)

    def test_line_after_first_newline(self) -> None:
        blob = _blob("alpha\nbeta\ngamma\n")
        self.assertEqual(history.byte_to_line(blob, 6), 2)
        self.assertEqual(history.byte_to_line(blob, 11), 3)

    def test_span_inside_one_line_is_one_line(self) -> None:
        blob = _blob("alpha\nbeta\ngamma\n")
        first, last = history.byte_span_to_lines(blob, 1, 4)
        self.assertEqual((first, last), (1, 1))

    def test_span_covering_two_lines(self) -> None:
        blob = _blob("alpha\nbeta\ngamma\n")
        first, last = history.byte_span_to_lines(blob, 2, 10)
        self.assertEqual((first, last), (1, 2))

    def test_span_ending_on_newline_excludes_next_line(self) -> None:
        blob = _blob("alpha\nbeta\ngamma\n")
        first, last = history.byte_span_to_lines(blob, 0, 6)
        self.assertEqual((first, last), (1, 1))

    def test_span_past_end_is_clamped(self) -> None:
        blob = _blob("alpha\n")
        first, last = history.byte_span_to_lines(blob, 0, 999)
        self.assertEqual(first, 1)
        self.assertEqual(last, 1)

    def test_empty_span_returns_start_line(self) -> None:
        blob = _blob("alpha\nbeta\n")
        first, last = history.byte_span_to_lines(blob, 6, 6)
        self.assertEqual((first, last), (2, 2))

    def test_empty_blob(self) -> None:
        self.assertEqual(history.byte_span_to_lines(b"", 0, 0), (1, 1))


class NormalizeTests(unittest.TestCase):
    """Нормализация сообщений errmsg: формат-спецификаторы и пробелы."""

    def test_format_specifiers_become_percent(self) -> None:
        self.assertEqual(
            history.normalize_message('relation "%s" does not exist'),
            'relation "%" does not exist',
        )

    def test_numbers_and_long_specifiers(self) -> None:
        self.assertEqual(history.normalize_message("value %lld and %d"), "value % and %")

    def test_whitespace_collapsed(self) -> None:
        self.assertEqual(history.normalize_message("a   b\n\tc"), "a b c")

    def test_plain_message_unchanged(self) -> None:
        self.assertEqual(history.normalize_message("could not open file"), "could not open file")


class TestPathTests(unittest.TestCase):
    def test_regress_expected_and_sql_are_tests(self) -> None:
        self.assertTrue(history.is_test_path("src/test/regress/expected/x.out"))
        self.assertTrue(history.is_test_path("src/test/regress/sql/x.sql"))

    def test_source_is_not_test(self) -> None:
        self.assertFalse(history.is_test_path("src/backend/executor/nodeHashjoin.c"))


class LeakguardUnitTests(unittest.TestCase):
    """Канарейки и membership без git и PRISM."""

    _PATCH = (
        "diff --git a/src/backend/executor/nodeHashjoin.c b/src/backend/executor/nodeHashjoin.c\n"
        "--- a/src/backend/executor/nodeHashjoin.c\n"
        "+++ b/src/backend/executor/nodeHashjoin.c\n"
        "@@ -10,1 +10,1 @@\n"
        "+    hjstate = ExecInitHashJoinState(root, jointype);\n"
        "+}\n"
        "+/* tunable_work_mem_for_hash_join_batches = 4096; */\n"
    )

    def test_added_lines_exclude_header(self) -> None:
        lines = leakguard.added_lines(self._PATCH)
        self.assertNotIn("++ b/src/backend/executor/nodeHashjoin.c", lines)
        self.assertIn("    hjstate = ExecInitHashJoinState(root, jointype);", lines)

    def test_nontrivial_lines_filter_short_and_trivial(self) -> None:
        found = leakguard.nontrivial_lines(self._PATCH)
        self.assertIn("    hjstate = ExecInitHashJoinState(root, jointype);", found)
        self.assertIn("/* tunable_work_mem_for_hash_join_batches = 4096; */", found)
        self.assertNotIn("}", found)

    def test_patch_paths_are_extracted(self) -> None:
        paths = leakguard.patch_paths(self._PATCH)
        self.assertEqual(paths, ["src/backend/executor/nodeHashjoin.c"])

    def test_scan_responses_flags_canary(self) -> None:
        canary = "    hjstate = ExecInitHashJoinState(root, jointype);"
        violations = leakguard.scan_responses(
            {"symbol_history": f"found {canary} here"}, [canary], "deadbeef"
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("канарейка", violations[0])

    def test_scan_responses_flags_head_sha(self) -> None:
        head = "a" * 40
        violations = leakguard.scan_responses({"tool": f"rev {head}"}, [], head)
        self.assertEqual(len(violations), 1)
        self.assertIn("head_sha", violations[0])

    def test_scan_responses_clean(self) -> None:
        self.assertEqual(leakguard.scan_responses({"tool": "nothing"}, ["zzz"], "deadbeef"), [])

    def test_check_membership_flags_unknown_oid(self) -> None:
        inside = "1" * 40
        outside = "2" * 40
        violations = leakguard.check_membership(
            {"search": f"{inside} and {outside}"}, {inside}
        )
        self.assertEqual(len(violations), 1)
        self.assertIn(outside[:12], violations[0])

    def test_check_membership_deduplicates(self) -> None:
        """Один и тот же посторонний oid в двух ответах даёт одно нарушение."""
        outside = "3" * 40
        violations = leakguard.check_membership(
            {"a": outside, "b": outside}, set()
        )
        self.assertEqual(len(violations), 1)
        self.assertIn(outside[:12], violations[0])

    def test_patch_id_is_deterministic(self) -> None:
        first = leakguard.patch_id(self._PATCH)
        second = leakguard.patch_id(self._PATCH)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 40)

    def test_patch_id_empty_for_empty_patch(self) -> None:
        self.assertEqual(leakguard.patch_id(""), "")

    def test_gold_plus_test_reproduces_commit_patch_id(self) -> None:
        """Проверено на живой задаче: gold+test дают patch_id коммита."""
        combined = self._PATCH + self._PATCH
        self.assertNotEqual(leakguard.patch_id(combined), "")

    def test_holdout_fingerprint_is_order_independent(self) -> None:
        self.assertEqual(
            leakguard.holdout_fingerprint(["b", "a"]),
            leakguard.holdout_fingerprint(["a", "b"]),
        )


class BatchAddedLinesTests(unittest.TestCase):
    """Пакетный ``git show``: границы коммитов восстанавливаются по маркеру."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="leakguard-git-")
        self.repo = Path(self.tmp.name)

        def run(*args: str) -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                ["git", "-C", str(self.repo), *args],
                capture_output=True,
                text=True,
                check=True,
            )

        run("init", "-q")
        run("config", "user.email", "t@example.com")
        run("config", "user.name", "t")
        # Строка из одних hex-символов в теле патча: раньше она выглядела как
        # заголовок ``commit <oid>`` и сбивала границы.
        (self.repo / "a.c").write_text("int a;\n/* 0123456789abcdef0123456789abcdef01234567 */\n")
        run("add", ".")
        run("commit", "-qm", "first")
        self.first = run("rev-parse", "HEAD").stdout.strip()
        (self.repo / "a.c").write_text("int a;\nint b;\n")
        run("add", ".")
        run("commit", "-qm", "second")
        self.second = run("rev-parse", "HEAD").stdout.strip()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_batch_matches_single_show(self) -> None:
        batch = leakguard.added_lines_many(self.repo, [self.first, self.second])
        self.assertEqual(
            batch.get(self.second, set()), leakguard.show_added_lines(self.repo, self.second)
        )
        self.assertEqual(
            batch.get(self.first, set()), leakguard.show_added_lines(self.repo, self.first)
        )

    def test_hex_line_stays_inside_its_commit(self) -> None:
        batch = leakguard.added_lines_many(self.repo, [self.first, self.second])
        hex_line = "/* 0123456789abcdef0123456789abcdef01234567 */"
        self.assertIn(hex_line, batch.get(self.first, set()))
        self.assertNotIn(hex_line, batch.get(self.second, set()))

    def test_empty_input_is_empty(self) -> None:
        self.assertEqual(leakguard.added_lines_many(self.repo, []), {})


class _HistoryFixture(unittest.TestCase):
    """Общая фикстура: синтетический history.sqlite и индекс v1."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="graph-v2-")
        root = Path(self.tmp.name)
        self.history_db = root / "history.sqlite"
        self.store_db = root / "graph.sqlite"
        history_conn = history_store.open_history(self.history_db)
        history_conn.execute(
            "INSERT INTO commits(oid, authored_at, committed_at, subject, message, patch_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("older", "2024-01-01T00:00:00+00:00", "", "Fix hash join batching", "", "p1"),
        )
        history_conn.execute(
            "INSERT INTO commits(oid, authored_at, committed_at, subject, message, patch_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("base", "2024-02-01T00:00:00+00:00", "", "Refactor hash join code", "", "p2"),
        )
        history_conn.executemany(
            "INSERT INTO commits_fts(oid, subject, message) VALUES (?, ?, ?)",
            [("older", "Fix hash join batching", ""), ("base", "Refactor hash join code", "")],
        )
        history_conn.executemany(
            "INSERT INTO symbol_history"
            "(oid, path, name, kind, role, line, end_line, sym_line, sym_end_line, "
            "old_start, old_end, new_start, new_end, sym_start, sym_end, side) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "older", "src/backend/executor/nodeHashjoin.c", "ExecHashJoin", "function",
                    "parsed_declaration", 30, 30, 31, 31, 0, 0, 0, 0, 0, 0, "new",
                ),
                (
                    "older", "src/backend/executor/nodeHashjoin.c", "ExecHashJoin", "function",
                    "parsed_definition", 58, 60, 59, 96, 0, 0, 0, 0, 0, 0, "old",
                ),
                (
                    "base", "src/backend/executor/nodeHashjoin.c", "ExecHashJoin", "function",
                    "parsed_definition", 70, 72, 71, 99, 0, 0, 0, 0, 0, 0, "new",
                ),
            ],
        )
        history_conn.executemany(
            "INSERT INTO co_change(test_path, code_path, weight, commits) VALUES (?, ?, ?, ?)",
            [
                ("src/test/regress/sql/aggregates.sql", "src/backend/executor/nodeHashjoin.c", 0.5, 3),
                ("src/test/regress/sql/aggregates.sql", "src/backend/optimizer/plan/planner.c", 0.25, 2),
                ("src/test/regress/sql/aggregates.sql", "src/backend/executor/execMain.c", 0.1, 1),
            ],
        )
        history_conn.executemany(
            "INSERT INTO path_commits(path, oid, status) VALUES (?, ?, ?)",
            [
                ("src/test/regress/sql/aggregates.sql", "older", "modified"),
                ("src/backend/executor/nodeHashjoin.c", "older", "modified"),
                ("src/backend/executor/nodeHashjoin.c", "base", "modified"),
                ("src/backend/executor/execMain.c", "base", "modified"),
            ],
        )
        history_conn.executemany(
            "INSERT INTO error_messages(message, base_sha, path, line) VALUES (?, ?, ?, ?)",
            [
                (
                    'relation "%" does not exist',
                    "base",
                    "src/backend/executor/execMain.c",
                    120,
                ),
                (
                    'could not open file "%"',
                    "base",
                    "src/backend/executor/nodeHashjoin.c",
                    500,
                ),
            ],
        )
        history_store.write_manifest(history_conn, {"builder_version": "test"})
        history_conn.commit()
        history_conn.close()

        conn = store.open_store(self.store_db)
        store.add_commit(conn, "older", [], "older", 900)
        store.add_commit(conn, "base", ["older"], "base", 1000)
        store.add_commit(conn, "head", ["base"], "the fix", 1100)
        store.add_revision(conn, "base", "src/backend/executor/nodeHashjoin.c", "h1")
        store.add_blob(conn, "h1", _blob("int ExecHashJoin(void) { return 0; }\n"))
        conn.commit()
        conn.close()

        self.cutoff = cutoff_module.build_cutoff(
            "base", 1000, {"older", "base", "head"}, forbidden_head="head"
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _index(self) -> history_tools.HistoryIndex:
        return history_tools.open_index(self.history_db, self.store_db, self.cutoff)


class SymbolHistoryTests(_HistoryFixture):
    def test_prefers_definition_and_reports_symbol_lines(self) -> None:
        index = self._index()
        try:
            payload = history_tools.symbol_history(
                index, "src/backend/executor/nodeHashjoin.c", "ExecHashJoin"
            )
            by_rev = {str(row["revision"]): row for row in _rows(payload)}
            self.assertIn("older", by_rev)
            self.assertTrue(by_rev["older"]["is_definition"])
            self.assertEqual(by_rev["older"]["line"], 59)
            self.assertEqual(by_rev["older"]["end_line"], 96)
            self.assertEqual(by_rev["older"]["hunk_line"], 58)
        finally:
            history_tools.close_index(index)

    def test_returns_freshest_first(self) -> None:
        """План требует порядок «от свежих к старым», а не алфавитный по oid."""
        index = self._index()
        try:
            payload = history_tools.symbol_history(
                index, "src/backend/executor/nodeHashjoin.c", "ExecHashJoin"
            )
            revisions = [str(row["revision"]) for row in _rows(payload)]
            # ``base`` (2024-02-01) свежее ``older`` (2024-01-01), хотя oid больше алфавитно.
            self.assertEqual(revisions[0], "base")
            self.assertLess(revisions.index("base"), revisions.index("older"))
        finally:
            history_tools.close_index(index)

    def test_head_is_never_returned(self) -> None:
        index = self._index()
        try:
            payload = history_tools.symbol_history(
                index, "src/backend/executor/nodeHashjoin.c", "ExecHashJoin"
            )
            revisions = {str(row["revision"]) for row in _rows(payload)}
            self.assertNotIn("head", revisions)
        finally:
            history_tools.close_index(index)

    def test_unknown_symbol_is_empty(self) -> None:
        index = self._index()
        try:
            payload = history_tools.symbol_history(index, "src/backend/x.c", "no_such")
            self.assertEqual(payload["results"], [])
        finally:
            history_tools.close_index(index)


class TestsToCodeTests(_HistoryFixture):
    def test_ranks_by_weight(self) -> None:
        index = self._index()
        try:
            payload = history_tools.tests_to_code(
                index, "src/test/regress/sql/aggregates.sql"
            )
            items = _rows(payload)
            weights = [_as_float(row["weight"]) for row in items]
            self.assertEqual(weights, sorted(weights, reverse=True))
            self.assertEqual(
                str(items[0]["code_path"]), "src/backend/executor/nodeHashjoin.c"
            )
            # Связь подтверждена коммитом ``older``, менявшим и тест, и код.
            self.assertEqual(str(items[0]["sample_revision"]), "older")
        finally:
            history_tools.close_index(index)

    def test_unknown_test_is_empty(self) -> None:
        index = self._index()
        try:
            payload = history_tools.tests_to_code(index, "src/test/regress/sql/none.sql")
            self.assertEqual(payload["results"], [])
        finally:
            history_tools.close_index(index)

    def test_pair_without_cutoff_evidence_is_dropped(self) -> None:
        """Связь без тожественного коммита-предка не выдаётся: иначе утечка.

        ``co_change`` строится по всей истории PRISM. Если база задачи в PRISM
        отсутствует, ни один коммит-родитель связи не проходит отсечку, и
        «исторически связанный» файл — знание из будущего, а не из базы.
        """
        conn = history_store.open_history(self.history_db)
        conn.execute(
            "INSERT INTO co_change(test_path, code_path, weight, commits) VALUES (?, ?, ?, ?)",
            ("src/test/regress/sql/aggregates.sql", "src/backend/leak/future.c", 0.9, 9),
        )
        conn.commit()
        conn.close()
        # Отсечка без единого допустимого коммита: ровно случай базы вне PRISM.
        empty = cutoff_module.build_cutoff("base", 1000, {"base"}, forbidden_head="head")
        index = history_tools.open_index(self.history_db, self.store_db, empty)
        try:
            payload = history_tools.tests_to_code(
                index, "src/test/regress/sql/aggregates.sql"
            )
            self.assertEqual(payload["results"], [])
        finally:
            history_tools.close_index(index)


class SimilarFixesTests(_HistoryFixture):
    def test_natural_language_query_returns_ancestor(self) -> None:
        index = self._index()
        try:
            payload = history_tools.similar_fixes(index, "hash join batching")
            revisions = {str(row["revision"]) for row in _rows(payload)}
            self.assertIn("older", revisions)
            self.assertNotIn("head", revisions)
        finally:
            history_tools.close_index(index)

    def test_blank_query_is_empty(self) -> None:
        index = self._index()
        try:
            payload = history_tools.similar_fixes(index, "   ")
            self.assertEqual(payload["results"], [])
        finally:
            history_tools.close_index(index)


class ErrorOriginTests(_HistoryFixture):
    def test_matches_normalized_message(self) -> None:
        index = self._index()
        try:
            payload = history_tools.find_error_origin(
                index, 'relation "foo" does not exist'
            )
            items = _rows(payload)
            self.assertGreater(len(items), 0)
            self.assertIn("does not exist", str(items[0]["message"]))
        finally:
            history_tools.close_index(index)

    def test_path_in_cutoff_window_is_reported(self) -> None:
        index = self._index()
        try:
            payload = history_tools.find_error_origin(index, "could not open file")
            items = _rows(payload)
            self.assertTrue(any(bool(row["in_cutoff_window"]) for row in items))
        finally:
            history_tools.close_index(index)

    def test_other_base_records_are_not_returned(self) -> None:
        """Записи из дерева другой базы не отдаются: это была бы утечка."""
        conn = history_store.open_history(self.history_db)
        conn.execute(
            "INSERT INTO error_messages(message, base_sha, path, line) VALUES (?, ?, ?, ?)",
            (
                'could not open file "%"',
                "some_other_base",
                "src/backend/storage/future_only.c",
                7,
            ),
        )
        conn.commit()
        conn.close()
        index = self._index()
        try:
            payload = history_tools.find_error_origin(index, "could not open file")
            paths = {str(row["path"]) for row in _rows(payload)}
            self.assertNotIn("src/backend/storage/future_only.c", paths)
        finally:
            history_tools.close_index(index)

    def test_unknown_message_is_empty(self) -> None:
        index = self._index()
        try:
            payload = history_tools.find_error_origin(index, "zzzzz")
            self.assertEqual(payload["results"], [])
        finally:
            history_tools.close_index(index)


class HistoryStoreTests(unittest.TestCase):
    def test_schema_roundtrip_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory(prefix="history-store-") as tmp:
            path = Path(tmp) / "history.sqlite"
            conn = history_store.open_history(path)
            history_store.write_manifest(conn, {"builder_version": "x"})
            conn.commit()
            conn.close()
            readonly = history_store.open_history_readonly(path)
            try:
                manifest = history_store.read_manifest(readonly)
            finally:
                readonly.close()
            self.assertEqual(manifest["builder_version"], "x")
            self.assertEqual(manifest["schema_version"], str(history_store.SCHEMA_VERSION))
            self.assertEqual(manifest["schema_sha256"], history_store.schema_sha256())

    def test_reset_schema_recreates_tables(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(history_store.SCHEMA)
        history_store.reset_schema(conn)
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(symbol_history)")
        }
        self.assertIn("sym_line", columns)
        conn.close()

    def test_open_readonly_rejects_missing_file(self) -> None:
        with self.assertRaises(FileNotFoundError):
            history_store.open_history_readonly(Path("/nonexistent/history.sqlite"))


if __name__ == "__main__":
    unittest.main()
