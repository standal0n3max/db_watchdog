"""Промпт прогона: абзац про историю для graph-рук и вариант формулировки.

Проверяется ортогональность двух параметров слоя 3: состав инструментов задан
рукой (``graph`` упоминает v1+v2, ``graph_v2`` — только v2), а рамка абзаца —
вариантом (``directive`` против ``situational``). Docker, сеть и LLM не нужны.
"""

from __future__ import annotations

import unittest

from db_pipeline.benchmark import runner

PUBLIC: dict[str, object] = {
    "area": "backend",
    "goal": "Fix the crash.",
    "contract": [],
    "graded_by": [],
}


class PromptVariantTests(unittest.TestCase):
    def test_bare_has_no_history_paragraph(self) -> None:
        text = runner._prompt(PUBLIC, "bare")  # pyright: ignore[reportPrivateUsage]
        self.assertNotIn("MCP", text)
        self.assertEqual(text, runner._prompt(PUBLIC, "bare"))  # pyright: ignore[reportPrivateUsage]

    def test_bare_ignores_variant(self) -> None:
        directive = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "bare", runner.PROMPT_DIRECTIVE
        )
        situational = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "bare", runner.PROMPT_SITUATIONAL
        )
        self.assertEqual(directive, situational)

    def test_graph_mentions_v1_and_v2_tools(self) -> None:
        text = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "graph", runner.PROMPT_SITUATIONAL
        )
        for name in ("find_code_symbols", "search_git_history", "symbol_history"):
            self.assertIn(name, text)

    def test_graph_v2_omits_disabled_v1_tools(self) -> None:
        """Промпт абляции не зовёт инструменты, выключенные в конфиге."""
        text = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "graph_v2", runner.PROMPT_SITUATIONAL
        )
        for name in ("find_code_symbols", "read_versioned_source", "search_git_history"):
            self.assertNotIn(name, text)
        for name in ("symbol_history", "tests_to_code", "similar_fixes", "find_error_origin"):
            self.assertIn(name, text)

    def test_directive_and_situational_differ(self) -> None:
        directive = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "graph", runner.PROMPT_DIRECTIVE
        )
        situational = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "graph", runner.PROMPT_SITUATIONAL
        )
        self.assertNotEqual(directive, situational)
        self.assertIn("Before editing, use the MCP tools", directive)
        self.assertIn("Start editing as soon as", situational)

    def test_variant_does_not_change_tool_set(self) -> None:
        """Рамка промпта не должна менять набор упомянутых инструментов."""
        directive = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "graph_v2", runner.PROMPT_DIRECTIVE
        )
        situational = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "graph_v2", runner.PROMPT_SITUATIONAL
        )
        for name in ("find_code_symbols", "symbol_history"):
            self.assertEqual(name in directive, name in situational)


class PromptVariantValidationTests(unittest.TestCase):
    def test_unknown_mode_is_rejected(self) -> None:
        from db_pipeline.errors import PipelineError

        with self.assertRaises(PipelineError):
            runner.run_one("postgres__missing", mode="graph_v3")

    def test_unknown_variant_is_rejected(self) -> None:
        from db_pipeline.errors import PipelineError

        with self.assertRaises(PipelineError):
            runner.run_one("postgres__missing", mode="graph", prompt_variant="terse")


if __name__ == "__main__":
    unittest.main()
