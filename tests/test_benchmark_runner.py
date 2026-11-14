"""Промпт прогона: сводка и имена ``graph_*`` для graph-руки.

Рамка абзаца зависит от варианта (``directive`` против ``situational``).
Docker, сеть и LLM не нужны.
"""

from __future__ import annotations

import hashlib
import unittest

from db_pipeline.benchmark import runner
from db_pipeline.benchmark.telemetry import TRACE_TEXT_LIMIT

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

    def test_graph_mentions_prefixed_tools(self) -> None:
        text = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "graph", runner.PROMPT_DIRECTIVE, "briefing body"
        )
        for name in (
            "graph_find_code_symbols",
            "graph_tests_to_code",
            "graph_similar_fixes",
            "graph_find_error_origin",
        ):
            self.assertIn(name, text)
        self.assertIn("briefing body", text)

    def test_directive_and_situational_differ(self) -> None:
        directive = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "graph", runner.PROMPT_DIRECTIVE
        )
        situational = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "graph", runner.PROMPT_SITUATIONAL
        )
        self.assertNotEqual(directive, situational)
        self.assertIn("Graph tools are available", directive)
        self.assertNotIn("Before editing", directive)
        self.assertIn("Start editing as soon as", situational)

    def test_variant_does_not_change_tool_set(self) -> None:
        """Рамка промпта не должна менять набор упомянутых инструментов."""
        directive = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "graph", runner.PROMPT_DIRECTIVE
        )
        situational = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            PUBLIC, "graph", runner.PROMPT_SITUATIONAL
        )
        for name in ("graph_find_code_symbols", "graph_symbol_history"):
            self.assertEqual(name in directive, name in situational)

    def test_default_prompt_is_directive(self) -> None:
        self.assertEqual(runner.PROMPT_DEFAULT, runner.PROMPT_DIRECTIVE)

    def test_bare_and_graph_prompts_omit_private_oracle(self) -> None:
        public = dict(PUBLIC)
        public["gold_patch"] = "SECRET_GOLD_PATCH"
        public["head"] = "SECRET_HEAD_SHA"
        bare = runner._prompt(public, "bare")  # pyright: ignore[reportPrivateUsage]
        graph = runner._prompt(  # pyright: ignore[reportPrivateUsage]
            public, "graph", runner.PROMPT_DIRECTIVE, "visible briefing"
        )
        for text in (bare, graph):
            self.assertNotIn("SECRET_GOLD_PATCH", text)
            self.assertNotIn("SECRET_HEAD_SHA", text)
        self.assertIn("visible briefing", graph)
        self.assertNotIn("visible briefing", bare)

    def test_prompt_sha_and_trace_input_share_the_prompt(self) -> None:
        text = runner._prompt(PUBLIC, "graph", runner.PROMPT_DIRECTIVE, "brief")  # pyright: ignore[reportPrivateUsage]
        record: dict[str, object] = {}
        runner.stamp_prompt(record, text)
        self.assertEqual(record["agent_prompt"], text)
        self.assertEqual(
            record["agent_prompt_sha256"],
            hashlib.sha256(text.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(runner.trace_input(text), text)
        long_prompt = "x" * (TRACE_TEXT_LIMIT + 50)
        clipped = runner.trace_input(long_prompt)
        self.assertEqual(len(clipped), TRACE_TEXT_LIMIT)
        self.assertTrue(long_prompt.startswith(clipped))

    def test_provider_check_accepts_deepinfra_and_flags_mismatch(self) -> None:
        record: dict[str, object] = {}
        before: dict[str, object] = {"provider": "", "requests": 1}
        after: dict[str, object] = {"provider": "DeepInfra", "requests": 4}
        runner.stamp_provider(record, before, after, "deepinfra")
        self.assertFalse(record["provider_mismatch"])
        check = record["provider_check"]
        self.assertIsInstance(check, dict)
        self.assertEqual(record["openrouter_requests"], 3)
        self.assertEqual(record["openrouter_provider_configured"], "deepinfra")
        other: dict[str, object] = {}
        together: dict[str, object] = {"provider": "Together", "requests": 2}
        runner.stamp_provider(other, before, together, "deepinfra")
        self.assertTrue(other["provider_mismatch"])


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
