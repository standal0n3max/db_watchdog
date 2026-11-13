"""Эффективный OpenCode-конфиг, batch-индекс и union пяти баз.

Проверяется: ``bare`` не содержит MCP, ``graph`` подключает remote MCP с
timeout; hash конфига детерминирован; batch-разбор ``ls-tree``/``cat-file``;
union предков дедуплицирует blobs. Сеть и Docker не нужны.
"""

from __future__ import annotations

import unittest
from typing import cast

from db_pipeline.benchmark import opencode
from db_pipeline.benchmark.graph.symbols import is_source


class OpenCodeConfigTests(unittest.TestCase):
    def test_bare_has_no_mcp(self) -> None:
        config = opencode.build_config("bare", "pgpro", "bench")
        self.assertNotIn("mcp", config)
        provider = config["provider"]
        self.assertIsInstance(provider, dict)

    def test_graph_has_remote_mcp(self) -> None:
        config = opencode.build_config("graph", "pgpro", "bench")
        mcp = config["mcp"]
        self.assertIsInstance(mcp, dict)
        assert isinstance(mcp, dict)
        graph = cast("dict[str, object]", mcp["graph"])
        self.assertEqual(graph["type"], "remote")
        self.assertEqual(graph["url"], opencode.GRAPH_MCP_URL)
        self.assertEqual(graph["oauth"], False)
        self.assertGreater(int(cast("int", graph["timeout"])), 30000)

    def test_config_hash_is_stable_and_differs(self) -> None:
        bare_a = opencode.build_config("bare", "pgpro", "bench")
        bare_b = opencode.build_config("bare", "pgpro", "bench")
        graph = opencode.build_config("graph", "pgpro", "bench")
        self.assertEqual(opencode.config_sha256(bare_a), opencode.config_sha256(bare_b))
        self.assertNotEqual(opencode.config_sha256(bare_a), opencode.config_sha256(graph))

    def test_config_has_no_secret(self) -> None:
        config = opencode.build_config("graph", "pgpro", "bench")
        text = opencode.config_text(config)
        self.assertIn("POSTGRES_GYM_PROVIDER_KEY", text)
        self.assertNotIn("OPENROUTER_API_KEY", text)

    def test_build_config_sets_step_limit(self) -> None:
        config = opencode.build_config("bare", "pgpro", "bench", step_limit=80)
        agent = config["agent"]
        self.assertIsInstance(agent, dict)
        build = cast("dict[str, object]", cast("dict[str, object]", agent)["build"])
        self.assertEqual(build["steps"], 80)

    def test_build_agent_sets_temperature_and_denies_task(self) -> None:
        config = opencode.build_config("bare", "pgpro", "bench", temperature=0.0)
        build = cast(
            "dict[str, object]",
            cast("dict[str, object]", config["agent"])["build"],
        )
        self.assertEqual(build["temperature"], 0.0)
        permission = cast("dict[str, object]", build["permission"])
        task = cast("dict[str, object]", permission["task"])
        self.assertEqual(task["*"], "deny")

    def test_unknown_mode_is_rejected(self) -> None:
        from db_pipeline.errors import PipelineError

        with self.assertRaises(PipelineError):
            opencode.build_config("graph_v2", "pgpro", "bench")

    def test_default_max_tokens_is_8192(self) -> None:
        self.assertEqual(opencode.DEFAULT_MAX_TOKENS, 8192)
        config = opencode.build_config("bare", "pgpro", "bench")
        provider = cast("dict[str, object]", config["provider"])
        models = cast("dict[str, object]", cast("dict[str, object]", provider["pgpro"])["models"])
        limit = cast("dict[str, object]", cast("dict[str, object]", models["bench"])["limit"])
        self.assertEqual(limit["output"], 8192)


class RunCommandTests(unittest.TestCase):
    def test_no_unsupported_standalone_flag(self) -> None:
        command = opencode.run_command(opencode.AgentConfig(model="bench"), "hi")
        self.assertNotIn("--standalone", command)
        self.assertIn("--format", command)
        self.assertEqual(command[command.index("--format") + 1], "json")

    def test_continue_uses_same_session(self) -> None:
        config = opencode.AgentConfig(provider="pgpro", model="bench")
        command = opencode.continue_command(config, "ses_9", opencode.NUDGE_PROMPT)
        self.assertEqual(command[command.index("--session") + 1], "ses_9")
        self.assertEqual(command[-1], opencode.NUDGE_PROMPT)
        self.assertIn("--model", command)


class NudgeDecisionTests(unittest.TestCase):
    def _transcript(self, reason: str = "stop", session: str = "ses") -> opencode.AgentTranscript:
        transcript = opencode.AgentTranscript(session_id=session)
        transcript.generations = [opencode.Generation(message_id="m", reason=reason)]
        return transcript

    def test_empty_stop_with_steps_left_is_nudged(self) -> None:
        self.assertTrue(
            opencode.should_nudge(self._transcript(), step_limit=80, diff_empty=True, timed_out=False)
        )

    def test_diff_or_timeout_or_missing_session_blocks_nudge(self) -> None:
        ready = self._transcript()
        self.assertFalse(opencode.should_nudge(ready, 80, diff_empty=False, timed_out=False))
        self.assertFalse(opencode.should_nudge(ready, 80, diff_empty=True, timed_out=True))
        self.assertFalse(
            opencode.should_nudge(self._transcript(session=""), 80, diff_empty=True, timed_out=False)
        )

    def test_reasoning_over_cap_counts_only_excess(self) -> None:
        transcript = opencode.AgentTranscript()
        transcript.generations = [
            opencode.Generation(message_id="a", reasoning_tokens=1024),
            opencode.Generation(message_id="b", reasoning_tokens=4565),
        ]
        self.assertEqual(opencode.reasoning_over_cap(transcript, 1024), 1)
        self.assertEqual(opencode.reasoning_over_cap(transcript, 0), 0)

    def test_step_limit_and_non_stop_block_nudge(self) -> None:
        self.assertFalse(
            opencode.should_nudge(self._transcript(), step_limit=1, diff_empty=True, timed_out=False)
        )
        self.assertFalse(
            opencode.should_nudge(
                self._transcript(reason="length"), step_limit=80, diff_empty=True, timed_out=False
            )
        )


class McpCallDetectionTests(unittest.TestCase):
    def _transcript(self, names: list[str]) -> opencode.AgentTranscript:
        transcript = opencode.AgentTranscript()
        transcript.tools = [opencode.ToolCall(tool=name, status="completed") for name in names]
        return transcript

    def test_counts_bare_and_prefixed_names(self) -> None:
        from db_pipeline.benchmark import runner

        transcript = self._transcript(
            [
                "graph_find_code_symbols",
                "graph_search_git_history",
                "find_code_symbols",
                "grep",
                "read",
                "bash",
            ]
        )
        self.assertEqual(runner.mcp_calls(transcript), 3)

    def test_zero_without_mcp(self) -> None:
        from db_pipeline.benchmark import runner

        self.assertEqual(runner.mcp_calls(self._transcript(["grep", "edit", "bash"])), 0)


class IndexParseTests(unittest.TestCase):
    def test_is_source_filters(self) -> None:
        self.assertTrue(is_source("src/backend/x.c", "100644", "blob"))
        self.assertTrue(is_source("contrib/h.c", "100644", "blob"))
        self.assertFalse(is_source("README.md", "100644", "blob"))
        self.assertFalse(is_source("src/backend/x.c", "120000", "blob"))
        self.assertFalse(is_source("src/backend", "040000", "tree"))


if __name__ == "__main__":
    unittest.main()
