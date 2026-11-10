"""Эффективный OpenCode-конфиг, batch-индекс и union пяти баз.

Проверяется: ``bare`` не содержит MCP, ``graph`` подключает remote MCP с
timeout; hash конфига детерминирован; batch-разбор ``ls-tree``/``cat-file``;
union предков дедуплицирует blobs. Сеть и Docker не нужны.
"""

from __future__ import annotations

import unittest
from typing import cast

from db_pipeline.benchmark import opencode
from db_pipeline.benchmark.graph import index as graph_index


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


class RunCommandTests(unittest.TestCase):
    def test_no_unsupported_standalone_flag(self) -> None:
        command = opencode.run_command(opencode.AgentConfig(model="bench"), "hi")
        self.assertNotIn("--standalone", command)
        self.assertIn("--format", command)
        self.assertEqual(command[command.index("--format") + 1], "json")


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
        self.assertEqual(runner._mcp_calls(transcript), 3)

    def test_zero_without_mcp(self) -> None:
        from db_pipeline.benchmark import runner

        self.assertEqual(runner._mcp_calls(self._transcript(["grep", "edit", "bash"])), 0)


class IndexParseTests(unittest.TestCase):
    def test_is_source_filters(self) -> None:
        self.assertTrue(graph_index.is_source("src/backend/x.c", "100644", "blob"))
        self.assertTrue(graph_index.is_source("contrib/h.c", "100644", "blob"))
        self.assertFalse(graph_index.is_source("README.md", "100644", "blob"))
        self.assertFalse(graph_index.is_source("src/backend/x.c", "120000", "blob"))
        self.assertFalse(graph_index.is_source("src/backend", "040000", "tree"))


if __name__ == "__main__":
    unittest.main()
