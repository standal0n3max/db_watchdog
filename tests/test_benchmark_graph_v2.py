"""Leak-guard единого графа: канарейки, membership и patch-id.

Проверки не требуют Docker. Prior-twin по ``patch_id`` смотрит в ``graph.sqlite``,
если он собран; без файла остаётся отказ «нет предков» только когда клона нет.
"""

from __future__ import annotations

import unittest

from db_pipeline.benchmark.graph import leakguard


class LeakguardPureTests(unittest.TestCase):
    def test_added_lines_exclude_header(self) -> None:
        patch = "diff --git a/x b/x\n--- a/x\n+++ b/x\n+kept\n+++ not a line\n"
        self.assertEqual(leakguard.added_lines(patch), ["kept"])

    def test_nontrivial_lines_filter_short_and_trivial(self) -> None:
        short = "+" + "x" * 10
        trivial = "+" + (" " * 50)
        real = "+" + ("alpha_beta_gamma_delta_epsilon_zeta_" * 2)
        patch = f"diff --git a/x b/x\n--- a/x\n+++ b/x\n{short}\n{trivial}\n{real}\n"
        found = leakguard.nontrivial_lines(patch)
        self.assertEqual(len(found), 1)
        self.assertIn("alpha_beta", found[0])

    def test_patch_paths_are_extracted(self) -> None:
        patch = (
            "diff --git a/src/backend/x.c b/src/backend/x.c\n"
            "diff --git a/src/test/regress/sql/t.sql b/src/test/regress/sql/t.sql\n"
        )
        self.assertEqual(
            leakguard.patch_paths(patch),
            ["src/backend/x.c", "src/test/regress/sql/t.sql"],
        )

    def test_scan_responses_flags_canary_and_head(self) -> None:
        head = "a" * 40
        self.assertTrue(
            leakguard.scan_responses({"tool": "secret canary line"}, ["secret canary line"], head)
        )
        self.assertTrue(leakguard.scan_responses({"tool": head}, [], head))
        self.assertEqual(leakguard.scan_responses({"tool": "clean"}, ["other"], head), [])

    def test_check_membership_flags_unknown_oid(self) -> None:
        allowed = {"b" * 40}
        unknown = "c" * 40
        violations = leakguard.check_membership({"tool": unknown}, allowed)
        self.assertEqual(len(violations), 1)
        self.assertEqual(leakguard.check_membership({"tool": "b" * 40}, allowed), [])

    def test_patch_id_empty_for_empty_patch(self) -> None:
        self.assertEqual(leakguard.patch_id(""), "")

    def test_holdout_fingerprint_is_order_independent(self) -> None:
        from db_pipeline.benchmark.graph import split as split_module

        left = split_module.holdout_sha256(["b", "a"])
        right = split_module.holdout_sha256(["a", "b"])
        self.assertEqual(left, right)


if __name__ == "__main__":
    unittest.main()
