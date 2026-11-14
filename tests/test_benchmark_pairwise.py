"""Слоты pairwise: план, checkpoint и resume без Docker."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from db_pipeline.benchmark import pairwise
from db_pipeline.benchmark.config import load_config
from db_pipeline.benchmark.session import gateway_environment
from db_pipeline.errors import PipelineError

EXPERIMENT = "holdout-frozen-r1-2026-11-14"
IMAGES = {"task": "sha256:task", "graph": "sha256:graph", "gateway": "sha256:gw"}


def _names(count: int) -> list[str]:
    return [f"postgres__{index:012d}" for index in range(count)]


class PlanTests(unittest.TestCase):
    def test_frozen_r1_has_558_unique_slots(self) -> None:
        names = _names(279)
        slots = pairwise.build_slots(EXPERIMENT, names, repeats=1)
        self.assertEqual(len(slots), 558)
        self.assertEqual(len({slot.slot_id for slot in slots}), 558)
        self.assertEqual(slots[0].mode, "bare")
        self.assertEqual(slots[1].mode, "graph")
        self.assertEqual(slots[2].mode, "graph")
        self.assertEqual(slots[3].mode, "bare")

    def test_duplicate_slot_id_is_rejected(self) -> None:
        slot = pairwise.SlotPlan("same", "postgres__a", "bare", 0, 0)
        with self.assertRaises(PipelineError):
            pairwise.ensure_unique([slot, slot])

    def test_plan_hash_changes_when_images_change(self) -> None:
        names = _names(1)
        slots = pairwise.build_slots(EXPERIMENT, names, 1)
        left = pairwise.plan_sha256(
            pairwise.plan_core(
                EXPERIMENT, "frozen", 1, "directive", names, slots, {"model": "m"}, "g", IMAGES
            )
        )
        other = dict(IMAGES)
        other["gateway"] = "sha256:other"
        right = pairwise.plan_sha256(
            pairwise.plan_core(
                EXPERIMENT, "frozen", 1, "directive", names, slots, {"model": "m"}, "g", other
            )
        )
        self.assertNotEqual(left, right)


class ResumeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="pairwise-")
        self.data = Path(self.tmp.name)
        self._saved = {
            key: os.environ.get(key)
            for key in ("BENCH_EXPERIMENT_ID", "BENCH_OPENROUTER_PROVIDER", "BENCH_PROVIDER")
        }
        os.environ["BENCH_EXPERIMENT_ID"] = EXPERIMENT
        os.environ["BENCH_OPENROUTER_PROVIDER"] = "deepinfra"
        os.environ["BENCH_PROVIDER"] = "pgpro"
        self.cfg = load_config()

    def tearDown(self) -> None:
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def _run(
        self,
        names: list[str],
        execute: pairwise.Execute,
        resume: bool = False,
        images: dict[str, str] | None = None,
    ) -> dict[str, object]:
        return pairwise.run_batch(
            self.data,
            "frozen",
            1,
            "directive",
            "holdout-frozen-r1-2026-11-14",
            names,
            resume=resume,
            no_budget_check=True,
            cfg=self.cfg,
            execute=execute,
            images=images or dict(IMAGES),
            graph_sha256="graph-sha",
        )

    def test_resume_skips_finalized_fail_and_accepts_disk_record(self) -> None:
        names = _names(1)
        calls: list[str] = []

        def execute(slot: pairwise.SlotPlan) -> dict[str, object]:
            calls.append(slot.slot_id)
            return {"record_finalized": True, "verdict": "FAIL", "pass": False}

        first = self._run(names, execute)
        self.assertEqual(first["exit_code"], 0)
        self.assertEqual(len(calls), 2)
        calls.clear()

        def fail_if_called(slot: pairwise.SlotPlan) -> dict[str, object]:
            raise AssertionError(slot.slot_id)

        second = self._run(names, fail_if_called, resume=True)
        self.assertEqual(second["exit_code"], 0)
        self.assertEqual(calls, [])

    def test_running_slot_with_finalized_record_is_not_rerun(self) -> None:
        names = _names(1)
        slots = pairwise.build_slots(EXPERIMENT, names, 1)
        running = slots[0]
        record = {
            "func": running.task,
            "mode": running.mode,
            "pairwise_slot_id": running.slot_id,
            "record_finalized": True,
            "repetition": 0,
            "verdict": "FAIL",
            "pass": False,
        }
        runs = self.data / "runs"
        runs.mkdir(parents=True)
        (runs / f"{running.task}__{running.mode}__trace.json").write_text(
            json.dumps(record), encoding="utf-8"
        )
        called: list[str] = []

        def execute(slot: pairwise.SlotPlan) -> dict[str, object]:
            called.append(slot.slot_id)
            if slot.slot_id == running.slot_id:
                raise AssertionError("финализированный слот запущен снова")
            return {"record_finalized": True, "verdict": "FAIL", "pass": False}

        report = self._run(names, execute)
        self.assertEqual(report["exit_code"], 0)
        self.assertNotIn(running.slot_id, called)
        self.assertEqual(len(called), 1)

    def test_plan_mismatch_and_missing_checkpoint_are_errors(self) -> None:
        names = _names(1)

        def execute(slot: pairwise.SlotPlan) -> dict[str, object]:
            del slot
            return {"record_finalized": True, "verdict": "FAIL", "pass": False}

        self._run(names, execute)
        with self.assertRaises(PipelineError):
            self._run(names, execute, resume=True, images={"task": "x", "graph": "y", "gateway": "z"})
        with self.assertRaises(PipelineError):
            pairwise.run_batch(
                self.data,
                "frozen",
                1,
                "directive",
                "missing-label",
                names,
                resume=True,
                no_budget_check=True,
                cfg=self.cfg,
                execute=execute,
                images=dict(IMAGES),
                graph_sha256="graph-sha",
            )

    def test_existing_checkpoint_without_resume_is_rejected(self) -> None:
        names = _names(1)

        def execute(slot: pairwise.SlotPlan) -> dict[str, object]:
            del slot
            return {"record_finalized": True, "verdict": "PASS", "pass": True}

        self._run(names, execute)
        with self.assertRaises(PipelineError):
            self._run(names, execute, resume=False)

    def test_provider_mismatch_stops_before_the_next_slot(self) -> None:
        names = _names(1)
        calls: list[str] = []

        def execute(slot: pairwise.SlotPlan) -> dict[str, object]:
            calls.append(slot.mode)
            return {"record_finalized": False, "provider_mismatch": True, "verdict": "FAIL"}

        report = self._run(names, execute)
        self.assertEqual(report["exit_code"], 3)
        self.assertEqual(calls, ["bare"])
        saved = json.loads(
            pairwise.checkpoint_path(self.data, EXPERIMENT).read_text(encoding="utf-8")
        )
        states = [row["state"] for row in saved["slots"]]
        self.assertEqual(states[0], "pending")
        self.assertEqual(states[1], "pending")

    def test_parallel_lock_is_rejected(self) -> None:
        path = pairwise.lock_path(self.data, "held")
        first = pairwise.acquire_lock(path)
        try:
            with self.assertRaises(PipelineError):
                pairwise.acquire_lock(path)
        finally:
            pairwise.release_lock(first)


class ProviderConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = {
            key: os.environ.get(key)
            for key in ("BENCH_OPENROUTER_PROVIDER", "BENCH_PROVIDER")
        }

    def tearDown(self) -> None:
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_openrouter_provider_is_not_the_opencode_alias(self) -> None:
        os.environ["BENCH_PROVIDER"] = "pgpro"
        os.environ["BENCH_OPENROUTER_PROVIDER"] = "deepinfra"
        cfg = load_config()
        self.assertEqual(cfg.provider, "pgpro")
        self.assertEqual(cfg.openrouter_provider, "deepinfra")
        manifest = cfg.to_manifest()
        self.assertEqual(manifest["provider"], "pgpro")
        self.assertEqual(manifest["openrouter_provider"], "deepinfra")
        environment = gateway_environment(cfg, "secret-key")
        self.assertEqual(environment["BENCH_OPENROUTER_PROVIDER"], "deepinfra")
        self.assertNotEqual(environment["BENCH_OPENROUTER_PROVIDER"], cfg.provider)
        self.assertEqual(environment["OPENROUTER_API_KEY"], "secret-key")


if __name__ == "__main__":
    unittest.main()
