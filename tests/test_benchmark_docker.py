"""Docker-интеграция: граница изоляции и anti-leak.

Тест включается переменной ``BENCH_DOCKER_TESTS=1``: нужен собранный образ
``db-pipeline-bench-postgres:local`` и запущенный Docker. Проверяется, что
одноразовый контейнер не имеет mounts, процесс непривилегированный, история
оборвана до одного baseline-коммита без remotes и reflog, ``git show`` по
эталонному sha не находит коммит, а скрытые тесты и oracle недоступны во время
фазы агента. Полная пересборка PostgreSQL не выполняется: проверяется граница,
а не сборка.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from db_pipeline.benchmark import container, convert, paths, postgres, tree
from db_pipeline.benchmark.models import as_str_list

TASK = "postgres__d764d61af316"
IMAGE = os.environ.get("BENCH_TASK_IMAGE", "db-pipeline-bench-postgres:local")


def _docker_tests_enabled() -> bool:
    return os.environ.get("BENCH_DOCKER_TESTS", "0") == "1"


@unittest.skipUnless(_docker_tests_enabled(), "BENCH_DOCKER_TESTS не включён")
class IsolationBoundaryTests(unittest.TestCase):
    oracle: dict[str, object] = {}
    public: dict[str, object] = {}

    @classmethod
    def setUpClass(cls) -> None:
        if container.docker_available() is not None:
            raise unittest.SkipTest("docker недоступен")
        if not (paths.tasks_dir() / f"{TASK}.json").is_file():
            raise unittest.SkipTest("нет сконвертированной задачи")
        cls.oracle = convert.load_oracle(TASK)
        cls.public = convert.load_public(TASK)

    def _create_and_start(self) -> str:
        spec = container.ContainerSpec(
            image=IMAGE, workspace=postgres.WORKSPACE, user=postgres.BENCH_USER
        )
        container_id = container.create_container(spec)
        head = str(self.oracle["head"])
        with tempfile.TemporaryDirectory(prefix="bench-docker-test-") as tmp:
            tree_dir = Path(tmp) / "tree"
            tree.prepare_buggy_tree(
                paths.pg_clone(), head, str(self.oracle["reverse_patch"]), tree_dir
            )
            archive_path = Path(tmp) / tree.ARCHIVE_NAME
            archive_path.write_bytes(tree.archive_tree(tree_dir))
            container.copy_in(container_id, archive_path, "/work/tree.tar.gz")
        container.start_detached(container_id)
        if not container.wait_ready(container_id, "/work/ready"):
            raise RuntimeError("entrypoint не поставил /work/ready")
        return container_id

    def test_no_mounts_and_non_root(self) -> None:
        container_id = self._create_and_start()
        try:
            mounts = container.inspect_mounts(container_id)
            self.assertEqual(mounts, [])
            code, output = postgres.run_in_container(
                container_id, "test -f configure && echo ready", timeout=120
            )
            self.assertEqual(code, 0)
            self.assertIn("ready", output)
            uid = container.inspect_running_user(container_id)
            self.assertNotEqual(uid, "0")
        finally:
            container.remove_container(container_id)

    def test_history_is_severed_and_head_unreachable(self) -> None:
        container_id = self._create_and_start()
        try:
            hidden = postgres.hide_tests(container_id, [])
            self.assertEqual(hidden, [])
            ok, output = postgres.sever_history(container_id)
            self.assertTrue(ok, output)
            state = postgres.git_state(container_id)
            self.assertEqual(state.get("commits"), 1)
            self.assertEqual(state.get("remotes"), 0)
            self.assertEqual(state.get("reflog"), 0)
            head = str(self.oracle["head"])
            code, show = postgres.run_in_container(
                container_id, f"git show {head}", timeout=120
            )
            self.assertNotEqual(code, 0)
            lowered = show.lower()
            self.assertTrue(
                "unknown revision" in lowered or "bad object" in lowered,
                show,
            )
        finally:
            container.remove_container(container_id)

    def test_hidden_test_is_absent_during_agent_phase(self) -> None:
        container_id = self._create_and_start()
        try:
            graded = as_str_list(self.public.get("graded_by"))
            if not graded:
                self.skipTest("нет graded_by")
            present = [
                rel
                for rel in graded
                if postgres.run_in_container(
                    container_id, f"test -e {rel} && echo here", timeout=60
                )[0]
                == 0
            ]
            self.assertEqual(present, graded, "тесты должны быть на месте до скрытия")
            hidden = postgres.hide_tests(container_id, graded)
            self.assertEqual(sorted(hidden), sorted(graded))
            for rel in graded:
                code, _ = postgres.run_in_container(
                    container_id, f"test -e {rel} && echo here", timeout=60
                )
                self.assertNotEqual(code, 0, f"{rel} должен быть скрыт")
        finally:
            container.remove_container(container_id)

    def test_parent_reads_are_absent(self) -> None:
        container_id = self._create_and_start()
        try:
            code, output = postgres.run_in_container(
                container_id,
                "test -d /data && echo leaked || echo isolated",
                timeout=60,
            )
            self.assertEqual(code, 0)
            self.assertIn("isolated", output)
        finally:
            container.remove_container(container_id)

    def test_worktree_diff_detects_edit(self) -> None:
        container_id = self._create_and_start()
        try:
            ok, output = postgres.sever_history(container_id)
            self.assertTrue(ok, output)
            code, _ = postgres.run_in_container(
                container_id,
                "printf '\\n/* agent edit */\\n' >> src/backend/main/main.c && echo edited",
                timeout=60,
            )
            self.assertEqual(code, 0)
            changed = postgres.changed_files(container_id)
            self.assertIn("src/backend/main/main.c", changed)
            diff = postgres.worktree_diff(container_id)
            self.assertIn("agent edit", diff)
        finally:
            container.remove_container(container_id)


if __name__ == "__main__":
    unittest.main()
