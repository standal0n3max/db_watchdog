"""Регресс: ``docker exec -i`` не должен наследовать stdin родителя.

Если ``docker exec -i`` получает stdin родителя (pty или незакрытый pipe),
stdin не даёт EOF, и ``opencode run`` зависает навсегда: прогон заканчивается
``crash_unknown`` по таймауту вместо ответа модели. Поэтому все обёртки над
``subprocess.run`` обязаны явно передавать ``stdin=subprocess.DEVNULL``.
"""

from __future__ import annotations

import subprocess
import unittest
from unittest import mock

from db_pipeline.benchmark import container, postgres, session


class _FakeCompleted:
    returncode = 0
    stdout = ""
    stderr = ""


def _captured_stdin(call_args: object) -> object:
    kwargs = getattr(call_args, "kwargs", {})
    return kwargs.get("stdin", "<missing>")


class DockerExecStdinTests(unittest.TestCase):
    def test_run_in_container_closes_stdin(self) -> None:
        with mock.patch.object(subprocess, "run", return_value=_FakeCompleted()) as run:
            postgres.run_in_container("cid", "echo hi", timeout=10)
        self.assertIs(_captured_stdin(run.call_args), subprocess.DEVNULL)

    def test_inspect_running_user_closes_stdin(self) -> None:
        with mock.patch.object(subprocess, "run", return_value=_FakeCompleted()) as run:
            container.inspect_running_user("cid")
        self.assertIs(_captured_stdin(run.call_args), subprocess.DEVNULL)

    def test_session_exec_closes_stdin(self) -> None:
        with mock.patch.object(subprocess, "run", return_value=_FakeCompleted()) as run:
            session._exec("cid", "echo hi", "")  # pyright: ignore[reportPrivateUsage]
        self.assertIs(_captured_stdin(run.call_args), subprocess.DEVNULL)


if __name__ == "__main__":
    unittest.main()
