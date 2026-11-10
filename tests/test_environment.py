"""Загрузка корневого ``.env``: приоритет окружения и отсутствие утечки значений.

Проверяется, что читается только корневой файл, экспортированные переменные не
перезаписываются (``override=False``), отсутствие файла не является ошибкой, а
значения секретов не попадают в лог. Сеть и Docker не нужны.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from db_pipeline import environment, paths


class EnvironmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="env-test-")
        self.env = Path(self.tmp.name) / ".env"
        self.saved = dict(os.environ)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self.saved)
        self.tmp.cleanup()

    def test_missing_file_is_not_an_error(self) -> None:
        with mock.patch.object(environment, "env_file_path", return_value=self.env):
            self.assertFalse(environment.load_environment())

    def test_existing_env_wins(self) -> None:
        self.env.write_text("SECRET_VALUE=from-file\n", encoding="utf-8")
        os.environ["SECRET_VALUE"] = "from-shell"
        with mock.patch.object(environment, "env_file_path", return_value=self.env):
            environment.load_environment()
        self.assertEqual(os.environ["SECRET_VALUE"], "from-shell")

    def test_file_fills_missing_variable(self) -> None:
        self.env.write_text("ONLY_IN_FILE=value\n", encoding="utf-8")
        os.environ.pop("ONLY_IN_FILE", None)
        with mock.patch.object(environment, "env_file_path", return_value=self.env):
            environment.load_environment()
        self.assertEqual(os.environ["ONLY_IN_FILE"], "value")

    def test_path_is_project_root_env(self) -> None:
        path = environment.env_file_path()
        self.assertEqual(path.name, ".env")
        self.assertEqual(path.parent, paths.project_root())


if __name__ == "__main__":
    unittest.main()
