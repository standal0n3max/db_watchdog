"""Интеграция конвертера на реальных данных (пропускается без них).

Если в ``data/benchmark/postgres`` есть сконвертированные задачи, проверяется
структура публичной части и oracle на пяти smoke-задачах. Без данных тест
пропускается, поэтому обычный ``pytest`` не зависит от локальных артефактов.
"""

from __future__ import annotations

import unittest
from typing import cast

from db_pipeline.benchmark import convert, paths

SMOKE = (
    "postgres__d764d61af316",
    "postgres__d8444163d22e",
    "postgres__96ab9a990eed",
    "postgres__3b120b1e94dd",
    "postgres__9b917a93116e",
)


def _as_list(value: object) -> list[object]:
    if isinstance(value, list):
        return list(cast("list[object]", value))
    return []


class RealDataTests(unittest.TestCase):
    def _available(self) -> list[str]:
        folder = paths.tasks_dir()
        found: list[str] = []
        for name in SMOKE:
            if (folder / f"{name}.json").is_file():
                found.append(name)
        return found

    def test_public_and_oracle_are_separate(self) -> None:
        available = self._available()
        if not available:
            self.skipTest("нет сконвертированных данных")
        for name in available:
            public = convert.load_public(name)
            oracle = convert.load_oracle(name)
            self.assertNotIn("reverse_patch", public)
            self.assertNotIn("reference", public)
            self.assertIn("head", oracle)
            self.assertIn("base", oracle)
            graded = [str(item) for item in _as_list(public.get("graded_by"))]
            self.assertEqual(graded, sorted(graded))

    def test_contract_is_grounded_in_test(self) -> None:
        name = "postgres__9b917a93116e"
        if not (paths.tasks_dir() / f"{name}.json").is_file():
            self.skipTest("нет данных")
        public = convert.load_public(name)
        oracle = convert.load_oracle(name)
        contract = _as_list(public.get("contract"))
        self.assertGreater(len(contract), 0)
        for message in contract:
            anchor = str(message).split('"')[0].strip()
            self.assertIn(anchor[:8], str(oracle["test"]))


if __name__ == "__main__":
    unittest.main()
