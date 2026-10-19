"""Очередь ``--harvest``: сколько задач ещё принять в следующем окне.

``next_window_accept(accepted, goal, scan)`` вычитает уже принятые в журнале из ``--goal``. ``None`` — потолок добран, ядро в очереди заканчивается. ``goal`` пустой или 0 оставляет окно размером ``scan`` до конца истории.
"""

from __future__ import annotations

import unittest

from db_pipeline.__main__ import next_window_accept


class SequenceTests(unittest.TestCase):
    def test_goal_counts_tasks_already_in_the_journal(self) -> None:
        self.assertEqual(next_window_accept(2, 300, 5000), 298)
        self.assertIsNone(next_window_accept(300, 300, 5000))
        self.assertIsNone(next_window_accept(301, 300, 5000))

    def test_missing_goal_follows_history_to_the_end_of_the_window(self) -> None:
        self.assertEqual(next_window_accept(49, None, 5000), 5000)
        self.assertEqual(next_window_accept(49, 0, 5000), 5000)


if __name__ == "__main__":
    unittest.main()
