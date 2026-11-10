"""Свой граф и MCP-сервер с временной отсечкой.

Модули: ``store`` (SQLite-индекс), ``index`` (наполнение), ``cutoff`` (проверка
предковства), ``server`` (Streamable HTTP MCP), ``tools`` (три инструмента).
``head_sha``, его патч и потомки никогда не индексируются в сессию.
"""

from __future__ import annotations

from db_pipeline.benchmark.graph import cutoff, store, tools

__all__ = ["cutoff", "store", "tools"]
