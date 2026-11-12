"""Свой граф и MCP-сервер с временной отсечкой.

Модули: ``gitgraph`` (сборка), ``view`` (отсечка и семь инструментов),
``symbols`` (сканер), ``cutoff``. ``head_sha`` и его потомки не выдаются.
"""

from __future__ import annotations

from db_pipeline.benchmark.graph import cutoff

__all__ = ["cutoff"]
