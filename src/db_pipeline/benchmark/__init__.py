"""Точки входа benchmark-прогона PostgreSQL.

Пакет не импортирует ``postgres_gym``: поведение перенесено, репозиторий стенда
берётся только как читаемый источник. Команды — ``python -m db_pipeline.benchmark``.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
