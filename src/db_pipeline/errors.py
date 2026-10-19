"""Ошибки сборщика с кодом причины для журнала.

``PipelineError(reason_code, reason)`` несёт оба поля. ``ConfigError`` — тот же тип с кодом ``config``.
"""

from __future__ import annotations


class PipelineError(Exception):
    def __init__(self, reason_code: str, reason: str) -> None:
        super().__init__(reason)
        self.reason_code = reason_code
        self.reason = reason


class ConfigError(PipelineError):
    def __init__(self, reason: str) -> None:
        super().__init__("config", reason)
