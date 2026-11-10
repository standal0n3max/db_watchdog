"""Модели benchmark: публичная задача, приватный oracle и run record.

``PublicTask`` — всё, что видит агент в промпте: ``func``, ``area``, ``goal``,
``contract``, ``graded_by``. Публичная часть не содержит эталонного патча и
содержимого тестов.

``Oracle`` — приватные данные оценки: ``head``/``base``, reverse source patch,
тестовый патч, эталон, список тестовых файлов, команда оракула и ожидаемые
тесты. ``graded_by`` содержит имена скрытых файлов.

``RunRecord`` — результат одного про��она: версии, конфигурация, gates, исход
тестов, вердикт и ссылка на трассу Langfuse.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

from beartype import beartype


@beartype
@dataclass(frozen=True)
class PublicTask:
    func: str
    area: str
    goal: str
    contract: list[str]
    graded_by: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "func": self.func,
            "area": self.area,
            "goal": self.goal,
            "contract": list(self.contract),
            "graded_by": list(self.graded_by),
        }


@beartype
@dataclass(frozen=True)
class Oracle:
    func: str
    head: str
    base: str
    source: str
    patch: str
    patch_text: str
    test: str
    test_files: list[str]
    reference: str
    oracle_command: str
    expected_tests: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "func": self.func,
            "head": self.head,
            "base": self.base,
            "source": self.source,
            "patch": self.patch,
            "reverse_patch": self.patch_text,
            "test": self.test,
            "test_files": list(self.test_files),
            "reference": self.reference,
            "oracle_command": self.oracle_command,
            "expected_tests": list(self.expected_tests),
        }


@beartype
@dataclass(frozen=True)
class CheckOutcome:
    """Исход одного шага harness или проверки тестов."""

    ok: bool
    seconds: float = 0.0
    total: int = 0
    failed: list[str] = field(default_factory=list[str])
    tail: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "seconds": self.seconds,
            "total": self.total,
            "failed": list(self.failed),
            "tail": self.tail,
        }


@beartype
@dataclass(frozen=True)
class GateResult:
    ok: bool
    detail: str = ""

    def to_dict(self) -> dict[str, object]:
        return {"ok": self.ok, "detail": self.detail}


@beartype
def oracle_from_dict(raw: dict[str, object]) -> Oracle:
    return Oracle(
        func=str(raw["func"]),
        head=str(raw["head"]),
        base=str(raw["base"]),
        source=str(raw["source"]),
        patch=str(raw["patch"]),
        patch_text=str(raw.get("reverse_patch") or ""),
        test=str(raw["test"]),
        test_files=as_str_list(raw.get("test_files")),
        reference=str(raw["reference"]),
        oracle_command=str(raw.get("oracle_command") or ""),
        expected_tests=as_str_list(raw.get("expected_tests")),
    )


@beartype
def public_from_dict(raw: dict[str, object]) -> PublicTask:
    return PublicTask(
        func=str(raw["func"]),
        area=str(raw["area"]),
        goal=str(raw["goal"]),
        contract=as_str_list(raw.get("contract")),
        graded_by=as_str_list(raw.get("graded_by")),
    )


@beartype
def as_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    items = cast("list[object]", value)
    return [str(item) for item in items]
