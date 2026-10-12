from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.errors import ConfigError


@beartype
def load_json_file(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


@beartype
def as_dict(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ConfigError(f"{label}: ожидался объект")
    raw = cast(dict[object, object], value)
    result: dict[str, object] = {}
    for key, item in raw.items():
        if not isinstance(key, str):
            raise ConfigError(f"{label}: ключ не строка")
        result[key] = item
    return result


@beartype
def as_str(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"{label}: ожидалась строка")
    return value


@beartype
def as_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{label}: ожидалось целое")
    return value


@beartype
def as_bool(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{label}: ожидался флаг")
    return value


@beartype
def as_object_list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise ConfigError(f"{label}: ожидался список")
    return cast(list[object], value)


@beartype
def as_str_list(value: object, label: str) -> list[str]:
    result: list[str] = []
    for item in as_object_list(value, label):
        if not isinstance(item, str):
            raise ConfigError(f"{label}: элемент списка не строка")
        result.append(item)
    return result


@beartype
def as_int_list(value: object, label: str) -> list[int]:
    result: list[int] = []
    for item in as_object_list(value, label):
        if isinstance(item, bool) or not isinstance(item, int):
            raise ConfigError(f"{label}: элемент списка не целое")
        result.append(item)
    return result


@beartype
def optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError("pr_number: ожидалось целое или null")
    return value
