from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

from beartype import beartype

from db_pipeline.errors import ConfigError
from db_pipeline.parsing import as_bool, as_dict, as_int, as_int_list, as_object_list, as_str, as_str_list
from db_pipeline.paths import project_root


@beartype
@dataclass(frozen=True)
class CoreConfig:
    id: str
    repository: str
    clone_url: str
    license: str
    oracle: str
    enabled: bool
    code_prefixes: list[str]
    test_prefixes: list[str]
    generated_prefixes: list[str]
    pinned_pr: int | None
    oracle_candidate_prs: list[int]
    build_command: str
    oracle_command: str
    image: str
    branch: str
    clone_depth: int
    samples_dir: str
    pinned_sha: str
    candidate_shas: list[str]
    sample_candidates: list[str]
    code_exclude: list[str]
    test_suffixes: list[str]
    generated_names: list[str]
    sql_code_prefixes: list[str]
    sql_test_prefixes: list[str]
    sql_test_suffixes: list[str]


@beartype
def repos_path() -> Path:
    return project_root() / "repos.toml"


@beartype
def load_cores(path: Path | None = None) -> list[CoreConfig]:
    source = repos_path() if path is None else path
    parsed: object = tomllib.loads(source.read_text(encoding="utf-8"))
    root = as_dict(parsed, "repos.toml")
    rows = as_object_list(root.get("cores"), "cores")
    if len(rows) == 0:
        raise ConfigError("repos.toml: список cores пуст")
    cores: list[CoreConfig] = []
    for row in rows:
        cores.append(_core_from_row(as_dict(row, "core")))
    return cores


@beartype
def enabled_cores(cores: list[CoreConfig]) -> list[CoreConfig]:
    return [core for core in cores if core.enabled]


@beartype
def require_core(cores: list[CoreConfig], core_id: str) -> CoreConfig:
    for core in cores:
        if core.id == core_id:
            return core
    raise ConfigError(f"ядро {core_id} не найдено в repos.toml")


@beartype
def _core_from_row(row: dict[str, object]) -> CoreConfig:
    core_id = as_str(row.get("id"), "id")
    enabled = as_bool(row.get("enabled"), f"{core_id}.enabled")
    code_prefixes = as_str_list(row.get("code_prefixes"), f"{core_id}.code_prefixes")
    test_prefixes = as_str_list(row.get("test_prefixes"), f"{core_id}.test_prefixes")
    if enabled and (len(code_prefixes) == 0 or len(test_prefixes) == 0):
        raise ConfigError(f"{core_id}: у включённого ядра пустые префиксы")
    pinned = as_int(row.get("pinned_pr"), f"{core_id}.pinned_pr")
    return CoreConfig(
        id=core_id,
        repository=as_str(row.get("repository"), f"{core_id}.repository"),
        clone_url=as_str(row.get("clone_url"), f"{core_id}.clone_url"),
        license=as_str(row.get("license"), f"{core_id}.license"),
        oracle=as_str(row.get("oracle"), f"{core_id}.oracle"),
        enabled=enabled,
        code_prefixes=code_prefixes,
        test_prefixes=test_prefixes,
        generated_prefixes=as_str_list(
            row.get("generated_prefixes"), f"{core_id}.generated_prefixes"
        ),
        pinned_pr=None if pinned == 0 else pinned,
        oracle_candidate_prs=as_int_list(
            row.get("oracle_candidate_prs"), f"{core_id}.oracle_candidate_prs"
        ),
        build_command=as_str(row.get("build_command"), f"{core_id}.build_command"),
        oracle_command=as_str(row.get("oracle_command"), f"{core_id}.oracle_command"),
        image=as_str(row.get("image"), f"{core_id}.image"),
        branch=_str_or(row, "branch", "main"),
        clone_depth=_int_or(row, "clone_depth", 200),
        samples_dir=_str_or(row, "samples_dir", ""),
        pinned_sha=_str_or(row, "pinned_sha", ""),
        candidate_shas=_str_list_or(row, "candidate_shas"),
        sample_candidates=_str_list_or(row, "sample_candidates"),
        code_exclude=_str_list_or(row, "code_exclude"),
        test_suffixes=_str_list_or(row, "test_suffixes"),
        generated_names=_str_list_or(row, "generated_names"),
        sql_code_prefixes=_str_list_or(row, "sql_code_prefixes"),
        sql_test_prefixes=_str_list_or(row, "sql_test_prefixes"),
        sql_test_suffixes=_str_list_or(row, "sql_test_suffixes"),
    )


@beartype
def _str_or(row: dict[str, object], key: str, default: str) -> str:
    if key not in row:
        return default
    return as_str(row.get(key), key)


@beartype
def _int_or(row: dict[str, object], key: str, default: int) -> int:
    if key not in row:
        return default
    return as_int(row.get(key), key)


@beartype
def _str_list_or(row: dict[str, object], key: str) -> list[str]:
    if key not in row:
        return []
    return as_str_list(row.get(key), key)
