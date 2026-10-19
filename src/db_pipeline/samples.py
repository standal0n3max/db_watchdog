"""Образцы PostgreSQL из pg-gym: закреплённый запуск читает их вместо истории.

``PostgresSample``: ``name``, ``sha``, ``parent``, ``subject``, ``goal``, ``solution_paths``, ``test_files``, ``oracle_tests``, ``oracle_command``. Каталог задаётся ``samples_dir`` в ``repos.toml`` и режется относительно корня пакета, если путь не абсолютный.

``bundled_git_dir`` ищет bare-клон на три уровня выше каталога образцов: ``pg-gym/stand/pg.git``. ``limit_regress_command`` меняет цель ``check`` на ``check-tests``: в дереве REL_17 ``check`` берёт ``parallel_schedule`` и игнорирует ``TESTS``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from beartype import beartype

from db_pipeline.config import CoreConfig
from db_pipeline.errors import PipelineError
from db_pipeline.parsing import as_dict, as_object_list, as_str, as_str_list, load_json_file
from db_pipeline.paths import project_root


@beartype
@dataclass(frozen=True)
class PostgresSample:
    name: str
    sha: str
    parent: str
    subject: str
    goal: str
    solution_paths: list[str]
    test_files: list[str]
    oracle_tests: list[str]
    oracle_command: str


@beartype
def samples_dir(core: CoreConfig) -> Path:
    if core.samples_dir == "":
        raise PipelineError("samples", f"у ядра {core.id} нет каталога образцов")
    path = Path(core.samples_dir)
    if not path.is_absolute():
        path = project_root() / path
    resolved = path.resolve()
    if not resolved.is_dir():
        raise PipelineError("samples", f"каталог образцов не найден: {resolved}")
    return resolved


@beartype
def bundled_git_dir(core: CoreConfig) -> Path | None:
    # Образцы лежат в pg-gym/suites/commit/data/samples.
    # Bare-клон того же зеркала уже есть в pg-gym/stand/pg.git.
    if core.samples_dir == "":
        return None
    git_dir = samples_dir(core).parents[3] / "stand" / "pg.git"
    if git_dir.exists():
        return git_dir
    return None


@beartype
def load_samples(core: CoreConfig) -> list[PostgresSample]:
    folder = samples_dir(core)
    samples: list[PostgresSample] = []
    for path in sorted(folder.glob("*.json")):
        samples.append(_parse_sample(path))
    if len(samples) == 0:
        raise PipelineError("samples", f"в {folder} нет образцов")
    return samples


@beartype
def sample_by_name(core: CoreConfig, name: str) -> PostgresSample:
    path = samples_dir(core) / f"{name}.json"
    if not path.is_file():
        raise PipelineError("samples", f"образец {name} не найден")
    return _parse_sample(path)


@beartype
def limit_regress_command(command: str) -> str:
    # В дереве REL_17 цель check всегда берёт parallel_schedule и игнорирует TESTS.
    # Один тест запускает check-tests.
    old = "make -C src/test/regress check TESTS="
    new = "make -C src/test/regress check-tests TESTS="
    if command.startswith(old):
        return new + command[len(old) :]
    return command


@beartype
def sample_by_sha(core: CoreConfig, sha: str) -> PostgresSample:
    for sample in load_samples(core):
        if sample.sha == sha:
            return sample
    raise PipelineError("samples", f"образец с sha {sha} не найден")


@beartype
def _parse_sample(path: Path) -> PostgresSample:
    raw = as_dict(load_json_file(path), path.name)
    solution: list[str] = []
    for item in as_object_list(raw.get("solution"), f"{path.name}.solution"):
        row = as_dict(item, "solution")
        solution.append(as_str(row.get("path"), "solution.path"))
    oracle = as_dict(raw.get("oracle"), f"{path.name}.oracle")
    return PostgresSample(
        name=as_str(raw.get("name"), "name"),
        sha=as_str(raw.get("sha"), "sha"),
        parent=as_str(raw.get("parent"), "parent"),
        subject=as_str(raw.get("subject"), "subject"),
        goal=as_str(raw.get("goal"), "goal"),
        solution_paths=solution,
        test_files=as_str_list(raw.get("test_files"), "test_files"),
        oracle_tests=as_str_list(oracle.get("tests"), "oracle.tests"),
        oracle_command=as_str(oracle.get("command"), "oracle.command"),
    )
