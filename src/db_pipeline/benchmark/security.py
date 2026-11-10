"""Проверки конвертера и gate-ы anti-leak границы.

``validate_source`` работает на хосте с локальным клоном PostgreSQL: reverse
patch на ``head_sha`` даёт buggy source, тестовый патч применяется поверх,
а ``graded_by`` соответствует файлам теста.

``validate_public`` проверяет, что публичная задача не содержит эталона,
тестов или ссылок на них.

Gate-ы прогона (``gates``) повторяют scoring commit-suite: непустой source diff,
отсутствие правок тестов, отсутствие пересозданных скрытых тестов.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark import tasks as task_rules
from db_pipeline.benchmark.models import GateResult, as_str_list
from db_pipeline.errors import PipelineError

TEST_ROOT = "src/test/"

_TEST_SOURCE_SUFFIXES = (".sql", ".out", ".spec")
_TEST_SOURCE_DIRS = ("/sql/", "/expected/", "/specs/")
_TEST_PLAN_NAMES = ("Makefile", "GNUmakefile", "meson.build")
_TEST_PLAN_SUFFIXES = (".pl", ".pm", ".py", ".sh")


@beartype
def is_test_source(rel: str) -> bool:
    return rel.endswith(_TEST_SOURCE_SUFFIXES) and any(
        part in rel for part in _TEST_SOURCE_DIRS
    )


@beartype
def is_test_plan(rel: str) -> bool:
    name = rel.rsplit("/", 1)[-1]
    return (
        name.endswith("_schedule")
        or name in _TEST_PLAN_NAMES
        or rel.endswith(_TEST_PLAN_SUFFIXES)
    )


@beartype
def is_test_area(rel: str) -> bool:
    return rel.startswith(TEST_ROOT)


@beartype
def is_protected(rel: str) -> bool:
    return is_test_area(rel) and (is_test_source(rel) or is_test_plan(rel))


@beartype
@dataclass(frozen=True)
class SourceCheck:
    ok: bool
    detail: str

    def to_dict(self) -> dict[str, object]:
        return {"ok": self.ok, "detail": self.detail}


@beartype
def validate_public(name: str, data: Path | None = None) -> SourceCheck:
    from db_pipeline.benchmark import convert

    public = convert.load_public(name, data)
    text = json.dumps(public, ensure_ascii=False)
    if "diff --git" in text or "+++ b/" in text or "@@" in text:
        return SourceCheck(False, "публичная задача содержит фрагмент diff")
    allowed = {"func", "area", "goal", "contract", "graded_by"}
    extra = sorted(set(public) - allowed)
    if extra:
        return SourceCheck(False, f"публичная задача содержит лишние поля: {extra}")
    for key in ("goal", "area", "graded_by"):
        if key not in public:
            return SourceCheck(False, f"нет поля {key}")
    return SourceCheck(True, "публичная часть без эталона и тестов")


@beartype
def validate_source(
    name: str,
    data: Path | None = None,
    clone: Path | None = None,
) -> SourceCheck:
    repo = clone if clone is not None else _default_clone(data)
    if not (repo / ".git").exists():
        return SourceCheck(False, f"нет клона {repo}")

    public = load_public_raw(name, data)
    oracle = load_oracle_raw(name, data)
    head = str(oracle["head"])
    reverse = str(oracle["reverse_patch"])
    test = str(oracle["test"])
    graded = as_str_list(public.get("graded_by"))

    tmp = Path(tempfile.mkdtemp(prefix="bench-validate-"))
    try:
        if not _install_head(repo, head, tmp):
            return SourceCheck(False, f"не удалось развернуть head {head[:12]}")
        # head_sha содержит и фикс, и тест. Reverse source patch даёт buggy source,
        # reverse test patch возвращает дерево к состоянию до теста.
        reverted = _apply(tmp, reverse, reverse=True)
        if not reverted[0]:
            return SourceCheck(False, f"reverse source patch не применился: {reverted[1][:200]}")
        reverted_test = _apply(tmp, test, reverse=True)
        if not reverted_test[0]:
            return SourceCheck(False, f"reverse test patch не применился: {reverted_test[1][:200]}")

        if task_rules.graded_files(graded) != graded:
            return SourceCheck(False, "graded_by содержит не тестовый файл")
        return SourceCheck(True, "reverse patch даёт buggy source, тест снимается")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@beartype
def _default_clone(data: Path | None) -> Path:
    from db_pipeline.benchmark import paths

    return paths.pg_clone(data)


@beartype
def _install_head(repo: Path, head: str, target: Path) -> bool:
    init = subprocess.run(
        ["git", "init", "-q"],
        cwd=target,
        check=False,
        capture_output=True,
        text=True,
    )
    if init.returncode != 0:
        return False
    checkout = subprocess.run(
        [
            "git",
            f"--git-dir={repo / '.git'}",
            f"--work-tree={target}",
            "checkout",
            "-f",
            head,
            "--",
            ".",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    return checkout.returncode == 0


@beartype
def _apply(target: Path, patch: str, reverse: bool = False) -> tuple[bool, str]:
    if patch.strip() == "":
        return False, "пустой патч"
    args = ["git", "apply", "--whitespace=nowarn"]
    if reverse:
        args.append("-R")
    args.append("-")
    result = subprocess.run(
        args,
        cwd=target,
        input=patch,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0, (result.stderr or result.stdout)


@beartype
def load_public_raw(name: str, data: Path | None = None) -> dict[str, object]:
    from db_pipeline.benchmark import convert

    return convert.load_public(name, data)


@beartype
def load_oracle_raw(name: str, data: Path | None = None) -> dict[str, object]:
    from db_pipeline.benchmark import convert

    return convert.load_oracle(name, data)


@beartype
def gates(changed: list[str], recreated: list[str]) -> dict[str, GateResult]:
    violations = sorted({rel for rel in changed if is_protected(rel)} | set(recreated))
    substantive = [rel for rel in changed if not is_test_area(rel)]
    return {
        "nonempty_diff": GateResult(
            ok=bool(substantive) or bool(recreated),
            detail=f"changed={len(changed)}",
        ),
        "no_test_edits": GateResult(
            ok=not violations,
            detail=", ".join(violations),
        ),
        "no_recreated_tests": GateResult(
            ok=not recreated,
            detail=", ".join(sorted(recreated)),
        ),
    }


@beartype
def verdict(gates_result: dict[str, GateResult], target_ok: bool, noregress_ok: bool) -> bool:
    if not all(gate.ok for gate in gates_result.values()):
        return False
    return target_ok and noregress_ok


@beartype
def require_ok(check: SourceCheck, name: str) -> None:
    if not check.ok:
        raise PipelineError("validate", f"{name}: {check.detail}")
