"""Конвертер watchdog → commit-suite.

Берёт карточки ``data/tasks/postgres__*.json`` (``TaskInstance``) и раскладывает
их в протокол commit-suite: публичная задача, приватный oracle и reverse
source patch.

Точка отсчёта — ``head_sha``: на ней дерево уже содержит баг, поэтому reverse
``gold_patch`` делает исходник buggy, а ``test_patch`` несёт скрытый тест.
``base_sha`` остаётся границей отсечки графа.

Проверки конвертера (``validate``): публичный JSON не содержит test/gold/reference;
reverse patch на ``head_sha`` даёт buggy source; тестовый патч соответствует
``graded_by``; oracle принят текущим журналом.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark import paths
from db_pipeline.benchmark import tasks as task_rules
from db_pipeline.benchmark.models import Oracle, PublicTask
from db_pipeline.errors import PipelineError
from db_pipeline.models import TaskInstance, task_from_dict
from db_pipeline.parsing import as_dict, load_json_file


@beartype
def load_instances(data: Path | None = None) -> list[TaskInstance]:
    folder = paths.resolve_data(data) / "tasks"
    instances: list[TaskInstance] = []
    for path in sorted(folder.glob("postgres__*.json")):
        if "reject" in path.stem:
            continue
        raw = load_json_file(path)
        instance = task_from_dict(raw)
        if instance.reject_reason != "":
            continue
        instances.append(instance)
    return instances


@beartype
def convert_one(instance: TaskInstance, data: Path | None = None) -> dict[str, object]:
    if instance.head_sha == "" or instance.base_sha == "":
        raise PipelineError("convert", f"{instance.instance_id}: пустые head/base sha")
    if instance.gold_patch.strip() == "":
        raise PipelineError("convert", f"{instance.instance_id}: пустой gold_patch")
    assert_on_master(instance.base_sha, instance.instance_id, data)

    name = func_name(instance)
    statement = task_rules.clean_statement(instance.statement)
    subject = task_rules.statement_subject(instance.statement)
    paths_touched = source_paths(instance.gold_patch)
    area = task_rules.derive_area(subject, paths_touched)
    contract = task_rules.extract_contract(instance.gold_patch, instance.test_patch)
    graded_by = task_rules.graded_files(test_files(instance.test_patch))
    expected = list(instance.fail_to_pass)

    public = PublicTask(
        func=name,
        area=area,
        goal=statement,
        contract=contract,
        graded_by=graded_by,
    )
    oracle = Oracle(
        func=name,
        head=instance.head_sha,
        base=instance.base_sha,
        source="postgres/postgres",
        patch=f"prep/{name}.patch",
        patch_text=instance.gold_patch,
        test=instance.test_patch,
        test_files=test_files(instance.test_patch),
        reference=instance.gold_patch,
        oracle_command=instance.oracle_command,
        expected_tests=expected,
    )
    return {"public": public, "oracle": oracle}


@beartype
def func_name(instance: TaskInstance) -> str:
    head = instance.head_sha[:12]
    return f"postgres__{head}"


@beartype
def source_paths(patch: str) -> list[str]:
    found: list[str] = []
    for chunk in patch.split("diff --git ")[1:]:
        header = chunk.splitlines()[0]
        rel = _path_from_header("diff --git " + header)
        if rel != "" and not task_rules.is_test_source(rel):
            found.append(rel)
    return found


@beartype
def test_files(test_patch: str) -> list[str]:
    found: list[str] = []
    for chunk in test_patch.split("diff --git ")[1:]:
        header = chunk.splitlines()[0]
        rel = _path_from_header("diff --git " + header)
        if rel != "":
            found.append(rel)
    return found


@beartype
def _path_from_header(header: str) -> str:
    marker = "diff --git a/"
    if not header.startswith(marker):
        return ""
    rest = header[len(marker) :]
    rel, separator, _ = rest.partition(" b/")
    if separator == "":
        return rest
    return rel


@beartype
def write_task(payload: dict[str, object], data: Path | None = None) -> dict[str, str]:
    public = payload["public"]
    oracle = payload["oracle"]
    if not isinstance(public, PublicTask) or not isinstance(oracle, Oracle):
        raise PipelineError("convert", "неверный payload конвертера")

    public_path = paths.tasks_dir(data) / f"{public.func}.json"
    oracle_path = paths.oracle_dir(data) / f"{public.func}.json"
    patch_path = paths.prep_dir(data) / f"{public.func}.patch"

    patch_path.write_text(oracle.patch_text, encoding="utf-8")
    public_path.write_text(
        json.dumps(public.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    oracle_path.write_text(
        json.dumps(oracle.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return {
        "func": public.func,
        "public": str(public_path),
        "oracle": str(oracle_path),
        "patch": str(patch_path),
    }


@beartype
def load_public(name: str, data: Path | None = None) -> dict[str, object]:
    raw = as_dict(load_json_file(paths.tasks_dir(data) / f"{name}.json"), "public")
    return raw


@beartype
def load_oracle(name: str, data: Path | None = None) -> dict[str, object]:
    return as_dict(load_json_file(paths.oracle_dir(data) / f"{name}.json"), "oracle")


@beartype
def assert_on_master(base_sha: str, label: str, data: Path | None = None) -> None:
    """База задачи обязана быть предком ``origin/master``.

    Коммит, которого нет в клоне (синтетический sha в тестах), не проверяется.
    Существующий коммит вне master в датасет не попадает: одна такая база
    (``postgres__04c25af21c02``, только ``REL_17_STABLE``) из датасета убрана.
    """
    repo = paths.pg_clone(data)
    git_dir = repo / ".git"
    if not git_dir.exists():
        return
    exists = subprocess.run(
        ["git", "-C", str(repo), "cat-file", "-e", f"{base_sha}^{{commit}}"],
        capture_output=True,
        check=False,
    )
    if exists.returncode != 0:
        return
    ancestor = subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", base_sha, "origin/master"],
        capture_output=True,
        check=False,
    )
    if ancestor.returncode != 0:
        raise PipelineError(
            "convert",
            f"{label}: base {base_sha} не предок origin/master",
        )
