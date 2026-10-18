from __future__ import annotations

from beartype import beartype

from db_pipeline.config import CoreConfig
from db_pipeline.models import ChangeView, StatementSource, TaskInstance
from db_pipeline.errors import PipelineError
from db_pipeline.patch_split import paths_in_diff, regress_test_names, split_by_paths, split_regress_diff, split_unified_diff


@beartype
def compile_task(
    change: ChangeView,
    core: CoreConfig,
    diff_text: str,
    statement: str,
    statement_source: StatementSource,
    gold_paths: list[str] | None = None,
    test_paths: list[str] | None = None,
    fail_to_pass: list[str] | None = None,
    oracle_command: str = "",
    regress_split: bool = False,
) -> TaskInstance:
    if regress_split:
        test_patch, gold_patch = split_regress_diff(diff_text)
        names = regress_test_names(test_patch)
        if len(names) == 0:
            raise PipelineError("compile", "в патче регресса нет expected/*.out")
    elif gold_paths is not None and test_paths is not None:
        test_patch, gold_patch = split_by_paths(diff_text, gold_paths, test_paths)
        names = fail_to_pass if fail_to_pass is not None else _fail_names(test_patch, core)
    else:
        test_patch, gold_patch = split_unified_diff(
            diff_text,
            core.code_prefixes,
            core.test_prefixes,
            core.test_suffixes,
            core.code_exclude,
        )
        names = fail_to_pass if fail_to_pass is not None else _fail_names(test_patch, core)
    return TaskInstance(
        instance_id=instance_id(core, change),
        repository=core.repository,
        license=core.license,
        base_sha=change.base_sha,
        statement=statement,
        statement_source=statement_source,
        test_patch=test_patch,
        gold_patch=gold_patch,
        fail_to_pass=names,
        pass_to_pass=[],
        oracle=core.oracle,
        reject_reason="",
        head_sha=change.sha,
        pr_number=change.pr_number,
        oracle_command=oracle_command,
    )


@beartype
def _fail_names(test_patch: str, core: CoreConfig) -> list[str]:
    names: list[str] = []
    for path in paths_in_diff(test_patch):
        if len(core.test_suffixes) > 0:
            if any(path.endswith(suffix) for suffix in core.test_suffixes):
                names.append(path)
            continue
        if (
            path.endswith(".test")
            or path.endswith(".test_slow")
            or path.endswith(".cpp")
            or path.endswith(".py")
        ):
            names.append(path)
    return names


@beartype
def instance_id(core: CoreConfig, change: ChangeView) -> str:
    repo = core.repository.replace("/", "__")
    if change.pr_number is not None:
        return f"{repo}-{change.pr_number}"
    return f"{repo}-{change.sha[:12]}"
