"""Один прогон OpenCode по уже принятому экземпляру.

Процесс стартует на хосте рядом с контейнером и правит смонтированное дерево. Константы: модель ``deepseek/deepseek-v4-flash``, обвязка ``opencode``, температура 0, лимит шагов 50, таймаут 1800 с. Ключ читается из ``OPENROUTER_API_KEY`` в момент запуска и в образ, репозиторий и ``data/`` не пишется.

В промпт входят постановка и ``test_patch``. ``gold_patch`` туда не входит. После прогона тестовые файлы возвращаются к патчу задачи, затем оракул гоняет ``fail_to_pass``. Пустой diff записывается как ``pass`` ложь. ``run_opencode(task, work, data)`` возвращает словарь прогона: модель, обвязка, температура, ``pass``, ``empty_diff``, длительность, лимит шагов, таймаут.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from beartype import beartype

from db_pipeline.config import CoreConfig, load_cores
from db_pipeline.errors import PipelineError
from db_pipeline.git_ops import apply_patch, run_git
from db_pipeline.models import TaskInstance
from db_pipeline.patch_split import paths_in_diff
from db_pipeline.paths import project_root

MODEL_NAME = "deepseek/deepseek-v4-flash"
MODEL_REF = "openrouter/deepseek/deepseek-v4-flash"
HARNESS_NAME = "opencode"
TEMPERATURE = 0.0
STEP_LIMIT = 50
TIMEOUT_SECONDS = 1800


@beartype
def run_opencode(task: TaskInstance, work: Path, data: Path) -> dict[str, object]:
    started = time.monotonic()
    started_at = _now()
    if shutil.which("opencode") is None:
        return _record(task, False, False, started, started_at, "opencode не найден в PATH")
    if os.environ.get("OPENROUTER_API_KEY", "") == "":
        return _record(task, False, False, started, started_at, "OPENROUTER_API_KEY не задан")

    _reset_to_tests(work, task.base_sha, task.test_patch)
    _commit_baseline(work)
    config_path = _write_config(data)
    prompt = _user_prompt(task.statement, task.test_patch)
    env = dict(os.environ)
    env["OPENCODE_CONFIG"] = str(config_path)
    try:
        result = subprocess.run(
            [
                "opencode",
                "run",
                "--pure",
                "--dangerously-skip-permissions",
                "--model",
                MODEL_REF,
                prompt,
            ],
            cwd=work,
            check=False,
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SECONDS,
            env=env,
            stdin=subprocess.DEVNULL,
        )
        log = data / "oracle" / task.head_sha / "opencode.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(result.stdout + "\n" + result.stderr, encoding="utf-8")
        detail = f"код выхода {result.returncode}"
    except subprocess.TimeoutExpired:
        detail = f"таймаут {TIMEOUT_SECONDS} с"
    _restore_tests(work, task.test_patch)
    diff_text = run_git(work, ["diff", "HEAD"])
    empty = diff_text.strip() == ""
    passed = False
    if not empty:
        passed = _tests_pass_after_agent(task, work, data)
    if empty:
        passed = False
    return _record(task, passed, empty, started, started_at, detail)


@beartype
def _reset_to_tests(work: Path, base_sha: str, test_patch: str) -> None:
    # Рабочее дерево — снимок без истории GitHub. Коммит base помечен сообщением.
    run_git(work, ["reset", "--hard", _snapshot_commit(work, base_sha)])
    run_git(work, ["clean", "-fd", "-e", "build"])
    apply_patch(work, test_patch)


@beartype
def _snapshot_commit(work: Path, base_sha: str) -> str:
    needle = f"base {base_sha}"
    output = run_git(work, ["log", "--format=%H%x09%s"])
    for line in output.splitlines():
        sha, separator, subject = line.partition("\t")
        if separator != "" and subject == needle:
            return sha
    raise PipelineError("git", f"нет снимка {needle}")


@beartype
def _commit_baseline(work: Path) -> None:
    # Каталог build/ уже в .gitignore репозитория. Явное исключение
    # заставляет git add завершиться с ошибкой.
    run_git(work, ["add", "-A"])
    env = dict(os.environ)
    env["GIT_AUTHOR_NAME"] = "db-pipeline"
    env["GIT_AUTHOR_EMAIL"] = "db-pipeline@localhost"
    env["GIT_COMMITTER_NAME"] = "db-pipeline"
    env["GIT_COMMITTER_EMAIL"] = "db-pipeline@localhost"
    result = subprocess.run(
        ["git", "-C", str(work), "commit", "--allow-empty", "-m", "test patch"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(detail or "не удалось зафиксировать тестовый патч")


@beartype
def _restore_tests(work: Path, test_patch: str) -> None:
    paths = paths_in_diff(test_patch)
    if len(paths) == 0:
        return
    run_git(work, ["checkout", "HEAD", "--", *paths])


@beartype
def _tests_pass_after_agent(task: TaskInstance, work: Path, data: Path) -> bool:
    from db_pipeline.verify_environment import rerun_tests

    core = _core_for_repository(task.repository)
    return rerun_tests(core, task, work, data)


@beartype
def _core_for_repository(repository: str) -> CoreConfig:
    for core in load_cores():
        if core.repository == repository:
            return core
    raise PipelineError("opencode", f"нет ядра для {repository}")


@beartype
def _write_config(data: Path) -> Path:
    template = (project_root() / "prompts" / "opencode.json").read_text(encoding="utf-8")
    prompt_path = project_root() / "prompts" / "opencode_system.txt"
    rendered = template.replace("PROMPT_PATH", str(prompt_path))
    parsed: object = json.loads(rendered)
    if not isinstance(parsed, dict):
        raise RuntimeError("шаблон OpenCode не объект")
    destination = data / "opencode" / "opencode.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return destination


@beartype
def _user_prompt(statement: str, test_patch: str) -> str:
    return (
        "Problem statement:\n"
        f"{statement}\n\n"
        "Test patch, already applied. Do not modify these files:\n"
        f"{test_patch}\n"
    )


@beartype
def _record(
    task: TaskInstance,
    passed: bool,
    empty_diff: bool,
    started: float,
    started_at: str,
    detail: str,
) -> dict[str, object]:
    return {
        "instance_id": task.instance_id,
        "model": MODEL_NAME,
        "harness": HARNESS_NAME,
        "temperature": TEMPERATURE,
        "pass": passed,
        "empty_diff": empty_diff,
        "duration_seconds": round(time.monotonic() - started, 3),
        "step_limit": STEP_LIMIT,
        "timeout_seconds": TIMEOUT_SECONDS,
        "started_at": started_at,
        "finished_at": _now(),
        "detail": detail,
    }


@beartype
def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
