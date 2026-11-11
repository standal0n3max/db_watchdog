"""Действия над деревом PostgreSQL внутри task-container.

Команды выполняются через ``docker exec``: применение и откат патчей, обрыв
истории до одного baseline-коммита, скрытие и восстановление тестовых файлов,
build/install и запуск регресса. Разбор вывода ``pg_regress`` вынесен в чистые
функции и проверяется тестами.

История обрывается после применения reverse patch и prebuild: ``.git``
заменяется новым репозиторием с одним baseline-коммитом, без remotes и reflog,
поэтому ``git show <head_sha>`` больше не находит эталон.
"""

from __future__ import annotations

import re
import shlex
import subprocess

from beartype import beartype

from db_pipeline.errors import PipelineError

WORKSPACE = "/workspace/postgres"
PREFIX = "/workspace/pg-install"
BENCH_USER = "bench"

TEST_SOURCE_SUFFIXES = (".sql", ".out", ".spec")

_FAILED_RE = re.compile(
    r"^not ok\s+\d+\s*[-+]?\s*(\S+)|^(?:test\s+)?(\S+)\s+\.\.\..*\bFAILED\b",
    re.MULTILINE,
)
_SUMMARY_RE = re.compile(r"(\d+) of (\d+) tests failed|All (\d+) tests passed")
_TAP_RE = re.compile(r"^(not )?ok\s+\d+\s*[-+]?\s*(\S+)(.*)$", re.MULTILINE)
_LEGACY_RE = re.compile(
    r"^(?:test\s+)?(\S+)\s+\.\.\.\s*(ok|FAILED|failed)\b", re.MULTILINE
)


@beartype
def run_in_container(
    container_id: str,
    script: str,
    timeout: int = 1800,
    cwd: str = WORKSPACE,
    user: str = BENCH_USER,
) -> tuple[int, str]:
    wrapped = f"cd {shlex.quote(cwd)} && {script}"
    command = ["docker", "exec", "-i"]
    if user != "":
        command += ["-u", user]
    command += [container_id, "sh", "-lc", wrapped]
    # ``stdin=DEVNULL`` обязателен: без него ``docker exec -i`` наследует
    # stdin родителя. Если родитель запущен под pty или незакрытым pipe,
    # stdin не даёт EOF, и ``opencode run`` (кормится промптом из stdin,
    # когда аргумент-промпт не распознан) зависает навсегда — прогон
    # заканчивается ``crash_unknown`` по таймауту вместо ответа модели.
    result = subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    return result.returncode, (result.stdout + result.stderr)


@beartype
def write_file(container_id: str, path: str, data: bytes, user: str = "") -> None:
    script = 'mkdir -p "$(dirname "$1")" && cat > "$1"'
    command = ["docker", "exec", "-i"]
    if user != "":
        command += ["-u", user]
    command += [container_id, "sh", "-c", script, "sh", path]
    result = subprocess.run(
        command,
        input=data,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace").strip()
        raise PipelineError("docker_exec", detail or f"не удалось записать {path}")


@beartype
def apply_patch(container_id: str, patch_text: str, reverse: bool = False) -> tuple[bool, str]:
    if patch_text.strip() == "":
        return False, "пустой патч"
    flag = " -R" if reverse else ""
    file = "/work/_patch.diff"
    write_file(container_id, file, patch_text.encode("utf-8"))
    code, output = run_in_container(
        container_id, f"git apply --whitespace=nowarn{flag} {file}", timeout=300
    )
    return code == 0, output


@beartype
def hide_tests(container_id: str, paths: list[str]) -> list[str]:
    hidden: list[str] = []
    for rel in paths:
        quoted = shlex.quote(rel)
        script = (
            f"name=$(echo {quoted} | tr / _); "
            f"if [ -f {quoted} ]; then mkdir -p /work/hidden; "
            f"mv {quoted} /work/hidden/$name && echo hid; fi"
        )
        code, output = run_in_container(container_id, script, timeout=120)
        if code == 0 and "hid" in output:
            hidden.append(rel)
    return hidden


@beartype
def remove_tests(container_id: str, paths: list[str]) -> list[str]:
    removed: list[str] = []
    for rel in paths:
        quoted = shlex.quote(rel)
        code, output = run_in_container(
            container_id,
            f"if [ -f {quoted} ]; then rm -f {quoted} && echo gone; fi",
            timeout=120,
        )
        if code == 0 and "gone" in output:
            removed.append(rel)
    return removed


@beartype
def restore_tests(container_id: str, paths: list[str]) -> list[str]:
    restored: list[str] = []
    for rel in paths:
        quoted = shlex.quote(rel)
        script = (
            f"name=$(echo {quoted} | tr / _); "
            f"if [ -f /work/hidden/$name ]; then mkdir -p $(dirname {quoted}); "
            f"mv /work/hidden/$name {quoted} && echo back; fi"
        )
        code, output = run_in_container(container_id, script, timeout=120)
        if code == 0 and "back" in output:
            restored.append(rel)
    return restored


@beartype
def sever_history(container_id: str) -> tuple[bool, str]:
    script = (
        "rm -rf .git && git init -q -b bench-baseline && "
        "git config user.name bench && git config user.email bench@localhost && "
        "git config gc.auto 0 && "
        "git add -A && git commit -qm 'bench: task baseline' && "
        "git reflog expire --expire=now --all && "
        "git gc --prune=now --force -q && echo severed"
    )
    code, output = run_in_container(container_id, script, timeout=600)
    return code == 0 and "severed" in output, output


@beartype
def git_state(container_id: str) -> dict[str, object]:
    script = (
        "echo commits=$(git rev-list --all --count); "
        "echo remotes=$(git remote | wc -l | tr -d ' '); "
        "echo reflog=$(git reflog | wc -l | tr -d ' '); "
        "echo head=$(git rev-parse HEAD)"
    )
    code, output = run_in_container(container_id, script, timeout=120)
    state: dict[str, object] = {"ok": code == 0, "raw": output.strip()}
    for line in output.splitlines():
        key, separator, value = line.partition("=")
        if separator == "":
            continue
        if key in ("commits", "remotes", "reflog"):
            try:
                state[key] = int(value.strip())
            except ValueError:
                state[key] = value.strip()
        elif key == "head":
            state[key] = value.strip()
    return state


@beartype
def changed_files(container_id: str) -> list[str]:
    code, output = run_in_container(container_id, "git status --porcelain", timeout=120)
    if code != 0:
        return []
    found: list[str] = []
    for line in output.splitlines():
        rel = line[3:].strip()
        if rel != "":
            found.append(rel)
    return sorted(set(found))


@beartype
def worktree_diff(container_id: str) -> str:
    code, output = run_in_container(container_id, "git diff HEAD", timeout=180)
    return output if code == 0 else ""


@beartype
def build(container_id: str, timeout: int = 3600) -> tuple[bool, str]:
    code, output = run_in_container(container_id, "make -j$(nproc)", timeout=timeout)
    return code == 0, output


@beartype
def install(container_id: str, timeout: int = 1800) -> tuple[bool, str]:
    code, output = run_in_container(container_id, "make install", timeout=timeout)
    return code == 0, output


@beartype
def configure(container_id: str, timeout: int = 1800) -> tuple[bool, str]:
    command = (
        f"./configure --prefix={PREFIX} --enable-cassert --enable-debug "
        "--with-icu --without-readline"
    )
    code, output = run_in_container(container_id, command, timeout=timeout)
    return code == 0, output


@beartype
def check(container_id: str, tests: list[str] | None = None, timeout: int = 1800) -> tuple[int, str]:
    # В дереве REL_17 цель ``check`` берёт parallel_schedule и игнорирует TESTS:
    # один или несколько тестов запускает ``check-tests``. ``parallel_schedule``
    # создаёт общие таблицы в ``test_setup``, поэтому одиночный тест без него
    # падает с «relation does not exist» и до эталона, и после.
    if tests:
        selected = with_setup(tests)
        command = f'cd src/test/regress && make check-tests TESTS="{" ".join(selected)}"'
    else:
        command = "cd src/test/regress && make check"
    return run_in_container(container_id, command, timeout=timeout, cwd=WORKSPACE)


@beartype
def with_setup(tests: list[str]) -> list[str]:
    if "test_setup" in tests:
        return tests
    return ["test_setup", *tests]


@beartype
def parse_failed(output: str) -> list[str]:
    found: set[str] = set()
    for tap, legacy in _FAILED_RE.findall(output):
        name = tap or legacy
        if name:
            found.add(name)
    return sorted(found)


@beartype
def parse_total(output: str) -> int:
    match = _SUMMARY_RE.search(output)
    if match is None:
        return 0
    return int(match.group(2) or match.group(3) or 0)


@beartype
def parse_statuses(output: str) -> dict[str, str]:
    found: dict[str, str] = {}
    for negated, name, rest in _TAP_RE.findall(output):
        if negated:
            found[name] = "failed"
        elif "# SKIP" in rest.upper():
            found[name] = "skipped"
        else:
            found[name] = "passed"
    for name, verdict in _LEGACY_RE.findall(output):
        found[name] = "passed" if verdict == "ok" else "failed"
    return found


@beartype
def is_test_source(rel: str) -> bool:
    return rel.endswith(TEST_SOURCE_SUFFIXES)


@beartype
def require_ok(ok: bool, stage: str, output: str) -> None:
    if not ok:
        raise PipelineError(stage, output[-2000:] or f"шаг {stage} не удался")
