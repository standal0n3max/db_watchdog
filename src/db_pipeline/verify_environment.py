"""Оракул: годится ли коммит в датасет и верен ли ответ агента.

Экземпляр принимается, если тесты из ``fail_to_pass`` падают на ``base_sha`` с ``test_patch`` и проходят после ``gold_patch``. ``verify_task(core, task, clone, data)`` выбирает программу по id ядра. DuckDB — бинарник ``build/release/test/unittest``. PostgreSQL и SQLite идут через общий ``_verify_shell``: сборка ``./configure --without-readline && make -j3`` или ``./configure && make testfixture``, команда теста с подстановкой ``{test}``. Таймауты сборки и теста — константы этого модуля, у каждого ядра свои.

В образе только инструменты. Исходники ``base_sha`` монтируются снаружи. После первой успешной сборки команда дописывается в ``repos.toml``. У PostgreSQL перед вторым прогоном сносятся ``tmp_install`` и ``tmp_check``: иначе postmaster поднимается на каталоге данных прогона «до эталона».

``rerun_tests`` проверяет diff агента по уже принятой задаче и в отбор коммита не входит.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from beartype import beartype

from db_pipeline.config import CoreConfig, repos_path
from db_pipeline.decisions import append_decision
from db_pipeline.errors import PipelineError
from db_pipeline.git_ops import apply_patch, checkout_local
from db_pipeline.models import TaskInstance
from db_pipeline.paths import project_root
from db_pipeline.samples import limit_regress_command

BUILD_TIMEOUT_SECONDS = 5400
TEST_TIMEOUT_SECONDS = 600
IMAGE_TIMEOUT_SECONDS = 1800
POSTGRES_BUILD_TIMEOUT_SECONDS = 7200
POSTGRES_TEST_TIMEOUT_SECONDS = 1800
SQLITE_BUILD_TIMEOUT_SECONDS = 3600
SQLITE_TEST_TIMEOUT_SECONDS = 900
# Команда из Makefile репозитория. В repos.toml попадает только после того,
# как сборка реально подняла бинарник тестов.
# Unity-сборка — штатный режим DuckDB: без неё GCC 13 не собирает sort.
# Три компилятора при 16 ГиБ Docker; линковка остаётся однопоточной.
# DISABLE_PARQUET: тесты кандидатов его не требуют.
DEFAULT_BUILD_COMMAND = (
    "CMAKE_BUILD_PARALLEL_LEVEL=3 make release DISABLE_SHELL=1 DISABLE_PARQUET=1 "
    "CMAKE_VARS_BUILD=-DDUCKDB_RELEASE_LINK_JOBS=1"
)
POSTGRES_BUILD_COMMAND = "./configure --without-readline && make -j3"
POSTGRES_ORACLE_TEMPLATE = "make -C src/test/regress check TESTS={test}"
SQLITE_BUILD_COMMAND = "./configure && make testfixture"
SQLITE_ORACLE_TEMPLATE = "./testfixture {test}"
POSTMASTER_REASON = "postmaster не стартовал"
# Каталоги, которые make check оставляет в дереве. Второй прогон иначе
# поднимает postmaster на каталоге данных от прогона «до эталона».
POSTGRES_LEFTOVER_DIRS = "tmp_install src/test/regress/tmp_check src/test/regress/log"
POSTGRES_STARTUP_LOGS = (
    "src/test/regress/log/postmaster.log",
    "tmp_install/log/initdb-template.log",
)


@beartype
@dataclass(frozen=True)
class OracleResult:
    status: str
    reason_code: str
    reason: str
    before_log: str
    after_log: str
    build_log: str


@beartype
def verify_task(core: CoreConfig, task: TaskInstance, clone: Path, data: Path) -> OracleResult:
    if core.id == "duckdb":
        return _verify_duckdb(core, task, clone, data)
    if core.id == "postgres":
        return _verify_shell(
            core,
            task,
            clone,
            data,
            POSTGRES_BUILD_COMMAND,
            POSTGRES_ORACLE_TEMPLATE,
            POSTGRES_BUILD_TIMEOUT_SECONDS,
            POSTGRES_TEST_TIMEOUT_SECONDS,
            None,
            True,
        )
    if core.id == "sqlite":
        return _verify_shell(
            core,
            task,
            clone,
            data,
            SQLITE_BUILD_COMMAND,
            SQLITE_ORACLE_TEMPLATE,
            SQLITE_BUILD_TIMEOUT_SECONDS,
            SQLITE_TEST_TIMEOUT_SECONDS,
            "testfixture",
            False,
        )
    return _rejected("oracle", f"нет оракула для {core.id}", "", "", "")


@beartype
def rerun_tests(core: CoreConfig, task: TaskInstance, work: Path, data: Path) -> bool:
    if core.id == "duckdb":
        build_command = core.build_command if core.build_command != "" else DEFAULT_BUILD_COMMAND
        rebuild_log = data / "oracle" / task.head_sha / "agent_build.log"
        build_code = docker_bash(core.image, work, build_command, rebuild_log, BUILD_TIMEOUT_SECONDS, None, False)
        if build_code != 0:
            return False
        binary = find_test_binary(work)
        if binary is None:
            return False
        log_path = data / "oracle" / task.head_sha / "agent.log"
        return run_tests(core.image, work, binary, task.fail_to_pass, log_path) == 0
    command = _test_command(core, task)
    build_command = _build_command(core)
    timeout = POSTGRES_BUILD_TIMEOUT_SECONDS if core.id == "postgres" else SQLITE_BUILD_TIMEOUT_SECONDS
    test_timeout = POSTGRES_TEST_TIMEOUT_SECONDS if core.id == "postgres" else SQLITE_TEST_TIMEOUT_SECONDS
    rebuild_log = data / "oracle" / task.head_sha / "agent_build.log"
    drop = core.id == "postgres"
    build_code = docker_bash(
        core.image,
        work,
        incremental_build_command(work, build_command),
        rebuild_log,
        timeout,
        None,
        not drop,
        drop,
    )
    if build_code != 0:
        return False
    log_path = data / "oracle" / task.head_sha / "agent.log"
    return _run_oracle(core.image, work, command, task.fail_to_pass, log_path, test_timeout, drop) == 0


@beartype
def _verify_duckdb(core: CoreConfig, task: TaskInstance, clone: Path, data: Path) -> OracleResult:
    if shutil.which("docker") is None:
        return _rejected("docker_missing", "docker не найден в PATH", "", "", "")
    if len(task.fail_to_pass) == 0:
        return _rejected("no_tests", "в test_patch нет файлов тестов", "", "", "")

    dockerfile = _dockerfile(core)
    image_ok, image_log = ensure_tools_image(core.image, dockerfile)
    if not image_ok:
        _note(data, core, "сборка образа инструментов не удалась", image_log)
        return _rejected("image_build_failed", "образ инструментов не собрался", "", "", image_log)

    work = data / "work" / task.head_sha
    logs = data / "oracle" / task.head_sha
    logs.mkdir(parents=True, exist_ok=True)
    if work.exists():
        shutil.rmtree(work)
    try:
        checkout_local(clone, work, task.base_sha)
        apply_patch(work, task.test_patch)
    except PipelineError as exc:
        return _rejected(exc.reason_code, exc.reason, "", "", "")

    build_command = core.build_command if core.build_command != "" else DEFAULT_BUILD_COMMAND
    build_log = logs / "build.log"
    build_code = docker_bash(core.image, work, build_command, build_log, BUILD_TIMEOUT_SECONDS, None, False)
    if build_code != 0:
        _note(data, core, f"сборка ядра завершилась с кодом {build_code}", _tail(build_log))
        return _rejected(
            "build_failed",
            f"сборка ядра завершилась с кодом {build_code}",
            "",
            "",
            str(build_log),
        )

    binary = find_test_binary(work)
    if binary is None:
        _note(data, core, "после сборки нет бинарника тестов", _tail(build_log))
        return _rejected("oracle_binary_missing", "бинарник тестов не найден", "", "", str(build_log))

    oracle_command = f"{binary} {{test}}"
    freeze_commands(core.id, build_command, oracle_command)
    before_log = logs / "before.log"
    before_code = run_tests(core.image, work, binary, task.fail_to_pass, before_log)
    if before_code == 0:
        return _rejected(
            "tests_already_passing",
            "тесты из test_patch прошли до эталона",
            str(before_log),
            "",
            str(build_log),
        )

    try:
        apply_patch(work, task.gold_patch)
    except PipelineError as exc:
        return _rejected(exc.reason_code, exc.reason, str(before_log), "", str(build_log))

    rebuild_log = logs / "rebuild.log"
    rebuild_code = docker_bash(core.image, work, build_command, rebuild_log, BUILD_TIMEOUT_SECONDS, None, False)
    if rebuild_code != 0:
        _note(data, core, f"пересборка после эталона завершилась с кодом {rebuild_code}", _tail(rebuild_log))
        return _rejected(
            "rebuild_failed",
            f"пересборка после эталона завершилась с кодом {rebuild_code}",
            str(before_log),
            "",
            str(rebuild_log),
        )

    after_log = logs / "after.log"
    after_code = run_tests(core.image, work, binary, task.fail_to_pass, after_log)
    if after_code != 0:
        return _rejected(
            "gold_did_not_fix",
            "эталон не починил падение тестов",
            str(before_log),
            str(after_log),
            str(rebuild_log),
        )
    append_decision(
        data,
        f"\n## Оракул принял {task.instance_id}\n\n"
        f"Сборка: `{build_command}`\n\n"
        f"Тест: `{oracle_command}`\n",
    )
    return OracleResult(
        "accepted",
        "accepted",
        "тесты упали до эталона и прошли после",
        str(before_log),
        str(after_log),
        str(build_log),
    )


@beartype
def _verify_shell(
    core: CoreConfig,
    task: TaskInstance,
    clone: Path,
    data: Path,
    default_build: str,
    oracle_template: str,
    build_timeout: int,
    test_timeout: int,
    marker: str | None,
    drop_privileges: bool,
) -> OracleResult:
    if shutil.which("docker") is None:
        return _rejected("docker_missing", "docker не найден в PATH", "", "", "")
    if len(task.fail_to_pass) == 0:
        return _rejected("no_tests", "в экземпляре нет тестов оракула", "", "", "")
    if task.oracle_command == "" and "{test}" not in oracle_template:
        return _rejected("no_tests", "нет команды оракула", "", "", "")

    dockerfile = _dockerfile(core)
    image_ok, image_log = ensure_tools_image(core.image, dockerfile)
    if not image_ok:
        _note(data, core, "сборка образа инструментов не удалась", image_log)
        return _rejected("image_build_failed", "образ инструментов не собрался", "", "", image_log)

    work = data / "work" / task.head_sha
    logs = data / "oracle" / task.head_sha
    logs.mkdir(parents=True, exist_ok=True)
    if work.exists():
        shutil.rmtree(work)
    try:
        checkout_local(clone, work, task.base_sha)
        apply_patch(work, task.test_patch)
    except PipelineError as exc:
        return _rejected(exc.reason_code, exc.reason, "", "", "")

    build_command = core.build_command if core.build_command != "" else default_build
    build_log = logs / "build.log"
    build_code = docker_bash(
        core.image,
        work,
        incremental_build_command(work, build_command),
        build_log,
        build_timeout,
        None,
        not drop_privileges,
        drop_privileges,
    )
    if build_code != 0:
        _note(data, core, f"сборка ядра завершилась с кодом {build_code}", _tail(build_log))
        return _rejected(
            "build_failed",
            f"сборка ядра завершилась с кодом {build_code}",
            "",
            "",
            str(build_log),
        )
    if marker is not None and not (work / marker).is_file():
        _note(data, core, "после сборки нет бинарника тестов", _tail(build_log))
        return _rejected("oracle_binary_missing", "бинарник тестов не найден", "", "", str(build_log))

    test_command = oracle_shell_command(core, task, oracle_template)
    freeze_commands(core.id, build_command, oracle_template if core.id == "sqlite" else _postgres_freeze(task, oracle_template))
    before_log = logs / "before.log"
    before_code = _run_oracle(
        core.image, work, test_command, task.fail_to_pass, before_log, test_timeout, drop_privileges
    )
    if _startup_failed(core.id, work, before_log):
        return _rejected("postmaster_failed", POSTMASTER_REASON, str(before_log), "", str(build_log))
    if before_code == 0:
        return _rejected(
            "tests_already_passing",
            "тесты прошли до эталона",
            str(before_log),
            "",
            str(build_log),
        )

    try:
        apply_patch(work, task.gold_patch)
    except PipelineError as exc:
        return _rejected(exc.reason_code, exc.reason, str(before_log), "", str(build_log))

    rebuild_log = logs / "rebuild.log"
    rebuild_code = docker_bash(
        core.image,
        work,
        incremental_build_command(work, build_command),
        rebuild_log,
        build_timeout,
        None,
        not drop_privileges,
        drop_privileges,
    )
    if rebuild_code != 0:
        _note(data, core, f"пересборка после эталона завершилась с кодом {rebuild_code}", _tail(rebuild_log))
        return _rejected(
            "rebuild_failed",
            f"пересборка после эталона завершилась с кодом {rebuild_code}",
            str(before_log),
            "",
            str(rebuild_log),
        )

    after_log = logs / "after.log"
    after_code = _run_oracle(
        core.image, work, test_command, task.fail_to_pass, after_log, test_timeout, drop_privileges
    )
    if after_code != 0:
        reason_code, reason = classify_failed_run(core.id, work, after_log)
        return _rejected(reason_code, reason, str(before_log), str(after_log), str(rebuild_log))
    append_decision(
        data,
        f"\n## Оракул принял {task.instance_id}\n\n"
        f"Сборка: `{build_command}`\n\n"
        f"Тест: `{test_command}`\n",
    )
    return OracleResult(
        "accepted",
        "accepted",
        "тесты упали до эталона и прошли после",
        str(before_log),
        str(after_log),
        str(build_log),
    )


@beartype
def _postgres_freeze(task: TaskInstance, template: str) -> str:
    if task.oracle_command.startswith("make -C src/test/regress check"):
        return template
    return task.oracle_command


@beartype
def _build_command(core: CoreConfig) -> str:
    if core.build_command != "":
        return core.build_command
    if core.id == "postgres":
        return POSTGRES_BUILD_COMMAND
    if core.id == "sqlite":
        return SQLITE_BUILD_COMMAND
    return DEFAULT_BUILD_COMMAND


@beartype
def oracle_shell_command(core: CoreConfig, task: TaskInstance, oracle_template: str) -> str:
    # Живой прогон берёт строку из JSON задачи. Её тоже надо сузить до check-tests,
    # иначе цель check игнорирует TESTS= и гоняет весь parallel_schedule.
    if task.oracle_command != "":
        raw = task.oracle_command
    elif core.oracle_command != "":
        raw = core.oracle_command
    else:
        raw = oracle_template
    if core.id != "postgres":
        return raw
    # parallel_schedule создаёт общие таблицы в test_setup. Один тест без этого
    # шага падает с «relation does not exist» и до эталона, и после.
    return _prepend_regress_setup(limit_regress_command(raw))


@beartype
def _prepend_regress_setup(command: str) -> str:
    key = "TESTS="
    index = command.find(key)
    if index < 0:
        return command
    head = command[:index]
    tail = command[index + len(key) :]
    if tail.startswith('"') and tail.endswith('"'):
        inner = tail[1:-1]
        if inner == "test_setup" or inner.startswith("test_setup "):
            return command
        return f'{head}{key}"test_setup {inner}"'
    if tail == "test_setup" or tail.startswith("test_setup "):
        return command
    return f'{head}{key}"test_setup {tail}"'


@beartype
def _test_command(core: CoreConfig, task: TaskInstance) -> str:
    fallback = SQLITE_ORACLE_TEMPLATE if core.id == "sqlite" else POSTGRES_ORACLE_TEMPLATE
    return oracle_shell_command(core, task, fallback)


@beartype
def postmaster_did_not_start(log_path: Path) -> bool:
    if not log_path.is_file():
        return False
    text = log_path.read_text(encoding="utf-8", errors="replace")
    return "postmaster failed" in text or "postmaster did not respond" in text


@beartype
def append_postmaster_logs(work: Path, log_path: Path) -> None:
    chunks: list[str] = []
    for relative in POSTGRES_STARTUP_LOGS:
        path = work / relative
        if not path.is_file():
            continue
        body = path.read_text(encoding="utf-8", errors="replace")
        chunks.append(f"\n# {relative}\n{body}")
    if len(chunks) == 0:
        return
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write("".join(chunks))


@beartype
def classify_failed_run(core_id: str, work: Path, log_path: Path) -> tuple[str, str]:
    if _startup_failed(core_id, work, log_path):
        return "postmaster_failed", POSTMASTER_REASON
    return "gold_did_not_fix", "эталон не починил падение тестов"


@beartype
def _startup_failed(core_id: str, work: Path, log_path: Path) -> bool:
    if core_id != "postgres" or not postmaster_did_not_start(log_path):
        return False
    append_postmaster_logs(work, log_path)
    return True


@beartype
def incremental_build_command(work: Path, command: str) -> str:
    # В дереве PostgreSQL Makefile есть до configure и только просит его запустить.
    # Повторный make допустим после появления config.status.
    if (work / "config.status").exists() and "&&" in command:
        return command.split("&&")[-1].strip()
    return command


@beartype
def _dockerfile(core: CoreConfig) -> Path:
    return project_root() / "docker" / f"{core.id}.Dockerfile"


@beartype
def _run_oracle(
    image: str,
    work: Path,
    command: str,
    tests: list[str],
    log_path: Path,
    timeout: int,
    drop_privileges: bool,
) -> int:
    if "{test}" not in command:
        return docker_bash(image, work, command, log_path, timeout, None, not drop_privileges, drop_privileges)
    # TESTS="test_setup {test}" уже в кавычках: подстановка не должна добавить вторую пару.
    if "test_setup {test}" in command:
        rendered = command.replace("{test}", "$test_path")
    else:
        rendered = command.replace("{test}", '"$test_path"')
    script = (
        "fail=0\n"
        'for test_path in "$@"; do\n'
        '  echo "RUN ${test_path}"\n'
        f"  if ! {rendered}; then\n"
        "    fail=1\n"
        "  fi\n"
        "done\n"
        'exit "$fail"\n'
    )
    return docker_bash(image, work, script, log_path, timeout, tests, not drop_privileges, drop_privileges)


@beartype
def ensure_tools_image(image: str, dockerfile: Path) -> tuple[bool, str]:
    inspect = subprocess.run(
        ["docker", "image", "inspect", image],
        check=False,
        capture_output=True,
        text=True,
    )
    if inspect.returncode == 0:
        return True, "образ уже есть"
    try:
        result = subprocess.run(
            ["docker", "build", "-t", image, "-f", str(dockerfile), str(dockerfile.parent)],
            check=False,
            capture_output=True,
            text=True,
            timeout=IMAGE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        return False, _timeout_text(exc)
    return result.returncode == 0, result.stdout + "\n" + result.stderr


@beartype
def find_test_binary(work: Path) -> str | None:
    for relative in ("build/release/test/unittest", "build/release/test/run"):
        if (work / relative).is_file():
            return relative
    return None


@beartype
def run_tests(image: str, work: Path, binary: str, tests: list[str], log_path: Path) -> int:
    script = (
        'set -u\n'
        'binary="$1"\n'
        'shift\n'
        'fail=0\n'
        'for test_path in "$@"; do\n'
        '  echo "RUN ${test_path}"\n'
        '  if ! "$binary" "$test_path"; then\n'
        '    fail=1\n'
        '  fi\n'
        'done\n'
        'exit "$fail"\n'
    )
    return docker_bash(image, work, script, log_path, TEST_TIMEOUT_SECONDS, [binary, *tests], False)


@beartype
def host_user_script(script: str) -> str:
    # initdb отказывается работать, если uid процесса нет в /etc/passwd.
    # Контейнер стартует от root. Для uid ниже 1000 без -o useradd может
    # завести другой номер. /src один раз получает владельца хоста, пока мы root.
    # Каталоги прошлого прогона удаляет уже этот пользователь, до сборки и до проверки.
    # Каталог данных на bind-mount virtiofs иногда виден postmaster с чужим uid.
    # Экземпляр pg_regress живёт на файловой системе контейнера.
    instance = "export EXTRA_REGRESS_OPTS=--temp-instance=/tmp/pg-regress-instance\n"
    cleanup = f"rm -rf {POSTGRES_LEFTOVER_DIRS}\n"
    return (
        'if [ "$HOST_UID" -lt 1000 ]; then\n'
        '  if ! getent group "$HOST_GID" >/dev/null; then groupadd -o -g "$HOST_GID" hostgroup; fi\n'
        '  if ! getent passwd "$HOST_UID" >/dev/null; then '
        'useradd -o -u "$HOST_UID" -g "$HOST_GID" -M -d /tmp -s /bin/bash builder; fi\n'
        "else\n"
        '  if ! getent group "$HOST_GID" >/dev/null; then groupadd -g "$HOST_GID" hostgroup; fi\n'
        '  if ! getent passwd "$HOST_UID" >/dev/null; then '
        'useradd -u "$HOST_UID" -g "$HOST_GID" -M -d /tmp -s /bin/bash builder; fi\n'
        "fi\n"
        'chown -R "$HOST_UID:$HOST_GID" /src\n'
        'exec runuser -u builder -- env HOME=/tmp TMPDIR=/tmp bash -lc "$(cat <<\'END_PIPELINE\'\n'
        f"{instance}"
        f"{cleanup}"
        f"{script}\n"
        "END_PIPELINE\n"
        ')" -- "$@"\n'
    )


@beartype
def docker_bash(
    image: str,
    work: Path,
    script: str,
    log_path: Path,
    timeout: int,
    args: list[str] | None = None,
    as_user: bool = False,
    drop_privileges: bool = False,
) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    command = ["docker", "run", "--rm"]
    if drop_privileges:
        script = host_user_script(script)
        command.extend(["-e", f"HOST_UID={os.getuid()}", "-e", f"HOST_GID={os.getgid()}"])
    elif as_user:
        command.extend(["-u", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp", "-e", "TMPDIR=/tmp"])
    command.extend(
        [
            "-v",
            f"{work.resolve()}:/src",
            "-w",
            "/src",
            image,
            "bash",
            "-lc",
            script,
            "bash",
        ]
    )
    if args is not None:
        command.extend(args)
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        log_path.write_text(_timeout_text(exc), encoding="utf-8")
        return 124
    log_path.write_text(result.stdout + "\n" + result.stderr, encoding="utf-8")
    return result.returncode


@beartype
def freeze_commands(core_id: str, build_command: str, oracle_command: str) -> None:
    path = repos_path()
    text = path.read_text(encoding="utf-8")
    updated = _fill_core_field(text, core_id, "build_command", build_command)
    updated = _fill_core_field(updated, core_id, "oracle_command", oracle_command)
    if updated != text:
        path.write_text(updated, encoding="utf-8")


@beartype
def _fill_core_field(text: str, core_id: str, key: str, value: str) -> str:
    parts = text.split("[[cores]]")
    rebuilt: list[str] = [parts[0]]
    needle = f'{key} = ""'
    for block in parts[1:]:
        if f'id = "{core_id}"' in block and needle in block:
            block = block.replace(needle, f'{key} = "{value}"', 1)
        rebuilt.append(block)
    return "[[cores]]".join(rebuilt)


@beartype
def _rejected(reason_code: str, reason: str, before_log: str, after_log: str, build_log: str) -> OracleResult:
    return OracleResult("rejected", reason_code, reason, before_log, after_log, build_log)


@beartype
def _note(data: Path, core: CoreConfig, title: str, detail: str) -> None:
    append_decision(data, f"\n## {core.id}: {title}\n\n```\n{detail[-4000:]}\n```\n")


@beartype
def _tail(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")[-4000:]


@beartype
def _timeout_text(exc: subprocess.TimeoutExpired) -> str:
    stdout = exc.stdout if isinstance(exc.stdout, str) else ""
    stderr = exc.stderr if isinstance(exc.stderr, str) else ""
    return f"таймаут {exc.timeout} с\n{stdout}\n{stderr}"
