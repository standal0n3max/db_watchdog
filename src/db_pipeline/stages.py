"""Стадии цепочки и два способа запуска: закреплённый проход и ``--harvest``.

Общее состояние — ``StageResult``. Поля: ``status``, ``repository_id``, ``sha``, ``base_sha``, ``pr_number``, ``instance_id``, ``reason_code``, ``reason``, ``statement_source``, ``task_path``, ``oracle_status``, ``run_path``, ``detail``.

``run_all(data, pr_number, core_id)`` идёт ``watch_repos`` → ``fetch_change`` → ``filter_stage`` → ``compile_stage`` → ``verify_stage`` → ``opencode_stage`` → ``write_record``. Стадия с уже неподходящим ``status`` возвращает состояние как есть. Отказ фильтра или оракула всё равно доходит до ``write_record``.

``detail == "harvest"`` переключает те же функции. Постановка PostgreSQL и SQLite — сообщение коммита, у DuckDB — issue, описание PR или сообщение коммита. Фильтр идёт по префиксам ``sql_*``, без списков образца. Разрез PostgreSQL — каталог ``src/test/regress/``. OpenCode не вызывается. Параметры окна: ``scan``, ``accept``, ``oracle_budget``. После записи журнала ``discard_workdir`` стирает ``data/work/<sha>``. Пропавший во время удаления файл не выходит наружу: результат оракула уже записан.

Закреплённый ``watch_repos``: DuckDB берёт PR (``pinned_pr`` или аргумент ``pr_number``), PostgreSQL — следующий образец pg-gym, пока отказов оракула меньше ``ORACLE_FAILURE_LIMIT`` (3), SQLite — ``pinned_sha``. Курсор обхода лежит в ``data/cursors/<core>.json``. Закреплённый проход дерево не стирает: следом ``opencode_stage`` делает ``git reset`` в том же каталоге.

Airflow пока не на сценарии ``--harvest``. Когда DAG переведут на тот же обход, удаление ``data/work/<sha>`` должно достаться им через ``run_harvest`` и ``detail == "harvest"``, а не второй копией в файлах DAG.

``reverify_postgres`` заново гоняет оракул по уже записанным JSON и второй файл задачи не создаёт.
"""

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict

from beartype import beartype

from db_pipeline.compile_task import compile_task
from db_pipeline.config import CoreConfig, load_cores, require_core
from db_pipeline.decisions import ensure_decisions
from db_pipeline.errors import PipelineError
from db_pipeline.filter_rules import filter_candidate
from db_pipeline.git_ops import (
    commit_body,
    commit_parents,
    commit_subject,
    diff_range,
    ensure_clone,
    ensure_origin_branch,
    ensure_sha,
    file_changes,
    harvest_window,
    pr_number_from_subject,
    resolve_pr_sha,
)
from db_pipeline.journal import (
    create_journal,
    harvest_known_shas,
    known_shas,
    pending_without_oracle,
    read_journal,
    reopen_harvest_record,
    update_journal,
    write_json,
    write_json_once,
)
from db_pipeline.models import (
    ChangeView,
    TaskInstance,
    change_from_dict,
    change_to_dict,
    parse_statement_source,
    task_from_dict,
    task_to_dict,
)
from db_pipeline.opencode_run import run_opencode
from db_pipeline.parsing import as_dict, as_str, as_str_list, load_json_file
from db_pipeline.paths import resolve_data
from db_pipeline.samples import (
    PostgresSample,
    bundled_git_dir,
    limit_regress_command,
    sample_by_name,
    sample_by_sha,
)
from db_pipeline.statements import choose_statement
from db_pipeline.verify_environment import OracleResult, verify_task

ORACLE_FAILURE_LIMIT = 3


class StageResult(TypedDict):
    status: str
    repository_id: str
    sha: str
    base_sha: str
    pr_number: str
    instance_id: str
    reason_code: str
    reason: str
    statement_source: str
    task_path: str
    oracle_status: str
    run_path: str
    detail: str


@beartype
def reverify_postgres(data: Path | None = None) -> StageResult:
    # Не вызывает watchRepos и не пишет второй JSON. OpenCode здесь не запускается:
    # он и так пропускается, пока оракул не принял экземпляр, а этот проход на нём останавливается.
    root = resolve_data(data)
    ensure_decisions(root)
    state = _blank()
    state["repository_id"] = "postgres"
    try:
        core = require_core(load_cores(), "postgres")
        for sha, task_path, task in reverify_targets(root, core):
            state["sha"] = sha
            state["base_sha"] = task.base_sha
            state["instance_id"] = task.instance_id
            state["task_path"] = str(task_path)
            state["statement_source"] = task.statement_source
            result = verify_task(core, task, _clone_path(root, core), root)
            save_oracle_result(root, sha, task_path, task, result)
            discard_workdir(root, sha)
            state["oracle_status"] = result.status
            state["reason_code"] = result.reason_code
            state["reason"] = result.reason
            if result.status == "accepted":
                state["status"] = "verified"
                state["detail"] = "opencode пропущен"
                return write_record(state, root)
            state["status"] = "rejected"
        state["status"] = "rejected"
        state["detail"] = "три отказа оракула, поиск образца остановлен"
    except (PipelineError, OSError, ValueError) as exc:
        _fail(state, "reverify_failed", str(exc))
    return write_record(state, root)


@beartype
def reverify_targets(root: Path, core: CoreConfig) -> list[tuple[str, Path, TaskInstance]]:
    if core.id != "postgres":
        raise PipelineError("reverify", "повторная проверка только для postgres")
    if len(core.sample_candidates) == 0:
        raise PipelineError("reverify", "в конфиге нет кандидатов PostgreSQL")
    targets: list[tuple[str, Path, TaskInstance]] = []
    for name in core.sample_candidates:
        sample = sample_by_name(core, name)
        record = read_journal(root, sample.sha)
        if record is None:
            raise PipelineError("reverify", f"в журнале нет {sample.sha}")
        instance_id = as_str(record.get("instance_id"), "instance_id")
        task_path = root / "tasks" / f"{instance_id}.json"
        if not task_path.is_file():
            raise PipelineError("reverify", f"нет JSON задачи {instance_id}")
        task = task_from_dict(load_json_file(task_path))
        targets.append((sample.sha, task_path, task))
    return targets


@beartype
def save_oracle_result(
    root: Path,
    sha: str,
    task_path: Path,
    task: TaskInstance,
    result: OracleResult,
) -> None:
    update_journal(
        root,
        sha,
        {
            "oracle_status": result.status,
            "oracle_reason": result.reason,
            "before_log": result.before_log,
            "after_log": result.after_log,
            "build_log": result.build_log,
        },
    )
    payload = task_to_dict(task)
    if result.status == "accepted":
        payload["reject_reason"] = ""
    else:
        payload["reject_reason"] = result.reason
    write_json(task_path, payload)


@beartype
def _ignore_missing_file(_function: object, _path: object, exc_info: tuple[object, ...]) -> None:
    # В Python 3.11 unlink внутри rmtree отдаёт только имя файла, например gc.pid.
    if len(exc_info) < 2:
        return
    error = exc_info[1]
    if isinstance(error, FileNotFoundError):
        return
    if isinstance(error, BaseException):
        raise error


@beartype
def discard_workdir(root: Path, head_sha: str) -> None:
    # Результат оракула уже в журнале. Ошибка очистки наружу не выходит.
    if head_sha == "":
        return
    work = root / "work" / head_sha
    for _attempt in range(2):
        if not work.is_dir():
            return
        try:
            shutil.rmtree(work, onerror=_ignore_missing_file)
        except OSError:
            continue
        if not work.exists():
            return
    if not work.is_dir():
        return
    shutil.rmtree(work, ignore_errors=True)


@beartype
def run_all(
    data: Path | None = None,
    pr_number: int | None = None,
    core_id: str = "duckdb",
) -> StageResult:
    root = resolve_data(data)
    ensure_decisions(root)
    state = watch_repos(root, pr_number, core_id)
    state = fetch_change(state, root)
    state = filter_stage(state, root)
    state = compile_stage(state, root)
    state = verify_stage(state, root)
    state = opencode_stage(state, root)
    return write_record(state, root)


@beartype
def run_harvest(
    data: Path | None = None,
    core_id: str = "duckdb",
    scan: int = 40,
    accept: int = 1,
    oracle_budget: int = 0,
) -> StageResult:
    # Закреплённый путь и счётчик трёх отказов сюда не входят.
    # OpenCode не вызывается: экземпляр для датасета готов после оракула.
    root = resolve_data(data)
    ensure_decisions(root)
    summary = _blank()
    summary["repository_id"] = core_id
    try:
        core = require_core(load_cores(), core_id)
        if not core.enabled:
            raise PipelineError("harvest", f"ядро {core.id} выключено")
        if len(core.sql_code_prefixes) == 0 or len(core.sql_test_prefixes) == 0:
            raise PipelineError("harvest", "в конфиге нет префиксов ядра")
        clone = _clone_path(root, core)
        ensure_origin_branch(core, clone)
        cursor = read_cursor(root, core.id)
        window = harvest_window(clone, core, harvest_known_shas(root), cursor, scan)
        if len(window) == 0:
            _fail(summary, "empty_window", "в окне нет новых коммитов")
            return write_record(summary, root)
        accepted = 0
        builds = 0
        last = summary
        for sha, subject in window:
            if oracle_budget > 0 and builds >= oracle_budget:
                last["detail"] = f"потолок сборок {builds}, принято {accepted}"
                if last["status"] != "verified":
                    _fail(last, "oracle_budget", "потолок сборок за этот запуск")
                return write_record(last, root)
            state = _ready_harvest(core.id, sha, subject)
            state = fetch_change(state, root)
            if state["status"] != "ready":
                _journal_fetch_failure(root, state)
                write_cursor(root, core.id, sha)
                last = write_record(state, root)
                continue
            state = filter_stage(state, root)
            state = compile_stage(state, root)
            if state["status"] == "accepted":
                builds += 1
                state = verify_stage(state, root)
                if state["status"] == "verified":
                    accepted += 1
            last = write_record(state, root)
            write_cursor(root, core.id, sha)
            if accepted >= accept:
                break
        last["detail"] = f"просмотрено {len(window)}, принято {accepted}, сборок {builds}"
        return write_record(last, root)
    except (PipelineError, OSError, ValueError) as exc:
        _fail(summary, "harvest_failed", str(exc))
    return write_record(summary, root)


@beartype
def watch_repos(
    data: Path | None = None,
    pr_number: int | None = None,
    core_id: str = "duckdb",
) -> StageResult:
    root = resolve_data(data)
    state = _blank()
    try:
        core = require_core(load_cores(), core_id)
        if not core.enabled:
            raise PipelineError("watch", f"ядро {core.id} выключено")
        state["repository_id"] = core.id
        if core.id == "duckdb":
            _watch_duckdb(state, root, core, pr_number)
        elif core.id == "postgres":
            _watch_postgres(state, root, core)
        elif core.id == "sqlite":
            _watch_sqlite(state, root, core)
        else:
            raise PipelineError("watch", f"ядро {core.id} не разбирается")
    except (PipelineError, OSError) as exc:
        _fail(state, "watch_failed", str(exc))
    return state


@beartype
def fetch_change(state: StageResult, data: Path | None = None) -> StageResult:
    root = resolve_data(data)
    if state["status"] != "ready":
        return state
    try:
        core = _core_of(state)
        clone = _clone_path(root, core)
        sha = state["sha"]
        if state["detail"] == "harvest":
            if core.id == "duckdb":
                _fetch_duckdb(state, root, core, clone, sha)
            else:
                _fetch_commit_message(state, root, core, clone, sha)
        elif core.id == "postgres":
            _fetch_postgres(state, root, core, clone, sha)
        elif core.id == "sqlite":
            _fetch_commit_message(state, root, core, clone, sha)
        else:
            _fetch_duckdb(state, root, core, clone, sha)
        state["status"] = "ready"
    except (PipelineError, OSError, ValueError) as exc:
        _fail(state, "fetch_failed", str(exc))
    return state


@beartype
def filter_stage(state: StageResult, data: Path | None = None) -> StageResult:
    root = resolve_data(data)
    if state["status"] != "ready":
        return state
    sha = state["sha"]
    existing = read_journal(root, sha)
    harvest = state["detail"] == "harvest"
    reopen = harvest and existing is not None and reopen_harvest_record(existing)
    if existing is not None and not reopen:
        state["status"] = "duplicate"
        state["reason_code"] = "already_in_journal"
        state["reason"] = "sha уже есть в журнале"
        return state
    try:
        core = _core_of(state)
        change = _load_change(root, sha)
        if harvest:
            solution_paths, test_paths = None, None
            seen = harvest_known_shas(root)
        else:
            solution_paths, test_paths = _sample_lists(root, core, sha)
            seen = known_shas(root)
        decision = filter_candidate(
            change,
            core,
            seen,
            solution_paths,
            test_paths,
            harvest,
        )
        state["reason_code"] = decision.reason_code
        state["reason"] = decision.reason
        if decision.accept:
            state["status"] = "accepted"
            decision_name = "accept"
        else:
            state["status"] = "rejected"
            decision_name = "reject"
        journal_fields: dict[str, object] = {
            "decision": decision_name,
            "reason_code": decision.reason_code,
            "reason": decision.reason,
            "sample": state["detail"],
            "oracle_status": "pending" if decision.accept else "skipped",
            "oracle_reason": "",
        }
        if reopen:
            update_journal(root, sha, journal_fields)
            created = True
        else:
            created = create_journal(
                root,
                {
                    "sha": sha,
                    "repository_id": state["repository_id"],
                    "instance_id": "",
                    "pr_number": state["pr_number"],
                    "recorded_at": _now(),
                    **journal_fields,
                },
            )
        if not created:
            state["status"] = "duplicate"
            state["reason_code"] = "already_in_journal"
            state["reason"] = "sha уже есть в журнале"
    except (PipelineError, OSError, ValueError) as exc:
        _fail(state, "filter_failed", str(exc))
    return state


@beartype
def compile_stage(state: StageResult, data: Path | None = None) -> StageResult:
    root = resolve_data(data)
    if state["status"] != "accepted":
        return state
    try:
        sha = state["sha"]
        core = _core_of(state)
        change = _load_change(root, sha)
        folder = root / "changes" / sha
        statement = (folder / "statement.txt").read_text(encoding="utf-8")
        source_text = (folder / "statement_source.txt").read_text(encoding="utf-8").strip()
        source = parse_statement_source(source_text)
        diff_text = (folder / "diff.patch").read_text(encoding="utf-8")
        if core.id == "postgres" and state["detail"] == "harvest":
            command = limit_regress_command(core.oracle_command)
            task = compile_task(
                change,
                core,
                diff_text,
                statement,
                source,
                oracle_command=command,
                regress_split=True,
            )
        elif core.id == "postgres":
            sample = _saved_sample(folder)
            task = compile_task(
                change,
                core,
                diff_text,
                statement,
                source,
                gold_paths=sample.solution_paths,
                test_paths=sample.test_files,
                fail_to_pass=sample.oracle_tests,
                oracle_command=limit_regress_command(sample.oracle_command),
            )
        else:
            task = compile_task(change, core, diff_text, statement, source)
        path = root / "tasks" / f"{task.instance_id}.json"
        created = write_json_once(path, task_to_dict(task))
        if not created:
            # Pending без оракула уже мог получить JSON. Это не второй экземпляр:
            # оракул ещё не запускался, проверка идёт по записанной задаче.
            current = read_journal(root, sha)
            if state["detail"] == "harvest" and current is not None and pending_without_oracle(current):
                update_journal(root, sha, {"instance_id": task.instance_id})
                state["instance_id"] = task.instance_id
                state["task_path"] = str(path)
                state["statement_source"] = source
                return state
            state["status"] = "duplicate"
            state["reason_code"] = "already_in_journal"
            state["reason"] = "JSON задачи уже есть"
            state["instance_id"] = task.instance_id
            state["task_path"] = str(path)
            return state
        update_journal(root, sha, {"instance_id": task.instance_id})
        state["instance_id"] = task.instance_id
        state["task_path"] = str(path)
        state["statement_source"] = source
    except (PipelineError, OSError, ValueError) as exc:
        _fail(state, "compile_failed", str(exc))
    return state


@beartype
def verify_stage(state: StageResult, data: Path | None = None) -> StageResult:
    root = resolve_data(data)
    if state["status"] != "accepted":
        return state
    harvest = state["detail"] == "harvest"
    try:
        task = task_from_dict(load_json_file(Path(state["task_path"])))
        core = _core_of(state)
        result = verify_task(core, task, _clone_path(root, core), root)
        state["oracle_status"] = result.status
        state["reason_code"] = result.reason_code
        state["reason"] = result.reason
        update_journal(
            root,
            state["sha"],
            {
                "oracle_status": result.status,
                "oracle_reason": result.reason,
                "before_log": result.before_log,
                "after_log": result.after_log,
                "build_log": result.build_log,
            },
        )
        if result.status != "accepted":
            payload = task_to_dict(task)
            payload["reject_reason"] = result.reason
            write_json(Path(state["task_path"]), payload)
            state["status"] = "rejected"
        else:
            state["status"] = "verified"
    except (PipelineError, OSError, ValueError) as exc:
        _fail(state, "verify_failed", str(exc))
        update_journal(
            root,
            state["sha"],
            {"oracle_status": "rejected", "oracle_reason": str(exc)},
        )
        if harvest:
            discard_workdir(root, state["sha"])
        return state
    if harvest:
        discard_workdir(root, task.head_sha)
    return state


@beartype
def opencode_stage(state: StageResult, data: Path | None = None) -> StageResult:
    root = resolve_data(data)
    if state["status"] != "verified":
        state["detail"] = state["detail"] or "opencode пропущен"
        return state
    try:
        core = _core_of(state)
        task = task_from_dict(load_json_file(Path(state["task_path"])))
        if core.id == "duckdb" and task.statement_source == "commit_message":
            state["detail"] = "прогон пропущен: постановка только из сообщения коммита"
            return state
        work = root / "work" / task.head_sha
        record = run_opencode(task, work, root)
        path = root / "runs" / f"{task.instance_id}.json"
        write_json_once(path, record)
        state["run_path"] = str(path)
        state["status"] = "ran"
        passed = record.get("pass")
        state["detail"] = "pass" if passed is True else "pass ложь"
    except (PipelineError, OSError, ValueError, RuntimeError) as exc:
        _fail(state, "opencode_failed", str(exc))
    return state


@beartype
def write_record(state: StageResult, data: Path | None = None) -> StageResult:
    root = resolve_data(data)
    if state["status"] == "duplicate":
        state["detail"] = "повторный запуск не создал вторую запись"
    summary = root / "pipeline_last.json"
    payload: dict[str, object] = {
        "status": state["status"],
        "repository_id": state["repository_id"],
        "sha": state["sha"],
        "base_sha": state["base_sha"],
        "pr_number": state["pr_number"],
        "instance_id": state["instance_id"],
        "reason_code": state["reason_code"],
        "reason": state["reason"],
        "statement_source": state["statement_source"],
        "task_path": state["task_path"],
        "oracle_status": state["oracle_status"],
        "run_path": state["run_path"],
        "detail": state["detail"],
    }
    write_json(summary, payload)
    return state


@beartype
def _watch_duckdb(state: StageResult, root: Path, core: CoreConfig, pr_number: int | None) -> None:
    chosen = core.pinned_pr if pr_number is None else pr_number
    if chosen is None:
        raise PipelineError("watch", "в конфиге нет закреплённого PR")
    clone = _clone_path(root, core)
    ensure_clone(core, clone)
    sha = resolve_pr_sha(clone, chosen)
    state["status"] = "ready"
    state["sha"] = sha
    state["pr_number"] = str(chosen)
    state["detail"] = "pinned"


@beartype
def _watch_postgres(state: StageResult, root: Path, core: CoreConfig) -> None:
    if _oracle_failures(root, core.id) >= ORACLE_FAILURE_LIMIT:
        raise PipelineError("watch", "три отказа оракула, поиск образца остановлен")
    if len(core.sample_candidates) == 0:
        raise PipelineError("watch", "в конфиге нет кандидатов PostgreSQL")
    known = known_shas(root)
    sample: PostgresSample | None = None
    for name in core.sample_candidates:
        candidate = sample_by_name(core, name)
        if candidate.sha not in known:
            sample = candidate
            break
    if sample is None:
        raise PipelineError("watch", "кандидаты PostgreSQL уже в журнале")
    clone = _clone_path(root, core)
    source = bundled_git_dir(core)
    ensure_sha(core, clone, sample.sha, source)
    ensure_sha(core, clone, sample.parent, source)
    state["status"] = "ready"
    state["sha"] = sample.sha
    state["detail"] = sample.name


@beartype
def _watch_sqlite(state: StageResult, root: Path, core: CoreConfig) -> None:
    if _oracle_failures(root, core.id) >= ORACLE_FAILURE_LIMIT:
        raise PipelineError("watch", "три отказа оракула, поиск коммита остановлен")
    candidates = core.candidate_shas if len(core.candidate_shas) > 0 else []
    if len(candidates) == 0 and core.pinned_sha != "":
        candidates = [core.pinned_sha]
    if len(candidates) == 0:
        raise PipelineError("watch", "sha SQLite не закреплён")
    known = known_shas(root)
    chosen = ""
    for sha in candidates:
        if sha not in known:
            chosen = sha
            break
    if chosen == "":
        raise PipelineError("watch", "кандидаты SQLite уже в журнале")
    clone = _clone_path(root, core)
    ensure_clone(core, clone)
    ensure_sha(core, clone, chosen, None)
    state["status"] = "ready"
    state["sha"] = chosen
    state["detail"] = "pinned"


@beartype
def _fetch_duckdb(state: StageResult, root: Path, core: CoreConfig, clone: Path, sha: str) -> None:
    parents = commit_parents(clone, sha)
    if len(parents) == 0:
        raise PipelineError("fetch", "у коммита нет родителя")
    pr_number = int(state["pr_number"]) if state["pr_number"] != "" else None
    subject = commit_subject(clone, sha)
    body = commit_body(clone, sha)
    statement, source, _issue = choose_statement(subject, body, pr_number)
    change = ChangeView(
        sha=sha,
        repository_id=core.id,
        subject=subject,
        body=body,
        parent_count=len(parents),
        files=file_changes(clone, parents[0], sha),
        pr_number=pr_number,
        base_sha=parents[0],
    )
    _store_change(root, change, diff_range(clone, parents[0], sha), statement, source)
    state["base_sha"] = parents[0]
    state["statement_source"] = source


@beartype
def _fetch_postgres(state: StageResult, root: Path, core: CoreConfig, clone: Path, sha: str) -> None:
    sample = sample_by_sha(core, sha)
    source_git = bundled_git_dir(core)
    ensure_sha(core, clone, sample.sha, source_git)
    ensure_sha(core, clone, sample.parent, source_git)
    subject = commit_subject(clone, sha)
    body = commit_body(clone, sha)
    parents = commit_parents(clone, sha)
    change = ChangeView(
        sha=sha,
        repository_id=core.id,
        subject=subject,
        body=body,
        parent_count=len(parents),
        files=file_changes(clone, sample.parent, sha),
        pr_number=None,
        base_sha=sample.parent,
    )
    _store_change(root, change, diff_range(clone, sample.parent, sha), sample.goal, "commit_message")
    _store_sample(root, sample)
    state["base_sha"] = sample.parent
    state["statement_source"] = "commit_message"
    state["detail"] = sample.name


@beartype
def _fetch_commit_message(state: StageResult, root: Path, core: CoreConfig, clone: Path, sha: str) -> None:
    parents = commit_parents(clone, sha)
    if len(parents) == 0:
        raise PipelineError("fetch", "у коммита нет родителя")
    subject = commit_subject(clone, sha)
    body = commit_body(clone, sha)
    statement = f"{subject}\n\n{body}".strip()
    if statement == "":
        raise PipelineError("fetch", "у коммита пустое сообщение")
    change = ChangeView(
        sha=sha,
        repository_id=core.id,
        subject=subject,
        body=body,
        parent_count=len(parents),
        files=file_changes(clone, parents[0], sha),
        pr_number=None,
        base_sha=parents[0],
    )
    _store_change(root, change, diff_range(clone, parents[0], sha), statement, "commit_message")
    state["base_sha"] = parents[0]
    state["statement_source"] = "commit_message"


@beartype
def _store_change(root: Path, change: ChangeView, diff_text: str, statement: str, source: str) -> None:
    folder = root / "changes" / change.sha
    folder.mkdir(parents=True, exist_ok=True)
    write_json(folder / "change.json", change_to_dict(change))
    (folder / "diff.patch").write_text(diff_text, encoding="utf-8")
    (folder / "statement.txt").write_text(statement, encoding="utf-8")
    (folder / "statement_source.txt").write_text(source, encoding="utf-8")


@beartype
def _store_sample(root: Path, sample: PostgresSample) -> None:
    payload: dict[str, object] = {
        "name": sample.name,
        "sha": sample.sha,
        "parent": sample.parent,
        "subject": sample.subject,
        "goal": sample.goal,
        "solution_paths": sample.solution_paths,
        "test_files": sample.test_files,
        "oracle_tests": sample.oracle_tests,
        "oracle_command": sample.oracle_command,
    }
    write_json(root / "changes" / sample.sha / "sample.json", payload)


@beartype
def _saved_sample(folder: Path) -> PostgresSample:
    raw = as_dict(load_json_file(folder / "sample.json"), "sample")
    return PostgresSample(
        name=as_str(raw.get("name"), "name"),
        sha=as_str(raw.get("sha"), "sha"),
        parent=as_str(raw.get("parent"), "parent"),
        subject=as_str(raw.get("subject"), "subject"),
        goal=as_str(raw.get("goal"), "goal"),
        solution_paths=_str_list(raw.get("solution_paths"), "solution_paths"),
        test_files=_str_list(raw.get("test_files"), "test_files"),
        oracle_tests=_str_list(raw.get("oracle_tests"), "oracle_tests"),
        oracle_command=as_str(raw.get("oracle_command"), "oracle_command"),
    )


@beartype
def _str_list(value: object, label: str) -> list[str]:
    return as_str_list(value, label)


@beartype
def _sample_lists(root: Path, core: CoreConfig, sha: str) -> tuple[list[str] | None, list[str] | None]:
    if core.id != "postgres":
        return None, None
    sample = _saved_sample(root / "changes" / sha)
    return sample.solution_paths, sample.test_files


@beartype
def _oracle_failures(root: Path, core_id: str) -> int:
    folder = root / "journal"
    if not folder.exists():
        return 0
    count = 0
    for path in folder.glob("*.json"):
        record = as_dict(load_json_file(path), "journal")
        if record.get("repository_id") != core_id:
            continue
        if record.get("oracle_status") == "rejected":
            count += 1
    return count


@beartype
def _core_of(state: StageResult) -> CoreConfig:
    if state["repository_id"] == "":
        raise PipelineError("stage", "ядро не выбрано")
    return require_core(load_cores(), state["repository_id"])


@beartype
def _clone_path(data: Path, core: CoreConfig) -> Path:
    return data / "clones" / core.id


@beartype
def _load_change(data: Path, sha: str) -> ChangeView:
    return change_from_dict(load_json_file(data / "changes" / sha / "change.json"))


@beartype
def read_cursor(root: Path, core_id: str) -> str:
    path = root / "cursors" / f"{core_id}.json"
    if not path.is_file():
        return ""
    raw = as_dict(load_json_file(path), "cursor")
    value = raw.get("sha")
    if not isinstance(value, str):
        return ""
    return value


@beartype
def write_cursor(root: Path, core_id: str, sha: str) -> None:
    if sha == "":
        return
    write_json(root / "cursors" / f"{core_id}.json", {"sha": sha})


@beartype
def _ready_harvest(core_id: str, sha: str, subject: str) -> StageResult:
    state = _blank()
    state["status"] = "ready"
    state["repository_id"] = core_id
    state["sha"] = sha
    state["detail"] = "harvest"
    if core_id == "duckdb":
        number = pr_number_from_subject(subject)
        if number is not None:
            state["pr_number"] = str(number)
    return state


@beartype
def _journal_fetch_failure(root: Path, state: StageResult) -> None:
    sha = state["sha"]
    if sha == "" or read_journal(root, sha) is not None:
        return
    create_journal(
        root,
        {
            "sha": sha,
            "repository_id": state["repository_id"],
            "decision": "reject",
            "reason_code": state["reason_code"],
            "reason": state["reason"],
            "instance_id": "",
            "pr_number": state["pr_number"],
            "sample": "",
            "recorded_at": _now(),
            "oracle_status": "skipped",
            "oracle_reason": "",
        },
    )


@beartype
def _blank() -> StageResult:
    return {
        "status": "",
        "repository_id": "",
        "sha": "",
        "base_sha": "",
        "pr_number": "",
        "instance_id": "",
        "reason_code": "",
        "reason": "",
        "statement_source": "",
        "task_path": "",
        "oracle_status": "",
        "run_path": "",
        "detail": "",
    }


@beartype
def _fail(state: StageResult, reason_code: str, reason: str) -> None:
    state["status"] = "rejected"
    state["reason_code"] = reason_code
    state["reason"] = reason


@beartype
def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
