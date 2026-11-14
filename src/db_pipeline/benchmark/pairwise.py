"""Парный прогон со слотами и возобновлением.

Слот — это ``(experiment_id, задача, рука, повтор)``. Checkpoint хранит
неизменяемый список задач, порядок слотов и состояния. ``--resume`` сверяет
хэш плана и не перезапускает финализированный FAIL. Запись, checkpoint и
отчёт пишутся атомарно; второй resume того же ярлыка не стартует.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import IO, cast

from beartype import beartype

from db_pipeline.benchmark import container, paths
from db_pipeline.benchmark import eval as eval_module
from db_pipeline.benchmark import runner
from db_pipeline.benchmark import session as smoke_session
from db_pipeline.benchmark.config import BenchmarkConfig, load_config
from db_pipeline.benchmark.graph import split as split_module
from db_pipeline.errors import PipelineError

HANDS = (eval_module.COMPARISON_LEFT, eval_module.COMPARISON_RIGHT)
Execute = Callable[["SlotPlan"], dict[str, object]]


@beartype
@dataclass
class SlotPlan:
    """Один слот партии: личность слота неизменна, ``state`` меняется."""

    slot_id: str
    task: str
    mode: str
    repetition: int
    task_index: int
    state: str = "pending"
    error: str = ""


@beartype
def slot_id_for(experiment_id: str, task: str, mode: str, repetition: int) -> str:
    """Стабильный ключ слота. Разные партии не делят один идентификатор."""
    raw = f"{experiment_id}\0{task}\0{mode}\0{repetition}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@beartype
def hand_order(task_index: int, repetition: int) -> tuple[str, str]:
    """Чётная пара начинает с bare, нечётная — с graph."""
    if (task_index + repetition) % 2 == 0:
        return HANDS
    return (HANDS[1], HANDS[0])


@beartype
def build_slots(experiment_id: str, names: list[str], repeats: int) -> list[SlotPlan]:
    """Порядок слотов: задача, затем повтор, затем чередование рук."""
    if repeats < 1:
        raise PipelineError("pairwise", "repeats должен быть не меньше 1")
    slots: list[SlotPlan] = []
    for index, name in enumerate(names):
        for repetition in range(repeats):
            for mode in hand_order(index, repetition):
                slots.append(
                    SlotPlan(
                        slot_id=slot_id_for(experiment_id, name, mode, repetition),
                        task=name,
                        mode=mode,
                        repetition=repetition,
                        task_index=index,
                    )
                )
    ensure_unique(slots)
    return slots


@beartype
def ensure_unique(slots: list[SlotPlan]) -> None:
    seen: set[str] = set()
    for slot in slots:
        if slot.slot_id in seen:
            raise PipelineError("pairwise", f"дубль слота {slot.slot_id}")
        seen.add(slot.slot_id)


@beartype
def resolve_names(section: str, tasks: list[str], data: Path | None) -> list[str]:
    """Явные задачи либо секция. ``frozen`` читает замороженный holdout."""
    if len(tasks) > 0:
        return [f"postgres__{value}" if len(value) == 12 else value for value in tasks]
    if section == "frozen":
        return split_module.read_holdout()
    return list(split_module.derive_split(data)[section])


@beartype
def plan_core(
    experiment_id: str,
    section: str,
    repeats: int,
    prompt_variant: str,
    names: list[str],
    slots: list[SlotPlan],
    config: dict[str, object],
    graph_sha256: str,
    image_ids: dict[str, str],
) -> dict[str, object]:
    """Тело плана без состояний слотов: его хэш обязан совпасть при resume."""
    return {
        "experiment_id": experiment_id,
        "section": section,
        "repeats": repeats,
        "prompt_variant": prompt_variant,
        "tasks": list(names),
        "holdout_sha256": split_module.holdout_sha256(names),
        "slots": [
            {
                "slot_id": slot.slot_id,
                "task": slot.task,
                "mode": slot.mode,
                "repetition": slot.repetition,
                "task_index": slot.task_index,
            }
            for slot in slots
        ],
        "config": config,
        "graph_sha256": graph_sha256,
        "image_ids": dict(image_ids),
    }


@beartype
def plan_sha256(core: dict[str, object]) -> str:
    raw = json.dumps(core, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@beartype
def checkpoint_path(data: Path | None, label: str) -> Path:
    return paths.reports_dir(data) / f"pairwise-{label}.checkpoint.json"


@beartype
def lock_path(data: Path | None, label: str) -> Path:
    return paths.reports_dir(data) / f"pairwise-{label}.lock"


@beartype
def report_path(data: Path | None, label: str, partial: bool) -> Path:
    suffix = ".partial.json" if partial else ".json"
    return paths.reports_dir(data) / f"pairwise-{label}{suffix}"


@beartype
def acquire_lock(path: Path) -> IO[str]:
    """Эксклюзивный lock. Второй resume получает ошибку, а не вторую партию."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise PipelineError("pairwise", "параллельный resume уже держит lock") from None
    return handle


@beartype
def release_lock(handle: IO[str]) -> None:
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    handle.close()


@beartype
def git_snapshot(root: Path | None = None) -> dict[str, object]:
    """Commit и признак грязного дерева. В хэш плана не входит."""
    folder = paths.project_root() if root is None else root
    return {
        "commit": _git(folder, "rev-parse", "HEAD"),
        "dirty": _git(folder, "status", "--porcelain") != "",
    }


@beartype
def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


@beartype
def file_sha256(path: Path) -> str:
    if not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if chunk == b"":
                break
            digest.update(chunk)
    return digest.hexdigest()


@beartype
def capture_images(cfg: BenchmarkConfig) -> dict[str, str]:
    return {
        "task": container.image_id(cfg.image),
        "graph": container.image_id(cfg.graph_image),
        "gateway": container.image_id(cfg.gateway_image),
    }


@beartype
def run_batch(
    data: Path | None,
    section: str,
    repeats: int,
    prompt_variant: str,
    report_label: str,
    names: list[str],
    resume: bool,
    no_budget_check: bool,
    cfg: BenchmarkConfig | None = None,
    execute: Execute | None = None,
    connect: bool | None = None,
    images: dict[str, str] | None = None,
    graph_sha256: str | None = None,
) -> dict[str, object]:
    """Гоняет слоты. ``execute`` подменяет Docker в тестах."""
    active_cfg = cfg or load_config()
    if active_cfg.experiment_id.strip() == "":
        raise PipelineError("pairwise", "BENCH_EXPERIMENT_ID не задан")
    if prompt_variant not in runner.PROMPT_VARIANTS:
        raise PipelineError("pairwise", f"вариант промпта {prompt_variant} неизвестен")
    if len(names) == 0:
        raise PipelineError("pairwise", f"секция {section} пуста")
    label = report_label or section
    use_docker = execute is None if connect is None else connect
    image_ids = images if images is not None else capture_images(active_cfg)
    graph_hash = (
        graph_sha256
        if graph_sha256 is not None
        else file_sha256(paths.graph_dir(data) / "graph.sqlite")
    )
    fresh = build_slots(active_cfg.experiment_id, names, repeats)
    core = plan_core(
        active_cfg.experiment_id,
        section,
        repeats,
        prompt_variant,
        names,
        fresh,
        active_cfg.to_manifest(),
        graph_hash,
        image_ids,
    )
    digest = plan_sha256(core)
    target = checkpoint_path(data, label)
    lock = acquire_lock(lock_path(data, label))
    try:
        if use_docker and not no_budget_check:
            guard = _budget(active_cfg)
            if not guard["ok"]:
                return {
                    "ok": False,
                    "exit_code": 2,
                    "error": "бюджет OpenRouter исчерпан",
                    "budget": guard,
                    "pairwise_plan_sha256": digest,
                }
        if resume:
            slots, body = _resume_checkpoint(target, digest, fresh)
        else:
            if target.is_file():
                raise PipelineError("pairwise", "checkpoint уже есть, нужен --resume")
            slots = fresh
            body = _checkpoint_body(
                label, section, repeats, prompt_variant, names, slots, digest,
                active_cfg, graph_hash, image_ids,
            )
            _save_checkpoint(target, body)
        return _drive(
            data, section, repeats, prompt_variant, label, names, slots, digest,
            active_cfg, target, body, execute, use_docker,
        )
    finally:
        release_lock(lock)


@beartype
def _budget(cfg: BenchmarkConfig) -> dict[str, object]:
    import os

    from db_pipeline.benchmark import budget

    report = budget.check_budget(
        os.environ.get("OPENROUTER_API_KEY", ""),
        max_tokens=cfg.max_tokens,
    )
    return report.to_dict()


@beartype
def _resume_checkpoint(
    target: Path,
    digest: str,
    fresh: list[SlotPlan],
) -> tuple[list[SlotPlan], dict[str, object]]:
    """Сверяет хэш и порядок. Состояния слотов берутся из файла."""
    if not target.is_file():
        raise PipelineError("pairwise", "нет checkpoint для --resume")
    saved = _load_checkpoint(target)
    if str(saved.get("pairwise_plan_sha256") or "") != digest:
        raise PipelineError("pairwise", "хэш плана не совпал с checkpoint")
    slots = _slots_from_checkpoint(saved)
    ensure_unique(slots)
    fresh_ids = [slot.slot_id for slot in fresh]
    saved_ids = [slot.slot_id for slot in slots]
    if fresh_ids != saved_ids:
        raise PipelineError("pairwise", "порядок слотов не совпал")
    return slots, saved


@beartype
def _drive(
    data: Path | None,
    section: str,
    repeats: int,
    prompt_variant: str,
    label: str,
    names: list[str],
    slots: list[SlotPlan],
    digest: str,
    cfg: BenchmarkConfig,
    target: Path,
    body: dict[str, object],
    execute: Execute | None,
    use_docker: bool,
) -> dict[str, object]:
    session: smoke_session.SmokeSession | None = None
    provider_stopped = False
    cleanup: dict[str, object] = {"ok": True, "skipped": not use_docker}
    try:
        if use_docker:
            session = smoke_session.open_session(cfg, data)
        runner_execute = execute or _docker_execute(session, cfg, data, prompt_variant, digest)
        for slot in slots:
            disk = _finalized_records(data, slot)
            if len(disk) > 1:
                raise PipelineError("pairwise", f"дубль финальной записи {slot.slot_id}")
            if slot.state == "finalized":
                continue
            if len(disk) == 1:
                slot.state = "finalized"
                slot.error = ""
                _write_slots(target, body, slots)
                continue
            slot.state = "running"
            _write_slots(target, body, slots)
            record = _safe_execute(runner_execute, slot)
            if record.get("provider_mismatch") is True:
                slot.state = "pending"
                slot.error = "фактический провайдер не совпал с настроенным"
                _write_slots(target, body, slots)
                provider_stopped = True
                _write_report(
                    data, section, repeats, prompt_variant, label, names, slots,
                    digest, cfg, cleanup, provider_stopped, partial=True,
                )
                break
            if record.get("record_finalized") is True:
                slot.state = "finalized"
                slot.error = ""
            else:
                slot.state = "pending"
                slot.error = str(record.get("error") or record.get("verdict") or "")[:500]
            _write_slots(target, body, slots)
            _write_report(
                data, section, repeats, prompt_variant, label, names, slots,
                digest, cfg, cleanup, provider_stopped, partial=True,
            )
    finally:
        if session is not None:
            cleanup = smoke_session.close_session(session)
    report = _write_report(
        data, section, repeats, prompt_variant, label, names, slots, digest, cfg,
        cleanup, provider_stopped, partial=False,
    )
    pending = sum(1 for slot in slots if slot.state != "finalized")
    cleanup_ok = bool(cleanup.get("ok"))
    if provider_stopped:
        code = 3
    elif pending > 0 or not cleanup_ok:
        code = 1
    else:
        code = 0
    report["exit_code"] = code
    report["ok"] = code == 0
    paths.write_atomic(
        report_path(data, label, partial=False),
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
    )
    return report


@beartype
def _docker_execute(
    session: smoke_session.SmokeSession | None,
    cfg: BenchmarkConfig,
    data: Path | None,
    prompt_variant: str,
    digest: str,
) -> Execute:
    def execute(slot: SlotPlan) -> dict[str, object]:
        if session is None:
            raise PipelineError("pairwise", "сессия Docker не открыта")
        try:
            return runner.run_one(
                slot.task,
                mode=slot.mode,
                config=cfg,
                data=data,
                smoke=session,
                prompt_variant=prompt_variant,
                pairwise={
                    "plan_sha256": digest,
                    "slot_id": slot.slot_id,
                    "task_index": slot.task_index,
                    "repetition": slot.repetition,
                },
            )
        except Exception as exc:  # noqa: BLE001
            timeout = type(exc).__name__ == "TimeoutExpired"
            return {
                "func": slot.task,
                "mode": slot.mode,
                "pass": False,
                "verdict": "ERROR",
                "record_finalized": False,
                "integration_failure": True,
                "agent_timeout": timeout,
                "error": f"{type(exc).__name__}: {exc}"[:500],
            }

    return execute


@beartype
def _safe_execute(execute: Execute, slot: SlotPlan) -> dict[str, object]:
    try:
        return execute(slot)
    except Exception as exc:  # noqa: BLE001
        return {
            "func": slot.task,
            "mode": slot.mode,
            "record_finalized": False,
            "verdict": "ERROR",
            "error": f"{type(exc).__name__}: {exc}"[:500],
        }


@beartype
def _finalized_records(data: Path | None, slot: SlotPlan) -> list[dict[str, object]]:
    found: list[dict[str, object]] = []
    folder = paths.runs_dir(data)
    for path in sorted(folder.glob(f"{slot.task}__{slot.mode}__*.json")):
        try:
            raw: object = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(raw, dict):
            continue
        record = cast("dict[str, object]", raw)
        if str(record.get("pairwise_slot_id") or "") != slot.slot_id:
            continue
        if record.get("record_finalized") is True:
            found.append(record)
    return found


@beartype
def _checkpoint_body(
    label: str,
    section: str,
    repeats: int,
    prompt_variant: str,
    names: list[str],
    slots: list[SlotPlan],
    digest: str,
    cfg: BenchmarkConfig,
    graph_hash: str,
    image_ids: dict[str, str],
) -> dict[str, object]:
    return {
        "label": label,
        "experiment_id": cfg.experiment_id,
        "section": section,
        "repeats": repeats,
        "prompt_variant": prompt_variant,
        "tasks": list(names),
        "holdout_sha256": split_module.holdout_sha256(names),
        "slots": [_slot_dict(slot) for slot in slots],
        "pairwise_plan_sha256": digest,
        "effective_config": cfg.to_manifest(),
        "git": git_snapshot(),
        "graph_sha256": graph_hash,
        "image_ids": dict(image_ids),
    }


@beartype
def _slot_dict(slot: SlotPlan) -> dict[str, object]:
    return {
        "slot_id": slot.slot_id,
        "task": slot.task,
        "mode": slot.mode,
        "repetition": slot.repetition,
        "task_index": slot.task_index,
        "state": slot.state,
        "error": slot.error,
    }


@beartype
def _write_slots(target: Path, body: dict[str, object], slots: list[SlotPlan]) -> None:
    body["slots"] = [_slot_dict(slot) for slot in slots]
    _save_checkpoint(target, body)


@beartype
def _save_checkpoint(target: Path, body: dict[str, object]) -> None:
    paths.write_atomic(target, json.dumps(body, ensure_ascii=False, indent=2) + "\n")


@beartype
def _load_checkpoint(target: Path) -> dict[str, object]:
    try:
        raw: object = json.loads(target.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise PipelineError("pairwise", "checkpoint не читается") from exc
    if not isinstance(raw, dict):
        raise PipelineError("pairwise", "checkpoint повреждён")
    return cast("dict[str, object]", raw)


@beartype
def _slots_from_checkpoint(raw: dict[str, object]) -> list[SlotPlan]:
    rows = raw.get("slots")
    if not isinstance(rows, list):
        raise PipelineError("pairwise", "в checkpoint нет списка слотов")
    slots: list[SlotPlan] = []
    items = cast("list[object]", rows)
    for item in items:
        if not isinstance(item, dict):
            raise PipelineError("pairwise", "слот checkpoint повреждён")
        row = cast("dict[str, object]", item)
        slots.append(
            SlotPlan(
                slot_id=str(row.get("slot_id") or ""),
                task=str(row.get("task") or ""),
                mode=str(row.get("mode") or ""),
                repetition=_as_int(row.get("repetition")),
                task_index=_as_int(row.get("task_index")),
                state=str(row.get("state") or "pending"),
                error=str(row.get("error") or ""),
            )
        )
    return slots


@beartype
def _as_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PipelineError("pairwise", "в слоте не целое поле")
    return value


@beartype
def _write_report(
    data: Path | None,
    section: str,
    repeats: int,
    prompt_variant: str,
    label: str,
    names: list[str],
    slots: list[SlotPlan],
    digest: str,
    cfg: BenchmarkConfig,
    cleanup: dict[str, object],
    provider_stopped: bool,
    partial: bool,
) -> dict[str, object]:
    report = eval_module.build_report(
        data,
        section if section != "frozen" else "holdout",
        repetitions=repeats,
        experiment_id=cfg.experiment_id,
    )
    report["section"] = section
    report["tasks"] = list(names)
    report["hands"] = list(HANDS)
    report["prompt_variant"] = prompt_variant
    report["experiment_id"] = cfg.experiment_id
    report["pairwise_plan_sha256"] = digest
    report["slots_total"] = len(slots)
    report["slots_finalized"] = sum(1 for slot in slots if slot.state == "finalized")
    report["slots_pending"] = sum(1 for slot in slots if slot.state != "finalized")
    report["provider_stopped"] = provider_stopped
    report["cleanup"] = cleanup
    report["partial"] = partial
    paths.write_atomic(
        report_path(data, label, partial=partial),
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
    )
    return report
