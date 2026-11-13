"""CLI benchmark: ``python -m db_pipeline.benchmark``.

Команды:

- ``convert`` — построить публичные задачи и приватные oracle из карточек watchdog;
- ``doctor`` — проверить Docker, образ задачи, Langfuse health/keys и probe trace;
- ``build`` — собрать образ задачи, graph и model-gateway;
- ``run`` — один прогон задачи в заданном режиме (``bare`` или ``graph``);
- ``smoke`` — парный smoke на пяти задачах: 5 ``bare`` и 5 ``graph``;
- ``graph-build`` — собрать единый ``graph.sqlite`` по всем базам (инкрементально по oid);
- ``release`` — публичная выгрузка задач со стабильными id, лицензиями и
  sha256-манифестом; ``release-verify`` — сверка распакованного релиза с манифестом.

Smoke требует ровно пять заранее зарегистрированных задач и десять запусков в
заранее фиксированном counterbalanced порядке B/G, G/B, B/G, G/B, B/G. Единичная
ошибка не останавливает оставшиеся пары; итог всегда записывается. Graph-run
без доставленной сводки или без отсечки считается integration failure. Вердикт
тестов при этом не переписывается. Run records immutable: повторный smoke не перезаписывает
предыдущие результаты.

CLI harvesting в ``db_pipeline.__main__`` не затрагивается.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark import budget, container, convert, paths, security
from db_pipeline.benchmark import telemetry
from db_pipeline.benchmark.config import load_config, missing_langfuse_vars
from db_pipeline.benchmark.eval import COMPARISON_LEFT, COMPARISON_RIGHT
from db_pipeline.benchmark.runner import (
    GRAPH_MODES,
    PROMPT_DEFAULT,
    PROMPT_VARIANTS,
)
from db_pipeline.environment import load_environment
from db_pipeline.errors import PipelineError

SMOKE_TASKS = (
    "d764d61af316",
    "d8444163d22e",
    "96ab9a990eed",
    "3b120b1e94dd",
    "9b917a93116e",
)

# Counterbalanced порядок: на каждой нечётной задаче bare идёт первым, на чётной —
# graph. Итого 5 bare и 5 graph, без reuse сессий.
SMOKE_ORDER: tuple[tuple[str, str], ...] = (
    ("bare", "graph"),
    ("graph", "bare"),
    ("bare", "graph"),
    ("graph", "bare"),
    ("bare", "graph"),
)


@beartype
def cmd_convert(args: argparse.Namespace) -> int:
    data = None if args.data is None else Path(args.data)
    instances = convert.load_instances(data)
    if len(args.name) > 0:
        wanted = _wanted_names(args.name)
        instances = [item for item in instances if convert.func_name(item) in wanted]
    written = 0
    failures: list[dict[str, str]] = []
    for instance in instances:
        try:
            payload = convert.convert_one(instance, data)
            convert.write_task(payload, data)
            written += 1
        except Exception as exc:  # noqa: BLE001
            failures.append({"func": convert.func_name(instance), "error": str(exc)[:300]})
    report = {"written": written, "failures": failures[:20]}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if len(failures) == 0 else 1


@beartype
def _wanted_names(values: list[str]) -> set[str]:
    found: set[str] = set()
    for value in values:
        if len(value) == 12:
            found.add(f"postgres__{value}")
        else:
            found.add(value)
    return found


@beartype
def cmd_validate(args: argparse.Namespace) -> int:
    data = None if args.data is None else Path(args.data)
    names = _task_names(args.name, data)
    results: list[dict[str, object]] = []
    checks: list[bool] = []
    for name in names:
        public = security.validate_public(name, data)
        source = security.validate_source(name, data)
        results.append({"func": name, "public": public.to_dict(), "source": source.to_dict()})
        checks.append(public.ok and source.ok)
    ok = all(checks)
    print(json.dumps({"ok": ok, "results": results}, ensure_ascii=False, indent=2))
    return 0 if ok else 1


@beartype
def cmd_doctor(args: argparse.Namespace) -> int:
    cfg = load_config()
    report: dict[str, object] = {}
    report["docker"] = container.docker_available() or "ok"
    report["image"] = cfg.image
    report["graph_image"] = cfg.graph_image
    report["gateway_image"] = cfg.gateway_image
    missing = missing_langfuse_vars()
    report["langfuse_missing"] = missing
    if len(missing) == 0:
        ok, detail = telemetry.health(cfg.otel)
        report["langfuse_health"] = {"ok": ok, "detail": detail}
        if ok and not args.no_probe:
            report["langfuse_probe"] = telemetry.probe(cfg.otel)
    report["model"] = cfg.model
    report["context_length"] = cfg.context_length
    report["max_tokens"] = cfg.max_tokens
    report["experiment_id"] = cfg.experiment_id
    report["openrouter_key_present"] = cfg.openrouter_key_present
    if cfg.openrouter_key_present:
        observed = budget.model_context_length(os.environ.get("OPENROUTER_API_KEY", ""), cfg.model)
        report["openrouter_context_length"] = observed
        report["context_length_ok"] = (
            observed is not None and cfg.context_length <= observed
        )
        if observed is not None and cfg.context_length < observed:
            report["context_window_note"] = (
                f"заданное окно {cfg.context_length} меньше окна модели {observed}"
            )
    if len(missing) == 0:
        project_ok, project_detail = telemetry.project_matches(cfg.otel, cfg.langfuse_project)
        report["langfuse_project"] = {
            "expected": cfg.langfuse_project,
            "ok": project_ok,
            "detail": project_detail,
        }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    healthy = report["docker"] == "ok"
    health = report.get("langfuse_health")
    if isinstance(health, dict):
        from typing import cast

        health_map = cast("dict[str, object]", health)
        healthy = healthy and bool(health_map.get("ok"))
    project = report.get("langfuse_project")
    if isinstance(project, dict):
        from typing import cast

        healthy = healthy and bool(cast("dict[str, object]", project).get("ok"))
    if "context_length_ok" in report:
        healthy = healthy and bool(report["context_length_ok"])
    return 0 if healthy else 1


@beartype
def cmd_build(args: argparse.Namespace) -> int:
    cfg = load_config()
    context = "" if args.context == "" else args.context
    for dockerfile, tag in (
        ("task.Dockerfile", cfg.image),
        ("graph.Dockerfile", cfg.graph_image),
        ("model-gateway/Dockerfile", cfg.gateway_image),
    ):
        command = container.docker_command(
            "build",
            "-f",
            str(paths.docker_dir() / dockerfile),
            "-t",
            tag,
            ".",
            context=context,
        )
        print(" ".join(command), flush=True)
        code = _run(command, cwd=str(paths.project_root()))
        if code != 0:
            return code
    return 0


@beartype
def _run(command: list[str], cwd: str) -> int:
    import subprocess

    return subprocess.run(command, cwd=cwd, check=False).returncode


@beartype
def cmd_run(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark import runner

    data = None if args.data is None else Path(args.data)
    name = _task_names([args.name], data)[0]
    record = runner.run_one(
        name,
        mode=args.mode,
        data=data,
        keep_container=args.keep,
        prompt_variant=args.prompt_variant,
    )
    print(json.dumps(record, ensure_ascii=False, indent=2))
    return 0 if record.get("pass") else 1


@beartype
def cmd_probe(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark import probe

    data = None if args.data is None else Path(args.data)
    cfg = load_config()
    report = probe.probe_task(cfg, data, name=args.name)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("ok") else 1


@beartype
def cmd_smoke(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark import session as smoke_session

    data = None if args.data is None else Path(args.data)
    names = _smoke_names(args.tasks, data)
    if len(names) != len(SMOKE_ORDER):
        print(
            json.dumps(
                {"error": f"нужно ровно {len(SMOKE_ORDER)} задач, найдено {len(names)}"},
                ensure_ascii=False,
            )
        )
        return 2
    cfg = load_config()
    active = smoke_session.open_session(cfg, data)
    records: list[dict[str, object]] = []
    try:
        for index, name in enumerate(names):
            for mode in SMOKE_ORDER[index]:
                records.append(_smoke_run(name, mode, data, active))
    finally:
        cleanup = smoke_session.close_session(active)
    report = _smoke_report(records, cleanup)
    report_path = paths.reports_dir(data) / "smoke-postgres.json"
    report_path.write_text(
        json.dumps({"tasks": records, "summary": report}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["ok"]:
        return 1
    return 0


@beartype
def _smoke_run(
    name: str,
    mode: str,
    data: Path | None,
    active: object,
    prompt_variant: str = PROMPT_DEFAULT,
) -> dict[str, object]:
    from db_pipeline.benchmark import runner
    from db_pipeline.benchmark import session as smoke_session
    from typing import cast

    session = cast("smoke_session.SmokeSession", active)
    try:
        return runner.run_one(
            name, mode=mode, data=data, smoke=session, prompt_variant=prompt_variant
        )
    except Exception as exc:  # noqa: BLE001 - единичная ошибка не останавливает пары
        from datetime import UTC, datetime

        timeout = type(exc).__name__ == "TimeoutExpired"
        return {
            "func": name,
            "mode": mode,
            "started": datetime.now(UTC).isoformat(timespec="seconds"),
            "pass": False,
            "verdict": "ERROR",
            "integration_failure": True,
            "agent_timeout": timeout,
            "completion": {"outcome": "timeout" if timeout else "crash_unknown"},
            "error": f"{type(exc).__name__}: {exc}"[:500],
        }


@beartype
def _smoke_report(
    records: list[dict[str, object]],
    cleanup: dict[str, object],
) -> dict[str, object]:
    total = len(records)
    passed = sum(1 for item in records if item.get("pass"))
    bare = [item for item in records if item.get("mode") == "bare"]
    graph = [item for item in records if item.get("mode") == "graph"]
    graph_without_mcp = [
        item.get("func") for item in graph if not item.get("mcp_calls")
    ]
    integration_failures = [
        item.get("func")
        for item in records
        if item.get("integration_failure") or item.get("verdict") == "ERROR"
    ]
    cleanup_ok = bool(cleanup.get("ok"))
    ok = (
        total == len(SMOKE_ORDER) * 2
        and len(integration_failures) == 0
        and cleanup_ok
    )
    return {
        "runs": total,
        "pass@1": round(passed / total, 4) if total else 0.0,
        "bare_pass": sum(1 for item in bare if item.get("pass")),
        "graph_pass": sum(1 for item in graph if item.get("pass")),
        "mcp_calls": [item.get("mcp_calls") for item in graph],
        "graph_run_without_mcp": graph_without_mcp,
        "integration_failures": integration_failures,
        "cleanup": cleanup,
        "ok": ok,
    }


@beartype
def _smoke_names(values: list[str], data: Path | None) -> list[str]:
    if len(values) > 0:
        return [f"postgres__{value}" if len(value) == 12 else value for value in values]
    available = {path.stem for path in paths.tasks_dir(data).glob("*.json")}
    return [name for name in (f"postgres__{item}" for item in SMOKE_TASKS) if name in available]


@beartype
def cmd_graph_build(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark.graph import gitgraph

    data = None if args.data is None else Path(args.data)
    bases = args.bases if len(args.bases) > 0 else None
    try:
        summary = gitgraph.build(data=data, bases=bases, fetch=args.fetch)
    except PipelineError as exc:
        print(json.dumps({"ok": False, "error": exc.reason}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("ok") else 1


@beartype
def cmd_graph_coverage(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark.graph import coverage as coverage_module

    data = None if args.data is None else Path(args.data)
    names = _task_names(args.tasks, data) if len(args.tasks) > 0 else None
    report = coverage_module.coverage(data, names)
    target = coverage_module.write_report(report, data)
    summary = {key: report[key] for key in ("tasks", "passed", "failed", "ok")}
    summary["report"] = str(target)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if report.get("ok") else 1


@beartype
def cmd_graph_split(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark.graph import split as split_module

    data = None if args.data is None else Path(args.data)
    split = split_module.derive_split(data)
    report: dict[str, object] = {
        "sizes": {section: len(split[section]) for section in split_module.SECTIONS},
        "holdout_sha256": split_module.holdout_sha256(split["holdout"]),
        "tune": split["tune"],
        "dev": split["dev"],
    }
    if args.freeze:
        target = split_module.write_holdout(split)
        report["frozen"] = str(target)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


@beartype
def cmd_graph_leak_audit(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark.graph import leakguard

    data = None if args.data is None else Path(args.data)
    report = leakguard.audit_split(
        data, args.section, progress_path=leakguard.progress_file(data, args.section)
    )
    target = paths.reports_dir(data) / f"leak-audit-{args.section}.json"
    target.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("ok") else 1


@beartype
def cmd_graph_canary(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark.graph import canary, split as split_module

    data = None if args.data is None else Path(args.data)
    names = _task_names(args.tasks, data)
    if len(args.tasks) == 0:
        names = split_module.derive_split(data)["dev"]
    reports = [canary.run_task(name, data) for name in names]
    target = paths.reports_dir(data) / "graph-canary.json"
    target.write_text(json.dumps(reports, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    return 0 if all(bool(report["ok"]) for report in reports) else 1


@beartype
def cmd_pairwise(args: argparse.Namespace) -> int:
    """Парный прогон ``bare`` против ``graph`` с counterbalanced порядком.

    Порядок рук чередуется по задачам и по повторам. Ошибка одной пары не
    останавливает остальные: запись об ошибке всё равно попадает в отчёт.
    Отчёт фильтруется по ``experiment_id`` из конфига.
    """
    from db_pipeline.benchmark import eval as eval_module
    from db_pipeline.benchmark import session as smoke_session

    data = None if args.data is None else Path(args.data)
    names = _pairwise_names(args, data)
    if len(names) == 0:
        print(json.dumps({"error": f"секция {args.section} пуста"}, ensure_ascii=False))
        return 2
    hands = (eval_module.COMPARISON_LEFT, eval_module.COMPARISON_RIGHT)
    cfg = load_config()
    guard = _budget_guard(args.no_budget_check, cfg.max_tokens)
    if not guard["ok"]:
        print(json.dumps({"error": "бюджет OpenRouter исчерпан", "budget": guard},
                         ensure_ascii=False))
        return 2
    session = smoke_session.open_session(cfg, data)
    records: list[dict[str, object]] = []
    try:
        for index, name in enumerate(names):
            for repetition in range(args.repeats):
                order = _hand_order(index, repetition, hands)
                for mode in order:
                    records.append(
                        _smoke_run(name, mode, data, session, args.prompt_variant)
                    )
    finally:
        cleanup = smoke_session.close_session(session)
    # Отчёт считается по записям этого прогона, а не по всему ``data/runs``:
    # иначе probe смешался бы со старым tune и повторы перенумеровались.
    report = eval_module.build_report(
        data,
        args.section,
        repetitions=args.repeats,
        records=records,
        modes=hands,
        experiment_id=cfg.experiment_id,
    )
    report["section"] = args.section
    report["tasks"] = names
    report["hands"] = list(hands)
    report["prompt_variant"] = args.prompt_variant
    report["experiment_id"] = cfg.experiment_id
    report["budget"] = guard
    report["cleanup"] = cleanup
    label = args.report_label or args.section
    target = paths.reports_dir(data) / f"pairwise-{label}.json"
    target.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    cleanup_ok = bool(cleanup.get("ok"))
    return 0 if cleanup_ok else 1


@beartype
def _pairwise_names(args: argparse.Namespace, data: Path | None) -> list[str]:
    """Список задач: явные ``--tasks`` либо секция split (``frozen`` — holdout)."""
    from db_pipeline.benchmark.graph import split as split_module

    if len(args.tasks) > 0:
        return _task_names(args.tasks, data)
    if args.section == "frozen":
        return split_module.read_holdout()
    return split_module.derive_split(data)[args.section]


@beartype
def _hand_order(
    task_index: int,
    repetition: int,
    hands: tuple[str, str] = (COMPARISON_LEFT, COMPARISON_RIGHT),
) -> tuple[str, str]:
    """Чередование рук: чётная пара даёт левую руку первой, нечётная — правую."""
    first = (task_index + repetition) % 2
    return hands if first == 0 else (hands[1], hands[0])


@beartype
def _budget_guard(skip: bool, max_tokens: int) -> dict[str, object]:
    """Проверяет бюджет OpenRouter перед batch, если его не отключили флагом."""
    if skip:
        return {"ok": True, "skipped": True}
    report = budget.check_budget(
        os.environ.get("OPENROUTER_API_KEY", ""),
        max_tokens=max_tokens,
    )
    return report.to_dict()


@beartype
def cmd_spec_audit(args: argparse.Namespace) -> int:
    """Помечает недоопределённые задачи tune и holdout. Эталон агенту не виден."""
    from db_pipeline.benchmark import spec_audit
    from db_pipeline.benchmark.graph import split as split_module

    data = None if args.data is None else Path(args.data)
    split = split_module.derive_split(data)
    names = list(split["tune"]) + list(split_module.read_holdout())
    report = spec_audit.audit_names(names, data)
    target = spec_audit.write_report(report, data)
    flagged = report["underspecified"]
    print(
        json.dumps(
            {
                "path": str(target),
                "tasks_total": report["tasks_total"],
                "underspecified": flagged,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


@beartype
def cmd_eval(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark import eval as eval_module

    data = None if args.data is None else Path(args.data)
    experiment_id = args.experiment_id or load_config().experiment_id
    report = eval_module.build_report(data, args.section, experiment_id=experiment_id)
    target = eval_module.write_report(data, args.section, experiment_id=experiment_id)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\nотчёт: {target}")
    return 0


@beartype
def cmd_causes(args: argparse.Namespace) -> int:
    """Пер-прогонный агрегатор причин: ``runs`` + ``oracle`` → ``cause``."""
    from db_pipeline.benchmark import causes

    data = None if args.data is None else Path(args.data)
    modes = tuple(args.mode) if len(args.mode) > 0 else ()
    experiment_id = args.experiment_id or load_config().experiment_id
    report = causes.build_cause_report(
        data,
        modes=modes,
        prompt_variant=args.prompt_variant,
        experiment_id=experiment_id,
    )
    target = paths.reports_dir(data) / f"{args.report_label}.json"
    target.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
    print(f"\nотчёт: {target}")
    return 0


@beartype
def cmd_graph_briefing_audit(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark.graph import briefing_audit, split as split_module

    data = None if args.data is None else Path(args.data)
    excluded = set(_task_names(args.exclude, data))
    split = split_module.derive_split(data)
    names = [
        name
        for section in ("dev", "tune")
        for name in split[section]
        if name not in excluded
    ]
    report = briefing_audit.audit_tasks(names, data)
    report["excluded"] = sorted(excluded)
    if len(args.canary) > 0:
        canary_names = _task_names(args.canary, data)
        report["canary"] = briefing_audit.canary_briefings(canary_names, data)
    target = briefing_audit.write_audit(report, data)
    print(json.dumps({"path": str(target), "mean_recall": report["mean_recall"], "tasks": report["tasks"]}, ensure_ascii=False, indent=2))
    canary = report.get("canary")
    if isinstance(canary, list):
        from typing import cast

        for item in cast("list[object]", canary):
            if not isinstance(item, dict):
                continue
            row = cast("dict[str, object]", item)
            if row.get("ok") is not True:
                return 1
    return 0


@beartype
def cmd_trace_dump(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark.graph import trace_dump

    data = None if args.data is None else Path(args.data)
    cfg = load_config()
    experiment_id = args.experiment_id or cfg.experiment_id
    report = trace_dump.dump_experiment(cfg, data, experiment_id, args.report_label)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    written = report["written"]
    return 0 if isinstance(written, int) else 1


@beartype
def cmd_release(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark import release as release_module

    data = None if args.data is None else Path(args.data)
    target = None if args.target == "" else Path(args.target)
    try:
        manifest = release_module.build(
            data=data,
            target=target,
            version=args.version,
            with_oracle=args.with_oracle,
            force=args.force,
        )
    except PipelineError as exc:
        print(json.dumps({"error": exc.reason}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


@beartype
def cmd_release_verify(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark import release as release_module

    target = Path(args.target)
    try:
        report = release_module.verify(target)
    except PipelineError as exc:
        print(json.dumps({"error": exc.reason, "ok": False}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("ok") else 1


@beartype
def _task_names(values: list[str], data: Path | None) -> list[str]:
    if len(values) == 0:
        available = sorted(path.stem for path in paths.tasks_dir(data).glob("*.json"))
        return available
    return [f"postgres__{value}" if len(value) == 12 else value for value in values]


@beartype
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="db_pipeline.benchmark")
    parser.add_argument("--data", default=None, help="каталог локальных данных")
    sub = parser.add_subparsers(dest="command", required=True)

    convert_parser = sub.add_parser("convert", help="watchdog → commit-suite")
    convert_parser.add_argument("name", nargs="*", help="sha или func")
    convert_parser.set_defaults(handler=cmd_convert)

    validate_parser = sub.add_parser("validate", help="проверить публичную часть и источник")
    validate_parser.add_argument("name", nargs="*")
    validate_parser.set_defaults(handler=cmd_validate)

    doctor_parser = sub.add_parser("doctor", help="проверить Docker и Langfuse")
    doctor_parser.add_argument("--no-probe", action="store_true")
    doctor_parser.set_defaults(handler=cmd_doctor)

    build_parser_cmd = sub.add_parser("build", help="собрать образы задачи, графа и gateway")
    build_parser_cmd.add_argument("--context", default="")
    build_parser_cmd.set_defaults(handler=cmd_build)

    run_parser = sub.add_parser("run", help="один прогон задачи")
    run_parser.add_argument("name")
    run_parser.add_argument(
        "--mode", choices=("bare", *GRAPH_MODES), default="bare"
    )
    run_parser.add_argument(
        "--prompt-variant",
        choices=PROMPT_VARIANTS,
        default=PROMPT_DEFAULT,
        help="формулировка абзаца об инструментах: directive или situational",
    )
    run_parser.add_argument("--keep", action="store_true", help="не удалять контейнер")
    run_parser.set_defaults(handler=cmd_run)

    smoke_parser = sub.add_parser("smoke", help="парный smoke bare/graph на пяти задачах")
    smoke_parser.add_argument("tasks", nargs="*")
    smoke_parser.set_defaults(handler=cmd_smoke)

    probe_parser = sub.add_parser("probe", help="дешёвый E2E probe gateway и MCP")
    probe_parser.add_argument("--name", default="", help="функция контрольной задачи")
    probe_parser.set_defaults(handler=cmd_probe)

    graph_build = sub.add_parser(
        "graph-build", help="собрать единый graph.sqlite по базам задач"
    )
    graph_build.add_argument("--fetch", action="store_true", help="git fetch origin master")
    graph_build.add_argument("--bases", nargs="*", default=[], help="явные base sha")
    graph_build.set_defaults(handler=cmd_graph_build)

    coverage_parser = sub.add_parser(
        "graph-coverage", help="гейт покрытия графа: структура и 7 инструментов"
    )
    coverage_parser.add_argument("tasks", nargs="*", help="ограничить задачи")
    coverage_parser.set_defaults(handler=cmd_graph_coverage)

    split_parser = sub.add_parser(
        "graph-split", help="разбить задачи на dev/tune/holdout и заморозить holdout"
    )
    split_parser.add_argument(
        "--freeze", action="store_true", help="записать holdout в constraints/"
    )
    split_parser.set_defaults(handler=cmd_graph_split)

    leak_parser = sub.add_parser(
        "graph-leak-audit", help="leak-guard: membership, prior-twin, канарейки"
    )
    leak_parser.add_argument(
        "--section", default="holdout", choices=("dev", "tune", "holdout")
    )
    leak_parser.set_defaults(handler=cmd_graph_leak_audit)

    canary_parser = sub.add_parser(
        "graph-canary", help="прогон канареек и membership по инструментам графа"
    )
    canary_parser.add_argument("tasks", nargs="*", help="имена задач (по умолчанию dev)")
    canary_parser.set_defaults(handler=cmd_graph_canary)

    pairwise_parser = sub.add_parser(
        "pairwise", help="парный прогон bare vs graph по секции split"
    )
    pairwise_parser.add_argument(
        "--section", default="tune", choices=("dev", "tune", "holdout", "frozen")
    )
    pairwise_parser.add_argument("--repeats", type=int, default=2)
    pairwise_parser.add_argument(
        "--tasks",
        nargs="*",
        default=[],
        help="явные задачи (sha или func) вместо секции split",
    )
    pairwise_parser.add_argument(
        "--report-label",
        default="",
        help="имя файла отчёта (по умолчанию равно секции)",
    )
    pairwise_parser.add_argument(
        "--no-budget-check",
        action="store_true",
        help="не проверять остаток кредитов OpenRouter перед batch",
    )
    pairwise_parser.add_argument(
        "--prompt-variant",
        choices=PROMPT_VARIANTS,
        default=PROMPT_DEFAULT,
        help="формулировка абзаца об инструментах: directive (нейтральная) или situational",
    )
    pairwise_parser.set_defaults(handler=cmd_pairwise)

    spec_parser = sub.add_parser(
        "spec-audit",
        help="пометить недоопределённые задачи tune и holdout",
    )
    spec_parser.set_defaults(handler=cmd_spec_audit)

    eval_parser = sub.add_parser("eval", help="отчёт McNemar по сохранённым прогонам")
    eval_parser.add_argument(
        "--section", default="holdout", choices=("dev", "tune", "holdout", "frozen")
    )
    eval_parser.add_argument(
        "--experiment-id",
        default="",
        help="фильтр партии; пусто — BENCH_EXPERIMENT_ID",
    )
    eval_parser.set_defaults(handler=cmd_eval)

    causes_parser = sub.add_parser(
        "causes", help="пер-прогонный агрегатор причин: runs + oracle → cause"
    )
    causes_parser.add_argument(
        "--mode",
        nargs="*",
        default=[],
        help="сузить руки (bare, graph); пусто — все",
    )
    causes_parser.add_argument(
        "--prompt-variant",
        default="",
        help="сузить вариант промпта (directive/situational); пусто — все",
    )
    causes_parser.add_argument(
        "--report-label",
        default="causes",
        help="имя файла отчёта (по умолчанию causes)",
    )
    causes_parser.add_argument("--experiment-id", default="")
    causes_parser.set_defaults(handler=cmd_causes)

    briefing_parser = sub.add_parser(
        "graph-briefing-audit",
        help="recall сводки графа на dev и tune без пилотных задач",
    )
    briefing_parser.add_argument(
        "--exclude",
        nargs="*",
        default=[],
        help="задачи, которые не входят в аудит (пилотные)",
    )
    briefing_parser.add_argument(
        "--canary",
        nargs="*",
        default=[],
        help="задачи, по тексту сводки которых гоняются канарейки",
    )
    briefing_parser.set_defaults(handler=cmd_graph_briefing_audit)

    trace_parser = sub.add_parser(
        "trace-dump", help="выгрузить traces и observations Langfuse"
    )
    trace_parser.add_argument("--experiment-id", default="")
    trace_parser.add_argument("--report-label", default="pilot-2026-11")
    trace_parser.set_defaults(handler=cmd_trace_dump)

    release_parser = sub.add_parser(
        "release", help="публичная выгрузка задач: стабильные id, лицензии, sha256-манифест"
    )
    release_parser.add_argument(
        "--target", default="", help="каталог выгрузки (по умолчанию data/releases/<version>)"
    )
    release_parser.add_argument("--version", default="0.1.0", help="версия релиза")
    release_parser.add_argument(
        "--with-oracle", action="store_true", help="включить приватный слой oracle"
    )
    release_parser.add_argument(
        "--force", action="store_true", help="перезаписать непустой каталог"
    )
    release_parser.set_defaults(handler=cmd_release)

    verify_parser = sub.add_parser(
        "release-verify", help="сверить распакованный релиз с манифестом"
    )
    verify_parser.add_argument("--target", required=True, help="каталог релиза")
    verify_parser.set_defaults(handler=cmd_release_verify)

    return parser


@beartype
def main(argv: list[str] | None = None) -> int:
    load_environment()
    args = build_parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
