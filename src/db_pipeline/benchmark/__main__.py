"""CLI benchmark: ``python -m db_pipeline.benchmark``.

Команды:

- ``convert`` — построить публичные задачи и приватные oracle из карточек watchdog;
- ``doctor`` — проверить Docker, образ задачи, Langfuse health/keys и probe trace;
- ``build`` — собрать образ задачи, graph и model-gateway;
- ``run`` — один прогон задачи в заданном режиме (``bare`` или ``graph``);
- ``smoke`` — парный smoke на пяти задачах: 5 ``bare`` и 5 ``graph``;
- ``graph-index`` — построить локальный индекс графа или union-индекс пяти баз.

Smoke требует ровно пять заранее зарегистрированных задач и десять запусков в
заранее фиксированном counterbalanced порядке B/G, G/B, B/G, G/B, B/G. Единичная
ошибка не останавливает оставшиеся пары; итог всегда записывается. Graph-run без
MCP-вызова и без опубликованной трассы считается integration failure и даёт
ненулевой exit code. Run records immutable: повторный smoke не перезаписывает
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
from db_pipeline.environment import load_environment

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
    report["openrouter_key_present"] = cfg.openrouter_key_present
    print(json.dumps(report, ensure_ascii=False, indent=2))
    healthy = report["docker"] == "ok"
    health = report.get("langfuse_health")
    if isinstance(health, dict):
        from typing import cast

        health_map = cast("dict[str, object]", health)
        healthy = healthy and bool(health_map.get("ok"))
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
    record = runner.run_one(name, mode=args.mode, data=data, keep_container=args.keep)
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
) -> dict[str, object]:
    from db_pipeline.benchmark import runner
    from db_pipeline.benchmark import session as smoke_session
    from typing import cast

    session = cast("smoke_session.SmokeSession", active)
    try:
        return runner.run_one(name, mode=mode, data=data, smoke=session)
    except Exception as exc:  # noqa: BLE001 - единичная ошибка не останавливает пары
        return {
            "func": name,
            "mode": mode,
            "pass": False,
            "verdict": "ERROR",
            "integration_failure": True,
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
        and len(graph_without_mcp) == 0
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
def cmd_graph_index(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark.graph import index as graph_index

    data = None if args.data is None else Path(args.data)
    if args.union:
        bases = _smoke_bases(data)
        summary = graph_index.build_union_index(
            data=data,
            base_shas=bases,
            per_base=args.per_base,
        )
    else:
        summary = graph_index.build_index(
            data=data,
            limit=args.limit,
            base_sha=args.base_sha,
        )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("ok") else 1


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
def cmd_graph_history(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark.graph import history

    data = None if args.data is None else Path(args.data)
    report = history.build(
        data=data,
        rebuild=not args.no_rebuild,
        index_errors=not args.no_errors,
        base_shas=args.bases,
    )
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0


@beartype
def cmd_graph_canary(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark.graph import canary, split as split_module

    data = None if args.data is None else Path(args.data)
    names = _task_names(args.tasks, data)
    if len(args.tasks) == 0:
        names = split_module.derive_split(data)["dev"]
    layer = args.layer
    reports = [
        canary.run_task(name, data, with_history=layer in ("auto", "v2"))
        for name in names
    ]
    target = paths.reports_dir(data) / "graph-canary.json"
    target.write_text(json.dumps(reports, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    return 0 if all(bool(report["ok"]) for report in reports) else 1


@beartype
def cmd_pairwise(args: argparse.Namespace) -> int:
    """Парный прогон ``bare`` vs ``graph`` по секции split с counterbalanced порядком.

    Порядок рук чередуется по задачам (``bare`` первым на чётных, ``graph`` — на
    нечётных) и по повторам, чтобы дрейф модели и кэш не благоприятствовали одной
    руке. Ошибка одной пары не останавливает остальные: запись об ошибке всё
    равно попадает в отчёт.
    """
    from db_pipeline.benchmark import eval as eval_module
    from db_pipeline.benchmark import session as smoke_session
    from db_pipeline.benchmark.graph import split as split_module

    data = None if args.data is None else Path(args.data)
    names = _pairwise_names(args, data)
    if len(names) == 0:
        print(json.dumps({"error": f"секция {args.section} пуста"}, ensure_ascii=False))
        return 2
    cfg = load_config()
    guard = _budget_guard(args.no_budget_check)
    if not guard["ok"]:
        print(json.dumps({"error": "бюджет OpenRouter исчерпан", "budget": guard},
                         ensure_ascii=False))
        return 2
    session = smoke_session.open_session(cfg, data)
    records: list[dict[str, object]] = []
    try:
        for index, name in enumerate(names):
            for repetition in range(args.repeats):
                order = _hand_order(index, repetition)
                for mode in order:
                    records.append(_smoke_run(name, mode, data, session))
    finally:
        cleanup = smoke_session.close_session(session)
    # Отчёт считается по записям этого прогона, а не по всему ``data/runs``:
    # иначе probe смешался бы со старым tune и повторы перенумеровались.
    report = eval_module.build_report(data, args.section, records=records)
    report["section"] = args.section
    report["tasks"] = names
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
def _hand_order(task_index: int, repetition: int) -> tuple[str, str]:
    """Чередование рук: чётная пара даёт ``bare`` первым, нечётная — ``graph``."""
    first = (task_index + repetition) % 2
    return ("bare", "graph") if first == 0 else ("graph", "bare")


@beartype
def _budget_guard(skip: bool) -> dict[str, object]:
    """Проверяет бюджет OpenRouter перед batch, если его не отключили флагом."""
    if skip:
        return {"ok": True, "skipped": True}
    report = budget.check_budget(os.environ.get("OPENROUTER_API_KEY", ""))
    return report.to_dict()


@beartype
def cmd_eval(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark import eval as eval_module

    data = None if args.data is None else Path(args.data)
    report = eval_module.build_report(data, args.section)
    target = eval_module.write_report(data, args.section)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\nотчёт: {target}")
    return 0


@beartype
def _smoke_bases(data: Path | None) -> list[str]:
    bases: list[str] = []
    for name in (f"postgres__{item}" for item in SMOKE_TASKS):
        oracle = convert.load_oracle(name, data)
        bases.append(str(oracle["base"]))
    return bases


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
    run_parser.add_argument("--mode", choices=("bare", "graph"), default="bare")
    run_parser.add_argument("--keep", action="store_true", help="не удалять контейнер")
    run_parser.set_defaults(handler=cmd_run)

    smoke_parser = sub.add_parser("smoke", help="парный smoke bare/graph на пяти задачах")
    smoke_parser.add_argument("tasks", nargs="*")
    smoke_parser.set_defaults(handler=cmd_smoke)

    probe_parser = sub.add_parser("probe", help="дешёвый E2E probe gateway и MCP")
    probe_parser.add_argument("--name", default="", help="функция контрольной задачи")
    probe_parser.set_defaults(handler=cmd_probe)

    graph_parser = sub.add_parser("graph-index", help="построить локальный индекс графа")
    graph_parser.add_argument("--limit", type=int, default=2000)
    graph_parser.add_argument("--base-sha", default="")
    graph_parser.add_argument(
        "--union", action="store_true", help="union-индекс последних предков пяти base_sha"
    )
    graph_parser.add_argument("--per-base", type=int, default=50)
    graph_parser.set_defaults(handler=cmd_graph_index)

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

    history_parser = sub.add_parser(
        "graph-history", help="собрать производный history.sqlite из данных PRISM"
    )
    history_parser.add_argument(
        "--no-rebuild", action="store_true", help="докидывать записи, не очищая таблицы"
    )
    history_parser.add_argument(
        "--no-errors", action="store_true", help="не индексировать шаблоны errmsg"
    )
    history_parser.add_argument("--bases", nargs="*", default=None, help="base_sha для errmsg")
    history_parser.set_defaults(handler=cmd_graph_history)

    canary_parser = sub.add_parser(
        "graph-canary", help="прогон канареек и membership по инструментам графа"
    )
    canary_parser.add_argument(
        "--layer", default="auto", choices=("auto", "v1", "v2"), help="слой инструментов"
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
    pairwise_parser.set_defaults(handler=cmd_pairwise)

    eval_parser = sub.add_parser("eval", help="отчёт McNemar по сохранённым прогонам")
    eval_parser.add_argument(
        "--section", default="holdout", choices=("dev", "tune", "holdout", "frozen")
    )
    eval_parser.set_defaults(handler=cmd_eval)

    return parser


@beartype
def main(argv: list[str] | None = None) -> int:
    load_environment()
    args = build_parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
