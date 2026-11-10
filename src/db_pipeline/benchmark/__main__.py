"""CLI benchmark: ``python -m db_pipeline.benchmark``.

Команды:

- ``convert`` — построить публичные задачи и приватные oracle из карточек watchdog;
- ``doctor`` — проверить Docker, образ задачи, Langfuse health/keys и probe trace;
- ``build`` — собрать образ задачи и model-gateway;
- ``run`` — один прогон задачи в заданном режиме (``bare`` или ``graph``);
- ``smoke`` — парный smoke на наборе задач (bare и graph);
- ``graph-index`` — построить локальный индекс графа с отсечкой.

CLI harvesting в ``db_pipeline.__main__`` не затрагивается.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark import container, convert, paths, security
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
    print(json.dumps(_public(record), ensure_ascii=False, indent=2))
    return 0 if record.get("pass") else 1


@beartype
def cmd_smoke(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark import runner

    data = None if args.data is None else Path(args.data)
    names = _smoke_names(args.tasks, data)
    records: list[dict[str, object]] = []
    for name in names:
        for mode in ("bare", "graph"):
            record = runner.run_one(name, mode=mode, data=data)
            records.append(_public(record))
    report = _smoke_report(records)
    report_path = paths.reports_dir(data) / "smoke-postgres.json"
    report_path.write_text(
        json.dumps({"tasks": records, "summary": report}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


@beartype
def _smoke_names(values: list[str], data: Path | None) -> list[str]:
    if len(values) > 0:
        return [f"postgres__{value}" if len(value) == 12 else value for value in values]
    available = {path.stem for path in paths.tasks_dir(data).glob("*.json")}
    return [name for name in (f"postgres__{item}" for item in SMOKE_TASKS) if name in available]


@beartype
def _smoke_report(records: list[dict[str, object]]) -> dict[str, object]:
    total = len(records)
    passed = sum(1 for item in records if item.get("pass"))
    bare = [item for item in records if item.get("mode") == "bare"]
    graph = [item for item in records if item.get("mode") == "graph"]
    return {
        "runs": total,
        "pass@1": round(passed / total, 4) if total else 0.0,
        "bare_pass": sum(1 for item in bare if item.get("pass")),
        "graph_pass": sum(1 for item in graph if item.get("pass")),
        "mcp_calls": [item.get("mcp_calls") for item in graph],
        "graph_run_without_mcp": [
            item.get("func") for item in graph if not item.get("mcp_calls")
        ],
    }


@beartype
def cmd_graph_index(args: argparse.Namespace) -> int:
    from db_pipeline.benchmark.graph import index as graph_index

    data = None if args.data is None else Path(args.data)
    summary = graph_index.build_index(
        data=data,
        limit=args.limit,
        base_sha=args.base_sha,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


@beartype
def _public(record: dict[str, object]) -> dict[str, object]:
    return {
        key: record.get(key)
        for key in (
            "func",
            "mode",
            "pass",
            "verdict",
            "mcp_calls",
            "tool_counts",
            "diff_size",
            "seconds",
            "trace_id",
        )
    }


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

    build_parser_cmd = sub.add_parser("build", help="собрать образы")
    build_parser_cmd.add_argument("--context", default="")
    build_parser_cmd.set_defaults(handler=cmd_build)

    run_parser = sub.add_parser("run", help="один прогон задачи")
    run_parser.add_argument("name")
    run_parser.add_argument("--mode", choices=("bare", "graph"), default="bare")
    run_parser.add_argument("--keep", action="store_true", help="не удалять контейнер")
    run_parser.set_defaults(handler=cmd_run)

    smoke_parser = sub.add_parser("smoke", help="парный smoke bare/graph")
    smoke_parser.add_argument("tasks", nargs="*")
    smoke_parser.set_defaults(handler=cmd_smoke)

    graph_parser = sub.add_parser("graph-index", help="построить локальный индекс графа")
    graph_parser.add_argument("--limit", type=int, default=2000)
    graph_parser.add_argument("--base-sha", default="")
    graph_parser.set_defaults(handler=cmd_graph_index)

    return parser


@beartype
def main(argv: list[str] | None = None) -> int:
    load_environment()
    args = build_parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
