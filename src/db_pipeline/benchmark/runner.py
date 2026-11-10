"""Оркестратор одного benchmark-прогона.

Порядок повторяет commit-suite и переносится из ``pg-gym/core/runner.py``:

1. создать task-container без bind mounts и передать архив дерева ``head``;
2. применить reverse source patch и собрать buggy дерево (prebuild/install);
3. удалить скрытые тесты и оборвать историю до одного baseline-коммита;
4. запустить OpenCode непривилегированным пользователем;
5. вернуть скрытые тесты, проверить gate-ы;
6. выполнить targeted и полный regression, посчитать verdict;
7. опубликовать трассу Langfuse и записать run record.

Режим ``graph`` использует ту же internal-сеть, что и ``bare``, но подключает
per-task graph-container и remote MCP-конфиг; ``bare`` MCP не видит. Эффективный
OpenCode-конфиг генерируется на каждый run и его hash попадает в manifest.

Run record фиксирует версии OpenCode, образов и конфигурации. Локальная копия
остаётся в игнорируемом ``data/runs``.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import (
    container,
    convert,
    opencode,
    paths,
    postgres,
    security,
    session as smoke_session,
    telemetry,
    tree,
)
from db_pipeline.benchmark.config import BenchmarkConfig, load_config
from db_pipeline.benchmark.models import CheckOutcome, as_str_list
from db_pipeline.errors import PipelineError

MODES = ("bare", "graph")


@beartype
def run_one(
    name: str,
    mode: str = "bare",
    config: BenchmarkConfig | None = None,
    data: Path | None = None,
    keep_container: bool = False,
    smoke: smoke_session.SmokeSession | None = None,
) -> dict[str, object]:
    if mode not in MODES:
        raise PipelineError("run", f"режим {mode} неизвестен")
    cfg = config or load_config()
    public = convert.load_public(name, data)
    oracle = convert.load_oracle(name, data)
    trace_id = telemetry.new_trace_id()
    started = datetime.now(UTC)
    phases: dict[str, dict[str, object]] = {}
    transcript = opencode.AgentTranscript()
    hidden: list[str] = []
    record: dict[str, object] = {
        "func": name,
        "mode": mode,
        "trace_id": trace_id,
        "started": started.isoformat(timespec="seconds"),
        "manifest": cfg.to_manifest(),
        "oracle": {"head": oracle["head"], "base": oracle["base"]},
        "effective": {"base": oracle["base"], "head": oracle["head"]},
    }

    owns_session = smoke is None
    active = smoke or smoke_session.open_session(cfg, data)
    graph_id = ""
    try:
        if mode == "graph":
            graph_id = smoke_session.start_graph(
                active, str(oracle["base"]), str(oracle["head"]), data
            )
            record["graph"] = graph_id
        _run_in_container(active, name, mode, cfg, public, oracle, data, record, phases, transcript,
                          keep_container, hidden)
        if graph_id != "":
            record["cutoff"] = _verify_cutoff(graph_id, oracle, active.context)
    finally:
        if graph_id != "":
            smoke_session.stop_graph(active, graph_id)
        if owns_session:
            record["cleanup"] = smoke_session.close_session(active)
    record["integration"] = _integration(record, mode, phases, transcript, hidden, graph_id)
    if not _integration_ok(record.get("integration")):
        record["pass"] = False
        record["verdict"] = "FAIL"
        record["integration_failure"] = True
    _write_record(record, data)
    return record


@beartype
def _integration_ok(integration: object) -> bool:
    if not isinstance(integration, dict):
        return False
    mapping = cast("dict[str, object]", integration)
    return bool(mapping.get("ok"))


@beartype
def _run_in_container(
    active: smoke_session.SmokeSession,
    name: str,
    mode: str,
    cfg: BenchmarkConfig,
    public: dict[str, object],
    oracle: dict[str, object],
    data: Path | None,
    record: dict[str, object],
    phases: dict[str, dict[str, object]],
    transcript: opencode.AgentTranscript,
    keep_container: bool,
    hidden: list[str],
) -> None:
    context = active.context
    spec = container.ContainerSpec(
        image=cfg.image,
        workspace=postgres.WORKSPACE,
        network=active.network,
        user=postgres.BENCH_USER,
        labels={"bench.task": name, "bench.mode": mode},
        environment={
            "POSTGRES_GYM_HARNESS_CONFIG": _harness_env(cfg),
            # Внутренний ключ провайдера: шлюз его не проверяет, реального
            # OPENROUTER_API_KEY task-container не видит.
            "POSTGRES_GYM_PROVIDER_KEY": "bench-internal-key",
        },
    )
    container_id = container.create_container(spec, context=context)
    record["container"] = container_id
    started = datetime.now(UTC)
    try:
        mounts = container.inspect_mounts(container_id, context=context)
        record["mounts"] = mounts
        if len(mounts) != 0:
            raise PipelineError("isolation", f"контейнер имеет mounts: {mounts}")

        _phase(phases, "prepare", _prepare(container_id, oracle, data, context))
        _phase(phases, "prebuild", _prebuild(container_id))

        hidden_list, protected_list = _hide_and_sever(container_id, public)
        hidden.extend(hidden_list)
        _phase(phases, "hide", CheckOutcome(ok=True, total=len(hidden)))

        prompt = _prompt(public, mode)
        _phase(
            phases,
            "agent",
            _run_agent(container_id, prompt, cfg, mode, record, transcript, context),
        )

        # Gate-ы и diff снимаются сразу после агента, пока скрытые тесты ещё
        # изъяты: иначе восстановление тестов выглядит как правка тестов агентом.
        changed = postgres.changed_files(container_id)
        recreated = _recreated(container_id, protected_list)
        gate_results = security.gates(changed, recreated)
        record["gates"] = {key: gate.to_dict() for key, gate in gate_results.items()}
        record["diff"] = postgres.worktree_diff(container_id)
        record["diff_size"] = len(str(record["diff"]).splitlines())

        restored = postgres.restore_tests(container_id, hidden)
        _phase(
            phases,
            "restore",
            CheckOutcome(ok=sorted(restored) == sorted(hidden), total=len(restored)),
        )

        _phase(phases, "build", _build(container_id))
        _phase(phases, "install", _install(container_id))
        outcome, target_ok, noregress_ok = _test(container_id, oracle)
        record["check"] = outcome.to_dict()
        record["target_ok"] = target_ok
        record["noregress_ok"] = noregress_ok

        passed = security.verdict(gate_results, target_ok, noregress_ok)
        record["pass"] = passed
        record["verdict"] = "PASS" if passed else "FAIL"
        record["phases"] = phases
        record["agent"] = transcript.totals()
        record["tool_counts"] = transcript.tool_counts()
        record["mcp_calls"] = _mcp_calls(transcript)
        record["generations"] = [asdict(g) for g in transcript.generations]
        record["tool_events"] = [asdict(t) for t in transcript.tools]
        record["seconds"] = round((datetime.now(UTC) - started).total_seconds(), 1)

        record["langfuse"] = _publish_trace(
            cfg, str(record["trace_id"]), name, mode, record, phases, transcript
        )
        _write_record(record, data)
    finally:
        if not keep_container:
            container.remove_container(container_id, context=context)


@beartype
def _verify_cutoff(graph_id: str, oracle: dict[str, object], context: str) -> dict[str, object]:
    """Проверяет отказ MCP по ``head_sha`` текущей задачи, пока контейнер жив."""
    import subprocess

    head = str(oracle["head"])
    script = (
        "python -m db_pipeline.benchmark.graph.probe_client "
        f"http://{smoke_session.GRAPH_ALIAS}:{smoke_session.GRAPH_PORT}{smoke_session.GRAPH_PATH} "
        f"{head}"
    )
    result = subprocess.run(
        container.docker_command("exec", "-i", graph_id, "sh", "-lc", script, context=context),
        capture_output=True,
        text=True,
        check=False,
    )
    report: dict[str, object] = {"head_sha": head, "returncode": result.returncode}
    for line in reversed(result.stdout.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                loaded: object = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(loaded, dict):
                report.update(cast("dict[str, object]", loaded))
            break
    report["ok"] = report.get("returncode") == 0 and bool(report.get("ok"))
    return report


@beartype
def _integration(
    record: dict[str, object],
    mode: str,
    phases: dict[str, dict[str, object]],
    transcript: opencode.AgentTranscript,
    hidden: list[str],
    graph_id: str,
) -> dict[str, object]:
    """Проверяет работоспособность интеграции: MCP, trace, cutoff и cleanup."""
    trace = record.get("langfuse")
    trace_ok = False
    if isinstance(trace, dict):
        trace_map = cast("dict[str, object]", trace)
        trace_ok = bool(trace_map.get("sent"))
    mcp_calls = _mcp_calls(transcript)
    cutoff = record.get("cutoff")
    cutoff_ok = isinstance(cutoff, dict) and bool(cast("dict[str, object]", cutoff).get("ok"))
    details: dict[str, object] = {
        "trace_published": trace_ok,
        "mcp_calls": mcp_calls,
        "hidden_restored": phases.get("restore", {}).get("ok") is True,
        "graph_container": graph_id,
    }
    if mode == "graph":
        details["mcp_used"] = mcp_calls > 0
        details["cutoff_enforced"] = cutoff_ok
    ok = trace_ok and details["hidden_restored"] is True
    if mode == "graph":
        ok = ok and mcp_calls > 0 and graph_id != "" and cutoff_ok
    details["ok"] = ok
    return details


@beartype
def _harness_env(cfg: BenchmarkConfig) -> str:
    return json.dumps(
        {
            "max_turns": cfg.max_turns,
            "temperature": cfg.temperature,
            "context_length": 32768,
            "max_tokens": 4096,
        }
    )


@beartype
def _phase(phases: dict[str, dict[str, object]], key: str, value: CheckOutcome) -> None:
    phases[key] = value.to_dict()


@beartype
def _prepare(
    container_id: str,
    oracle: dict[str, object],
    data: Path | None,
    context: str,
) -> CheckOutcome:
    """Пакует buggy-дерево на хосте и копирует архив в остановленный контейнер."""
    head = str(oracle["head"])
    gold = str(oracle["reverse_patch"])
    repo = paths.pg_clone(data)
    with tempfile.TemporaryDirectory(prefix="bench-tree-") as tmp:
        tree_dir = Path(tmp) / "tree"
        try:
            tree.prepare_buggy_tree(repo, head, gold, tree_dir)
        except PipelineError as exc:
            return CheckOutcome(ok=False, tail=str(exc)[:2000])
        archive = tree.archive_tree(tree_dir)
        archive_path = Path(tmp) / tree.ARCHIVE_NAME
        archive_path.write_bytes(archive)
        container.copy_in(container_id, archive_path, "/work/tree.tar.gz", context=context)
    container.start_detached(container_id, context=context)
    if not container.wait_ready(container_id, "/work/ready", timeout=600, context=context):
        return CheckOutcome(ok=False, tail="entrypoint не поставил /work/ready")
    code, output = postgres.run_in_container(
        container_id,
        "test -f configure && test -d src/backend && echo ready",
        timeout=300,
    )
    ok = code == 0 and "ready" in output
    return CheckOutcome(ok=ok, tail=output[-2000:] if not ok else "")


@beartype
def _prebuild(container_id: str) -> CheckOutcome:
    ok_cfg, out_cfg = postgres.configure(container_id)
    if not ok_cfg:
        return CheckOutcome(ok=False, tail=out_cfg[-2000:])
    ok, output = postgres.build(container_id)
    if not ok:
        return CheckOutcome(ok=False, tail=output[-2000:])
    ok_inst, out_inst = postgres.install(container_id)
    return CheckOutcome(ok=ok_inst, tail=out_inst[-2000:] if not ok_inst else "")


@beartype
def _hide_and_sever(
    container_id: str, public: dict[str, object]
) -> tuple[list[str], list[str]]:
    """Скрывает тесты, удаляет их из дерева и обрывает историю.

    Возвращает ``(hidden, protected)``: скрытые файлы и полный набор путей,
    которые агент не должен пересоздавать (graded и варианты ``_N.out``).
    """
    graded = as_str_list(public.get("graded_by"))
    hidden = postgres.hide_tests(container_id, graded)
    variants = _expected_variants(container_id, graded)
    hidden += postgres.hide_tests(container_id, variants)
    postgres.remove_tests(container_id, graded)
    postgres.remove_tests(container_id, variants)
    sever_ok, sever_out = postgres.sever_history(container_id)
    if not sever_ok:
        raise PipelineError("sever", sever_out[-2000:])
    protected = sorted(set(graded) | set(variants))
    return sorted(set(hidden)), protected


@beartype
def _expected_variants(container_id: str, graded: list[str]) -> list[str]:
    variants: list[str] = []
    for rel in graded:
        if not rel.endswith(".out"):
            continue
        stem = rel[: -len(".out")]
        for n in range(1, 10):
            candidate = f"{stem}_{n}.out"
            code, _ = postgres.run_in_container(
                container_id, f"test -f {candidate} && echo yes", timeout=60
            )
            if code == 0:
                variants.append(candidate)
    return variants


@beartype
def _recreated(container_id: str, protected: list[str]) -> list[str]:
    found: list[str] = []
    for rel in protected:
        code, _ = postgres.run_in_container(
            container_id, f"test -e {rel} && echo here", timeout=60
        )
        if code == 0:
            found.append(rel)
    return sorted(found)


@beartype
def _prompt(public: dict[str, object], mode: str) -> str:
    from db_pipeline.benchmark import tasks as task_rules

    text = task_rules.build_prompt(public)
    if mode == "graph":
        text += (
            "\n\nBefore editing, use the MCP tools find_code_symbols, "
            "read_versioned_source and search_git_history to locate the relevant "
            "code and its history. Any revision the tools return is at or before "
            "the cutoff commit."
        )
    return text


@beartype
def _run_agent(
    container_id: str,
    prompt: str,
    cfg: BenchmarkConfig,
    mode: str,
    record: dict[str, object],
    transcript: opencode.AgentTranscript,
    context: str,
) -> CheckOutcome:
    postgres.write_file(container_id, "/work/prompt.txt", prompt.encode("utf-8"))
    config = opencode.build_config(
        mode,
        provider=cfg.provider,
        model=cfg.model,
        context_length=32768,
        max_tokens=4096,
    )
    config_text = opencode.config_text(config)
    record["opencode_config_sha256"] = opencode.config_sha256(config)
    postgres.write_file(
        container_id,
        opencode.CONFIG_PATH,
        config_text.encode("utf-8"),
        user=postgres.BENCH_USER,
    )
    agent_config = opencode.AgentConfig(
        provider=cfg.provider,
        model=cfg.model,
        max_turns=cfg.max_turns,
        temperature=cfg.temperature,
        timeout=cfg.agent_timeout,
    )
    command = opencode.run_command(agent_config, prompt)
    script = " ".join(_shell_quote(part) for part in command)
    code, output = postgres.run_in_container(
        container_id, script, timeout=cfg.agent_timeout, user=postgres.BENCH_USER
    )
    parsed = opencode.parse_jsonl(output)
    transcript.generations = parsed.generations
    transcript.tools = parsed.tools
    transcript.errors = parsed.errors
    transcript.raw_events = parsed.raw_events
    record["agent_returncode"] = code
    record["agent_jsonl"] = output
    return CheckOutcome(ok=code == 0, tail=output[-2000:] if code != 0 else "")


@beartype
def _shell_quote(part: str) -> str:
    import shlex

    return shlex.quote(part)


@beartype
def _build(container_id: str) -> CheckOutcome:
    ok, output = postgres.build(container_id)
    return CheckOutcome(ok=ok, tail=output[-2000:] if not ok else "")


@beartype
def _install(container_id: str) -> CheckOutcome:
    ok, output = postgres.install(container_id)
    return CheckOutcome(ok=ok, tail=output[-2000:] if not ok else "")


@beartype
def _test(container_id: str, oracle: dict[str, object]) -> tuple[CheckOutcome, bool, bool]:
    """Целевой тест и полный regression: verdict как в commit-suite.

    ``target_ok`` требует, чтобы целевой тест прошёл. ``noregress_ok`` требует,
    чтобы полный regression не дал падений сверх ожидаемых целевых тестов.
    """
    expected = as_str_list(oracle.get("expected_tests"))
    code_targeted, targeted_output = postgres.check(container_id, expected or None)
    statuses = postgres.parse_statuses(targeted_output)
    code_full, full_output = postgres.check(container_id, None)
    combined = targeted_output + "\n" + full_output
    statuses = {**postgres.parse_statuses(full_output), **statuses}
    failed = postgres.parse_failed(combined)
    total = postgres.parse_total(full_output) or postgres.parse_total(targeted_output)
    target_ok = bool(expected) and all(statuses.get(name) == "passed" for name in expected)
    unexpected = sorted(set(failed) - set(expected))
    noregress_ok = code_full == 0 and len(unexpected) == 0
    outcome = CheckOutcome(
        ok=code_targeted == 0,
        total=total,
        failed=failed,
        tail=combined[-2000:] if failed else "",
    )
    return outcome, target_ok, noregress_ok


MCP_TOOL_NAMES = frozenset(
    {"find_code_symbols", "read_versioned_source", "search_git_history"}
)


@beartype
def _mcp_calls(transcript: opencode.AgentTranscript) -> int:
    """Считает вызовы MCP-инструментов графа.

    OpenCode префиксует инструменты именем MCP-сервера (``graph_find_code_symbols``),
    поэтому имя проверяется и по точному совпадению, и по суффиксу.
    """
    return sum(1 for tool in transcript.tools if _is_mcp_tool(tool.tool))


@beartype
def _is_mcp_tool(name: str) -> bool:
    if name in MCP_TOOL_NAMES:
        return True
    return any(name.endswith(f"_{mcp_name}") for mcp_name in MCP_TOOL_NAMES)


@beartype
def _publish_trace(
    cfg: BenchmarkConfig,
    trace_id: str,
    name: str,
    mode: str,
    record: dict[str, object],
    phases: dict[str, dict[str, object]],
    transcript: opencode.AgentTranscript,
) -> dict[str, object]:
    start = telemetry.now_ns()
    end = start + 1
    root = telemetry.OtelSpan(
        name=f"{name} · {mode}",
        start_ns=start,
        end_ns=end,
        attributes={
            "langfuse.trace.name": f"{name} · {mode}",
            "langfuse.observation.input": json.dumps(
                {"func": name, "mode": mode}, ensure_ascii=False
            ),
            "langfuse.observation.output": json.dumps(
                {"verdict": record.get("verdict"), "pass": record.get("pass")},
                ensure_ascii=False,
            ),
        },
    )
    children: list[telemetry.OtelSpan] = []
    children.append(
        telemetry.OtelSpan(
            name="harness:verdict",
            start_ns=start,
            end_ns=end,
            parent_id=root.span_id,
            attributes={
                "langfuse.observation.metadata.gates": json.dumps(
                    record.get("gates"), ensure_ascii=False
                ),
                "langfuse.observation.output": str(record.get("verdict", "")),
            },
        )
    )
    for key, value in phases.items():
        children.append(
            telemetry.OtelSpan(
                name=f"harness:{key}",
                start_ns=start,
                end_ns=end,
                parent_id=root.span_id,
                attributes={"langfuse.observation.metadata.ok": str(value.get("ok"))},
            )
        )
    for generation in transcript.generations:
        children.append(
            telemetry.OtelSpan(
                name="opencode:generation",
                start_ns=start,
                end_ns=end,
                parent_id=root.span_id,
                attributes={
                    "langfuse.observation.type": "generation",
                    "langfuse.observation.model.name": cfg.model,
                    "langfuse.observation.input": generation.text[:2000],
                    "langfuse.observation.usage_details": json.dumps(
                        {
                            "input": generation.input_tokens,
                            "output": generation.output_tokens,
                            "reasoning": generation.reasoning_tokens,
                        }
                    ),
                    "langfuse.observation.cost_details": json.dumps(
                        {"total": generation.cost}
                    ),
                },
            )
        )
    for tool in transcript.tools:
        children.append(
            telemetry.OtelSpan(
                name=f"opencode:tool:{tool.tool}",
                start_ns=telemetry.ms_to_ns(tool.started),
                end_ns=telemetry.ms_to_ns(tool.ended),
                parent_id=root.span_id,
                attributes={
                    "langfuse.observation.type": "tool",
                    "langfuse.observation.input": tool.input,
                    "langfuse.observation.output": tool.output,
                    "langfuse.observation.metadata.status": tool.status,
                },
            )
        )
    payload = telemetry.build_otlp(trace_id, root, children)
    published = telemetry.publish(cfg.otel, trace_id, payload)
    published["expected_events"] = 1 + len(children)
    return published


@beartype
def _write_record(record: dict[str, object], data: Path | None) -> Path:
    folder = paths.runs_dir(data)
    run_id = str(record.get("trace_id") or record["func"])
    target = folder / f"{record['func']}__{record['mode']}__{run_id}.json"
    target.write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return target
