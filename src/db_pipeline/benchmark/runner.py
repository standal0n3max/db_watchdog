"""Оркестратор одного benchmark-прогона.

Порядок повторяет commit-suite и переносится из ``pg-gym/core/runner.py``:

1. создать task-container без bind mounts и передать архив дерева ``head``;
2. применить reverse source patch и собрать buggy дерево (prebuild/install);
3. удалить скрытые тесты и оборвать историю до одного baseline-коммита;
4. запустить OpenCode непривилегированным пользователем;
5. вернуть скрытые тесты, проверить gate-ы;
6. выполнить targeted и полный regression, посчитать verdict;
7. опубликовать трассу Langfuse и записать run record.

Run record фиксирует версии OpenCode, образов и конфигурации. Локальная копия
остаётся в игнорируемом ``data/runs``.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark import (
    container,
    convert,
    opencode,
    paths,
    postgres,
    security,
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
    recreated: list[str] = []
    record: dict[str, object] = {
        "func": name,
        "mode": mode,
        "trace_id": trace_id,
        "started": started.isoformat(timespec="seconds"),
        "manifest": cfg.to_manifest(),
        "oracle": {"head": oracle["head"], "base": oracle["base"]},
    }

    spec = container.ContainerSpec(
        image=cfg.image,
        workspace=postgres.WORKSPACE,
        network=cfg.internal_network if mode == "graph" else cfg.network,
        user=postgres.BENCH_USER,
        labels={"bench.task": name, "bench.mode": mode},
        environment={"POSTGRES_GYM_HARNESS_CONFIG": _harness_env(cfg)},
    )
    context = ""
    container_id = container.create_container(spec, context=context)
    record["container"] = container_id
    try:
        mounts = container.inspect_mounts(container_id, context=context)
        record["mounts"] = mounts
        if len(mounts) != 0:
            raise PipelineError("isolation", f"контейнер имеет mounts: {mounts}")

        _phase(phases, "prepare", _prepare(container_id, oracle, data, context))
        _phase(phases, "prebuild", _prebuild(container_id))

        hidden, recreated = _hide_and_sever(container_id, public, oracle)
        _phase(phases, "hide", CheckOutcome(ok=True, total=len(hidden)))

        prompt = _prompt(public, mode)
        _phase(phases, "agent", _run_agent(container_id, prompt, cfg, record, transcript))

        restored = postgres.restore_tests(container_id, hidden)
        _phase(phases, "restore", CheckOutcome(ok=sorted(restored) == sorted(hidden), total=len(restored)))

        changed = postgres.changed_files(container_id)
        gate_results = security.gates(changed, recreated)
        record["gates"] = {key: gate.to_dict() for key, gate in gate_results.items()}
        record["diff"] = postgres.worktree_diff(container_id)
        record["diff_size"] = len(str(record["diff"]).splitlines())

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

        record["langfuse"] = _publish_trace(cfg, trace_id, name, mode, record, phases, transcript)
        _write_record(record, data)
        return record
    finally:
        if not keep_container:
            container.remove_container(container_id, context=context)


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
    container_id: str, public: dict[str, object], oracle: dict[str, object]
) -> tuple[list[str], list[str]]:
    graded = as_str_list(public.get("graded_by"))
    hidden = postgres.hide_tests(container_id, graded)
    variants = _expected_variants(container_id, graded)
    hidden += postgres.hide_tests(container_id, variants)
    postgres.remove_tests(container_id, graded)
    postgres.remove_tests(container_id, variants)
    sever_ok, sever_out = postgres.sever_history(container_id)
    if not sever_ok:
        raise PipelineError("sever", sever_out[-2000:])
    recreated = _recreated(container_id, graded + variants)
    return sorted(set(hidden)), recreated


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
    record: dict[str, object],
    transcript: opencode.AgentTranscript,
) -> CheckOutcome:
    postgres.write_file(container_id, "/work/prompt.txt", prompt.encode("utf-8"))
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
    expected = as_str_list(oracle.get("expected_tests"))
    code, targeted_output = postgres.check(container_id, expected or None)
    statuses = postgres.parse_statuses(targeted_output)
    failed = postgres.parse_failed(targeted_output)
    total = postgres.parse_total(targeted_output)
    if expected and not all(statuses.get(name) in ("passed", "failed", "skipped") for name in expected):
        _, full_output = postgres.check(container_id, None)
        targeted_output += "\n" + full_output
        failed = postgres.parse_failed(targeted_output)
        total = postgres.parse_total(targeted_output)
    target_ok = bool(expected) and all(statuses.get(name) == "passed" for name in expected)
    unexpected = sorted(set(failed) - set(expected))
    noregress_ok = code == 0 and len(unexpected) == 0
    outcome = CheckOutcome(
        ok=code == 0,
        total=total,
        failed=failed,
        tail=targeted_output[-2000:] if failed else "",
    )
    return outcome, target_ok, noregress_ok


@beartype
def _mcp_calls(transcript: opencode.AgentTranscript) -> int:
    names = {"find_code_symbols", "read_versioned_source", "search_git_history"}
    return sum(1 for tool in transcript.tools if tool.tool in names)


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
    target = folder / f"{record['func']}__{record['mode']}.json"
    target.write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return target
