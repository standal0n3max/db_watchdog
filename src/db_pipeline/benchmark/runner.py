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
import sqlite3
import tempfile
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import (
    completion,
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
from db_pipeline.benchmark.graph.briefing import Briefing
from db_pipeline.benchmark.models import CheckOutcome, as_str_list
from db_pipeline.errors import PipelineError

MODES = ("bare", "graph")

# Руки, которым поднимается graph-контейнер. Сводка графа кладётся в промпт
# до первого шага; свои вызовы MCP агента в вердикт не входят.
GRAPH_MODES = ("graph",)

# Варианты абзаца промпта про историю. ``directive`` — нейтральная рамка
# «graph tools are available» без требования звать инструменты до правки;
# ``situational`` оставляет ту же сводку и те же имена, но просит начать
# правку, как только причина локализована.
PROMPT_DIRECTIVE = "directive"
PROMPT_SITUATIONAL = "situational"
PROMPT_VARIANTS = (PROMPT_DIRECTIVE, PROMPT_SITUATIONAL)
GRAPH_FRAME = "Graph tools are available."

# Пилот фиксирует нейтральную форму. Ситуативная остаётся флагом.
PROMPT_DEFAULT = PROMPT_DIRECTIVE


@beartype
def run_one(
    name: str,
    mode: str = "bare",
    config: BenchmarkConfig | None = None,
    data: Path | None = None,
    keep_container: bool = False,
    smoke: smoke_session.SmokeSession | None = None,
    prompt_variant: str = PROMPT_DEFAULT,
) -> dict[str, object]:
    if mode not in MODES:
        raise PipelineError("run", f"режим {mode} неизвестен")
    if prompt_variant not in PROMPT_VARIANTS:
        raise PipelineError("run", f"вариант промпта {prompt_variant} неизвестен")
    cfg = config or load_config()
    public = convert.load_public(name, data)
    oracle = convert.load_oracle(name, data)
    trace_id = telemetry.new_trace_id()
    started = datetime.now(UTC)
    phases: dict[str, dict[str, object]] = {}
    transcript = opencode.AgentTranscript()
    hidden: list[str] = []
    manifest = cfg.to_manifest()
    manifest["prompt_variant"] = prompt_variant
    manifest["graph_frame"] = _frame_sentence(prompt_variant)
    record: dict[str, object] = {
        "func": name,
        "mode": mode,
        "trace_id": trace_id,
        "started": started.isoformat(timespec="seconds"),
        "manifest": manifest,
        "oracle": {"head": oracle["head"], "base": oracle["base"]},
        "effective": {"base": oracle["base"], "head": oracle["head"]},
        "prompt_variant": prompt_variant,
        "experiment_id": cfg.experiment_id,
    }

    owns_session = smoke is None
    active = smoke or smoke_session.open_session(cfg, data)
    record["gateway_health"] = dict(active.gateway_health)
    graph_id = ""
    try:
        if mode in GRAPH_MODES:
            graph_id = smoke_session.start_graph(
                active, str(oracle["base"]), str(oracle["head"]), data
            )
            record["graph"] = graph_id
        _run_in_container(active, name, mode, cfg, public, oracle, data, record, phases, transcript,
                          keep_container, hidden, prompt_variant)
        if graph_id != "":
            record["cutoff"] = _verify_cutoff(graph_id, oracle, active.context)
    finally:
        if graph_id != "":
            smoke_session.stop_graph(active, graph_id)
        if owns_session:
            record["cleanup"] = smoke_session.close_session(active)
    record["integration"] = _integration(record, mode, phases, transcript, hidden, graph_id)
    integration = record.get("integration")
    if isinstance(integration, dict):
        record["agent_mcp_calls"] = cast("dict[str, object]", integration).get(
            "agent_mcp_calls", 0
        )
    # Вердикт тестов не переписывается. Ошибка интеграции остаётся флагом:
    # раньше graph без вызова MCP получал FAIL поверх реального результата.
    if not _integration_ok(record.get("integration")):
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
    prompt_variant: str,
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

        briefing_text = ""
        if mode in GRAPH_MODES:
            built = _task_briefing(public, oracle, data)
            record["briefing"] = built.to_dict()
            record["briefing_calls"] = [dict(item) for item in built.calls]
            briefing_text = built.text
        prompt = _prompt(public, mode, prompt_variant, briefing_text)
        before_gateway = smoke_session.read_gateway_health(active)
        _phase(
            phases,
            "agent",
            _run_agent(container_id, prompt, cfg, mode, record, transcript, context),
        )
        after_gateway = smoke_session.read_gateway_health(active)
        record["openrouter_provider"] = str(after_gateway.get("provider") or "")
        record["empty_retries"] = _empty_retry_delta(before_gateway, after_gateway)

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
        record["mcp_calls"] = mcp_calls(transcript)
        record["agent_mcp_calls"] = record["mcp_calls"]
        record["generations"] = [asdict(g) for g in transcript.generations]
        record["tool_events"] = [asdict(t) for t in transcript.tools]
        record.update(opencode.metrics(transcript))
        record["reasoning_over_cap"] = opencode.reasoning_over_cap(
            transcript, cfg.reasoning_max_tokens
        )
        record["completion"] = completion.classify(record, cfg.step_limit).to_dict()
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
    calls = mcp_calls(transcript)
    cutoff = record.get("cutoff")
    cutoff_ok = isinstance(cutoff, dict) and bool(cast("dict[str, object]", cutoff).get("ok"))
    briefing = record.get("briefing")
    briefing_ok = isinstance(briefing, dict) and bool(
        cast("dict[str, object]", briefing).get("delivered")
    )
    details: dict[str, object] = {
        "trace_published": trace_ok,
        "mcp_calls": calls,
        "agent_mcp_calls": calls,
        "hidden_restored": phases.get("restore", {}).get("ok") is True,
        "graph_container": graph_id,
    }
    if mode in GRAPH_MODES:
        details["mcp_used"] = calls > 0
        details["briefing_delivered"] = briefing_ok
        details["cutoff_enforced"] = cutoff_ok
    ok: bool = trace_ok and details["hidden_restored"] is True
    if mode in GRAPH_MODES:
        ok = ok and briefing_ok and graph_id != "" and cutoff_ok
    details["ok"] = ok
    return details


@beartype
def _harness_env(cfg: BenchmarkConfig) -> str:
    """Передаёт в контейнер ровно те лимиты, что заданы в конфиге прогона.

    ``step_limit`` пишется как реальный лимит ходов, а не снятый ``max_turns``.
    """
    return json.dumps(
        {
            "step_limit": cfg.step_limit,
            "temperature": cfg.temperature,
            "context_length": cfg.context_length,
            "max_tokens": cfg.max_tokens,
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
def _prompt(
    public: dict[str, object],
    mode: str,
    variant: str = PROMPT_DIRECTIVE,
    briefing: str = "",
) -> str:
    """Строит промпт. Для graph добавляет сводку и имена инструментов ``graph_*``.

    Сводка уже посчитана оркестратором на отсечке задачи. Вариант меняет только
    короткую рамку: директива называет инструменты доступными и не требует
    звать их до правки, ситуативная разрешает начать правку сразу после
    локализации.
    """
    from db_pipeline.benchmark import tasks as task_rules

    text = task_rules.build_prompt(public)
    if mode not in GRAPH_MODES:
        return text
    return text + _history_paragraph(variant, briefing)


# Имена в форме, в которой их регистрирует OpenCode: префикс сервера ``graph_``.
_GRAPH_TOOL_NAMES = (
    "graph_find_code_symbols, graph_read_versioned_source, "
    "graph_search_git_history, graph_symbol_history, graph_tests_to_code, "
    "graph_similar_fixes, graph_find_error_origin"
)


@beartype
def _frame_sentence(variant: str) -> str:
    """Короткая рамка, которая попадает в manifest прогона."""
    if variant == PROMPT_SITUATIONAL:
        return "History tools are available when you need them."
    return GRAPH_FRAME


@beartype
def _history_paragraph(variant: str, briefing: str) -> str:
    """Сводка графа и короткий абзац с именами ``graph_*``."""
    names = f"Call them by these names: {_GRAPH_TOOL_NAMES}."
    if variant == PROMPT_SITUATIONAL:
        frame = (
            "\n\nHistory tools are available when you need them. "
            + names
            + " Start editing as soon as you have localized the cause."
        )
    else:
        frame = "\n\n" + GRAPH_FRAME + " " + names
    if briefing.strip() == "":
        return frame
    return "\n\nGraph briefing, computed before the first step:\n" + briefing + frame


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
    import subprocess

    postgres.write_file(container_id, "/work/prompt.txt", prompt.encode("utf-8"))
    config = opencode.build_config(
        mode,
        provider=cfg.provider,
        model=cfg.model,
        context_length=cfg.context_length,
        max_tokens=cfg.max_tokens,
        step_limit=cfg.step_limit,
        temperature=cfg.temperature,
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
        temperature=cfg.temperature,
        max_tokens=cfg.max_tokens,
        step_limit=cfg.step_limit,
        timeout=cfg.agent_timeout,
    )
    command = opencode.run_command(agent_config, prompt)
    script = " ".join(_shell_quote(part) for part in command)
    try:
        code, output = postgres.run_in_container(
            container_id, script, timeout=cfg.agent_timeout, user=postgres.BENCH_USER
        )
    except subprocess.TimeoutExpired:
        return _timeout_outcome(record, cfg)
    parsed = opencode.parse_jsonl(output)
    retries = 0
    # Один ретрай, если прогон оборвал потолок ответа (``reason=length``): ответ
    # обрезан и отброшен, но шаги уже потрачены. Перед повтором дерево
    # возвращается к baseline, иначе обрезанная попытка наслаивается на повтор.
    while retries < LENGTH_RETRY_LIMIT and _truncated_by_length(parsed):
        retries += 1
        _reset_worktree(container_id)
        try:
            code, output = postgres.run_in_container(
                container_id, script, timeout=cfg.agent_timeout, user=postgres.BENCH_USER
            )
        except subprocess.TimeoutExpired:
            record["agent_length_retries"] = retries
            return _timeout_outcome(record, cfg)
        parsed = opencode.parse_jsonl(output)
    transcript.generations = parsed.generations
    transcript.tools = parsed.tools
    transcript.errors = parsed.errors
    transcript.raw_events = parsed.raw_events
    transcript.session_id = parsed.session_id
    record["agent_returncode"] = code
    record["agent_length_retries"] = retries
    record["agent_jsonl"] = output
    record["nudges"] = _nudge_empty_stop(container_id, agent_config, cfg, record, transcript)
    return CheckOutcome(ok=code == 0, tail=output[-2000:] if code != 0 else "")


LENGTH_RETRY_LIMIT = 1


@beartype
def _timeout_outcome(record: dict[str, object], cfg: BenchmarkConfig) -> CheckOutcome:
    """Фиксирует таймаут агента, не теряя ``started`` у уже открытой записи."""
    record["agent_timeout"] = True
    record["agent_returncode"] = -1
    record["nudges"] = _stat_int(record.get("nudges"))
    record["completion"] = completion.classify(record, cfg.step_limit).to_dict()
    return CheckOutcome(ok=False, tail="timeout")


@beartype
def _nudge_empty_stop(
    container_id: str,
    agent_config: opencode.AgentConfig,
    cfg: BenchmarkConfig,
    record: dict[str, object],
    transcript: opencode.AgentTranscript,
) -> int:
    """Один и тот же ход для bare и graph: продолжить сессию, если diff пустой."""
    import subprocess

    done = 0
    while done < cfg.empty_stop_nudges and opencode.should_nudge(
        transcript,
        cfg.step_limit,
        _diff_empty(container_id),
        record.get("agent_timeout") is True,
    ):
        command = opencode.continue_command(
            agent_config, transcript.session_id, opencode.NUDGE_PROMPT
        )
        script = " ".join(_shell_quote(part) for part in command)
        try:
            code, output = postgres.run_in_container(
                container_id, script, timeout=cfg.agent_timeout, user=postgres.BENCH_USER
            )
        except subprocess.TimeoutExpired:
            record["agent_timeout"] = True
            break
        extra = opencode.parse_jsonl(output)
        opencode.append_transcript(transcript, extra)
        record["agent_returncode"] = code
        record["agent_jsonl"] = str(record.get("agent_jsonl") or "") + output
        done += 1
    return done


@beartype
def _diff_empty(container_id: str) -> bool:
    return postgres.worktree_diff(container_id).strip() == ""


@beartype
def _empty_retry_delta(before: dict[str, object], after: dict[str, object]) -> int:
    """Сколько пустых ответов шлюз повторил за этот прогон."""
    return max(0, _stat_int(after.get("empty_retries_total")) - _stat_int(before.get("empty_retries_total")))


@beartype
def _stat_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    return int(value)


@beartype
def _task_briefing(
    public: dict[str, object],
    oracle: dict[str, object],
    data: Path | None,
) -> Briefing:
    """Сводка тем же GraphView, что и MCP: та же sqlite, base и запрещённый head.

    Считается на хосте до старта агента. В функцию сводки уходит только
    публичная задача; поля эталона нужны лишь чтобы открыть ту же отсечку.
    """
    from db_pipeline.benchmark.graph import briefing as briefing_module
    from db_pipeline.benchmark.graph import view as graph_view
    from db_pipeline.benchmark.graph.gitgraph import graph_db

    try:
        db = graph_db(data)
        opened = graph_view.open_view(db, str(oracle["base"]), str(oracle["head"]))
    except (OSError, PipelineError, sqlite3.Error) as exc:
        return Briefing(
            text=f"briefing unavailable: {type(exc).__name__}",
            sha256="",
            tokens=0,
            calls=(),
            delivered=False,
        )
    try:
        return briefing_module.build_briefing(opened, public)
    finally:
        graph_view.close_view(opened)

# Откат правок обрезанной попытки: сохраняются только сборочные артефакты.
RESET_WORKTREE_COMMAND = "git checkout -- . && git clean -fd -e build"


@beartype
def _reset_worktree(container_id: str) -> None:
    """Возвращает рабочее дерево к baseline перед повтором на ``length``."""
    postgres.run_in_container(
        container_id,
        RESET_WORKTREE_COMMAND,
        timeout=300,
        user=postgres.BENCH_USER,
    )


@beartype
def _truncated_by_length(parsed: opencode.AgentTranscript) -> bool:
    """Прогон оборван потолком ответа: последняя генерация с ``reason=length``.

    MCP и размер ответов инструментов тут ни при чём: ``length`` приходит от
    провайдера как причина последней генерации.
    """
    if len(parsed.generations) == 0:
        return False
    last = parsed.generations[-1]
    return last.reason == "length" and last.output_tokens == 0


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
    {
        "find_code_symbols",
        "read_versioned_source",
        "search_git_history",
        "symbol_history",
        "tests_to_code",
        "similar_fixes",
        "find_error_origin",
    }
)


@beartype
def mcp_calls(transcript: opencode.AgentTranscript) -> int:
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
    start, end = _trace_window(transcript)
    root = telemetry.OtelSpan(
        name=f"{name} · {mode}",
        start_ns=start,
        end_ns=end,
        attributes={
            "langfuse.trace.name": f"{name} · {mode}",
            "langfuse.trace.metadata.experiment_id": cfg.experiment_id,
            "langfuse.trace.metadata.langfuse_project": cfg.langfuse_project,
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
        gen_start = telemetry.ms_to_ns(generation.started) if generation.started else start
        gen_end = telemetry.ms_to_ns(generation.ended) if generation.ended else gen_start + 1
        children.append(
            telemetry.OtelSpan(
                name="opencode:generation",
                start_ns=gen_start,
                end_ns=max(gen_end, gen_start + 1),
                parent_id=root.span_id,
                attributes=telemetry.generation_attributes(
                    cfg.model,
                    generation.text,
                    generation.input_tokens,
                    generation.output_tokens,
                    generation.reasoning_tokens,
                    generation.cache_read,
                    generation.cost,
                    generation.compaction,
                ),
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
                    "langfuse.observation.input": telemetry.clip_text(tool.input),
                    "langfuse.observation.output": telemetry.clip_text(tool.output),
                    "langfuse.observation.metadata.status": tool.status,
                },
            )
        )
    payload = telemetry.build_otlp(trace_id, root, children)
    published = telemetry.publish(cfg.otel, trace_id, payload)
    published["expected_events"] = 1 + len(children)
    return published


@beartype
def _trace_window(transcript: opencode.AgentTranscript) -> tuple[int, int]:
    """Реальные времена JSONL, а не один момент публикации трассы."""
    stamps: list[int] = []
    for generation in transcript.generations:
        if generation.started > 0:
            stamps.append(generation.started)
        if generation.ended > 0:
            stamps.append(generation.ended)
    for tool in transcript.tools:
        if tool.started > 0:
            stamps.append(tool.started)
        if tool.ended > 0:
            stamps.append(tool.ended)
    if len(stamps) == 0:
        now = telemetry.now_ns()
        return now, now + 1
    start = telemetry.ms_to_ns(min(stamps))
    end = telemetry.ms_to_ns(max(stamps))
    if end <= start:
        end = start + 1
    return start, end


@beartype
def _write_record(record: dict[str, object], data: Path | None) -> Path:
    folder = paths.runs_dir(data)
    run_id = str(record.get("trace_id") or record["func"])
    target = folder / f"{record['func']}__{record['mode']}__{run_id}.json"
    target.write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return target
