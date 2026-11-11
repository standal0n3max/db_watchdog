"""Парная оценка ``bare`` против ``graph v2``: McNemar и метрики прогонов.

Читает run records из ``data/runs`` (их пишет ``runner.run_one``) и считает:

- **pass / verdict** по парам: одна задача, один повтор, две руки;
- **McNemar**: ``b`` — bare прошёл, graph нет; ``c`` — наоборот; точный
  двусторонний биномиальный p-value. При 30 парах различим только крупный
  эффект, поэтому p-value публикуется, но не выдаётся за доказательство.
- **gates**, tokens, вызовы MCP, ``mcp_before_first_edit`` — вызывалась ли
  история до первой правки, и ``turns_to_first_mcp`` — сколько ходов агент
  потратил до первого обращения к графу;
- **страта ``prior_twin``**: задачи, чья правка уже применялась в истории базы,
  отделяются, потому что там «найти готовый ответ» легче.

Повторы: в run record нет номера повтора, поэтому он выводится детерминированно —
внутри ``(задача, рука)`` записи сортируются по ``started`` и нумеруются. Так
отчёт воспроизводим на одном и том же наборе записей.

Здесь нет обращений к Docker и сети: модуль работает на уже сохранённых JSON.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from math import comb
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import paths

# Инструменты, которые меняют код: по ним определяется «первая правка».
EDIT_TOOLS = ("edit", "write", "patch", "multiedit", "apply_patch", "str_replace")


@beartype
@dataclass
class PairOutcome:
    """Результат одной пары: задача, повтор, обе руки."""

    task: str
    repetition: int
    bare_pass: bool = False
    graph_pass: bool = False
    bare: dict[str, object] = field(default_factory=dict[str, object])
    graph: dict[str, object] = field(default_factory=dict[str, object])

    @property
    def discordant(self) -> bool:
        return self.bare_pass != self.graph_pass


@beartype
@dataclass
class McNemarResult:
    """Итог McNemar: расхождения и точный двусторонний p-value."""

    pairs: int
    bare_only: int
    graph_only: int
    both_pass: int
    both_fail: int
    p_value: float

    def to_dict(self) -> dict[str, object]:
        return {
            "pairs": self.pairs,
            "bare_only": self.bare_only,
            "graph_only": self.graph_only,
            "both_pass": self.both_pass,
            "both_fail": self.both_fail,
            "p_value": round(self.p_value, 6),
        }


@beartype
def load_records(data: Path | None = None) -> list[dict[str, object]]:
    """Читает все run records из ``data/runs``."""
    found: list[dict[str, object]] = []
    for path in sorted(paths.runs_dir(data).glob("*.json")):
        try:
            raw: object = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(raw, dict):
            continue
        record = cast("dict[str, object]", raw)
        if record.get("func") and record.get("mode"):
            found.append(record)
    return found


@beartype
def assign_repetitions(records: list[dict[str, object]]) -> None:
    """Нумерует однотипные записи по ``started``: повторы без внешнего счётчика."""
    groups: dict[tuple[str, str], list[dict[str, object]]] = {}
    for record in records:
        key = (str(record["func"]), str(record["mode"]))
        groups.setdefault(key, []).append(record)
    for group in groups.values():
        group.sort(key=lambda item: str(item.get("started", "")))
        for index, record in enumerate(group):
            record["repetition"] = index


@beartype
def build_pairs(records: list[dict[str, object]]) -> list[PairOutcome]:
    """Собирает пары ``bare``/``graph`` по ``(задача, повтор)``."""
    buckets: dict[tuple[str, int], PairOutcome] = {}
    for record in records:
        repetition = int(cast("int", record.get("repetition", 0)))
        key = (str(record["func"]), repetition)
        pair = buckets.setdefault(key, PairOutcome(task=key[0], repetition=key[1]))
        if record["mode"] == "bare":
            pair.bare = record
            pair.bare_pass = bool(record.get("pass"))
        elif record["mode"] == "graph":
            pair.graph = record
            pair.graph_pass = bool(record.get("pass"))
    return sorted(buckets.values(), key=lambda item: (item.task, item.repetition))


@beartype
def mcnemar(pairs: list[PairOutcome], task_level: bool = False) -> McNemarResult:
    """Считает McNemar: каждая пара — это одна задача плюс повтор.

    При ``task_level=True`` повторы одной задачи сводятся большинством голосов:
    иначе повторные запуски одной задачи считаются независимыми, что завышает
    значимость.
    """
    if task_level:
        pairs = _majority_pairs(pairs)
    both_pass = sum(1 for pair in pairs if pair.bare_pass and pair.graph_pass)
    both_fail = sum(1 for pair in pairs if not pair.bare_pass and not pair.graph_pass)
    bare_only = sum(1 for pair in pairs if pair.bare_pass and not pair.graph_pass)
    graph_only = sum(1 for pair in pairs if pair.graph_pass and not pair.bare_pass)
    return McNemarResult(
        pairs=len(pairs),
        bare_only=bare_only,
        graph_only=graph_only,
        both_pass=both_pass,
        both_fail=both_fail,
        p_value=exact_binomial(bare_only, graph_only),
    )


@beartype
def _majority_pairs(pairs: list[PairOutcome]) -> list[PairOutcome]:
    """Сводит повторы задачи к одной паре большинством голосов."""
    grouped: dict[str, list[PairOutcome]] = {}
    for pair in pairs:
        grouped.setdefault(pair.task, []).append(pair)
    reduced: list[PairOutcome] = []
    for task in sorted(grouped):
        group = grouped[task]
        bare_votes = sum(1 for pair in group if pair.bare_pass)
        graph_votes = sum(1 for pair in group if pair.graph_pass)
        half = len(group) / 2
        reduced.append(
            PairOutcome(
                task=task,
                repetition=0,
                bare_pass=bare_votes > half,
                graph_pass=graph_votes > half,
            )
        )
    return reduced


@beartype
def exact_binomial(bare_only: int, graph_only: int) -> float:
    """Точный двусторонний биномиальный p-value для ``n = b + c``, ``p = 0.5``."""
    total = bare_only + graph_only
    if total == 0:
        return 1.0
    smaller = min(bare_only, graph_only)
    tail: float = sum(comb(total, k) for k in range(0, smaller + 1)) / (2**total)
    return min(1.0, 2 * tail)


@beartype
def first_edit_index(record: dict[str, object]) -> int:
    """Индекс первого изменяющего инструмента в списке событий, иначе ``-1``."""
    events = record.get("tool_events")
    if not isinstance(events, list):
        return -1
    for index, raw in enumerate(cast("list[object]", events)):
        if not isinstance(raw, dict):
            continue
        name = str(cast("dict[str, object]", raw).get("tool", ""))
        short = name.rsplit("_", 1)[-1]
        if name in EDIT_TOOLS or short in EDIT_TOOLS:
            return index
    return -1


@beartype
def first_mcp_index(record: dict[str, object]) -> int:
    """Индекс первого вызова MCP-инструмента графа, иначе ``-1``."""
    events = record.get("tool_events")
    if not isinstance(events, list):
        return -1
    from db_pipeline.benchmark.runner import MCP_TOOL_NAMES

    for index, raw in enumerate(cast("list[object]", events)):
        if not isinstance(raw, dict):
            continue
        name = str(cast("dict[str, object]", raw).get("tool", ""))
        if name in MCP_TOOL_NAMES or any(
            name.endswith(f"_{known}") for known in MCP_TOOL_NAMES
        ):
            return index
    return -1


@beartype
def history_used_before_edit(record: dict[str, object]) -> bool:
    """Граф вызывался до первой правки кода.

    Порядок берётся из ``tool_events``: там события идут в хронологическом
    порядке, поэтому сравнение индексов корректно. Правка без обращения к графу
    тоже считается: ``first_mcp_index`` вернёт ``-1``.
    """
    edit = first_edit_index(record)
    mcp = first_mcp_index(record)
    if mcp < 0:
        return False
    if edit < 0:
        return True
    return mcp < edit


@beartype
def turns_to_first_mcp(record: dict[str, object]) -> int:
    """Сколько событий агент потратил до первого обращения к графу.

    ``-1`` означает, что граф не вызывался вообще: это надо видеть как «не
    воспользовался», а не как «ноль ходов».
    """
    mcp = first_mcp_index(record)
    return -1 if mcp < 0 else mcp + 1


@beartype
def gate_summary(record: dict[str, object]) -> dict[str, bool]:
    """Значения gate-ов из run record."""
    gates = record.get("gates")
    if not isinstance(gates, dict):
        return {}
    found: dict[str, bool] = {}
    for key, raw in cast("dict[str, object]", gates).items():
        if isinstance(raw, dict):
            found[key] = bool(cast("dict[str, object]", raw).get("ok"))
    return found


@beartype
def aggregate(records: list[dict[str, object]]) -> dict[str, object]:
    """Сводка по руке: pass, gate-ы, tokens, MCP и порядок вызовов."""
    total = len(records)
    passed = sum(1 for record in records if record.get("pass"))
    gates: dict[str, int] = {}
    tokens_in = 0
    tokens_out = 0
    mcp_runs = 0
    used_before_edit = 0
    turns: list[int] = []
    for record in records:
        if record.get("mcp_calls"):
            mcp_runs += 1
        if history_used_before_edit(record):
            used_before_edit += 1
        turn = turns_to_first_mcp(record)
        if turn >= 0:
            turns.append(turn)
        agent = record.get("agent")
        if isinstance(agent, dict):
            mapping = cast("dict[str, object]", agent)
            tokens_in += int(cast("int", mapping.get("tokens_input", 0)))
            tokens_out += int(cast("int", mapping.get("tokens_output", 0)))
        for name, ok in gate_summary(record).items():
            if not ok:
                gates[name] = gates.get(name, 0) + 1
    return {
        "runs": total,
        "pass": passed,
        "pass_rate": round(passed / total, 4) if total else 0.0,
        "gate_failures": gates,
        "tokens_input": tokens_in,
        "tokens_output": tokens_out,
        "runs_with_mcp": mcp_runs,
        "history_before_edit": used_before_edit,
        "turns_to_first_mcpmedian": median(turns),
    }


@beartype
def median(values: list[int]) -> float:
    if not values:
        return -1.0
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return float(ordered[middle])
    return (ordered[middle - 1] + ordered[middle]) / 2


@beartype
def prior_twin_tasks(data: Path | None = None, section: str = "holdout") -> set[str]:
    """Задачи со стратой ``prior_twin``: берётся из отчёта leak-guard, если он есть."""
    return _report_task_set(data, section, "prior_twin_tasks")


@beartype
def prism_missing_tasks(data: Path | None = None, section: str = "holdout") -> set[str]:
    """Задачи, чьей базы нет в снимке PRISM: у v2 там нет истории.

    Для таких задач инструменты v2 отвечают почти пусто по построению, поэтому
    их нельзя смешивать с покрытыми: иначе средний эффект v2 занижается не
    изъяном графа, а отсутствием данных в корпусе.
    """
    return _report_task_set(data, section, "prism_missing_tasks")


@beartype
def _report_task_set(data: Path | None, section: str, key: str) -> set[str]:
    report = paths.reports_dir(data) / f"leak-audit-{section}.json"
    if not report.is_file():
        return set()
    try:
        raw: object = json.loads(report.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return set()
    if not isinstance(raw, dict):
        return set()
    names = cast("dict[str, object]", raw).get(key)
    if not isinstance(names, list):
        return set()
    return {str(name) for name in cast("list[object]", names)}


@beartype
def build_report(
    data: Path | None = None,
    section: str = "holdout",
    repetitions: int = 2,
) -> dict[str, object]:
    """Собирает итоговый отчёт: пары, McNemar, метрики и страту ``prior_twin``."""
    records = [record for record in load_records(data) if record.get("mode") in ("bare", "graph")]
    assign_repetitions(records)
    pairs = build_pairs(records)
    complete = [pair for pair in pairs if pair.bare and pair.graph]
    twins = prior_twin_tasks(data, section)
    missing_prism = prism_missing_tasks(data, section)
    normal = [pair for pair in complete if pair.task not in twins]
    twin_pairs = [pair for pair in complete if pair.task in twins]
    covered = [pair for pair in complete if pair.task not in missing_prism]
    covered_normal = [pair for pair in covered if pair.task not in twins]
    bare_records = [record for record in records if record.get("mode") == "bare"]
    graph_records = [record for record in records if record.get("mode") == "graph"]
    repeat_counts = sorted({pair.repetition for pair in complete})
    return {
        "pairs": len(complete),
        "incomplete_pairs": len(pairs) - len(complete),
        "repetitions": len(repeat_counts),
        "repetitions_requested": repetitions,
        "repetitions_ok": len(repeat_counts) >= min(repetitions, max(1, len(complete))),
        "mcnemar_pair_level": mcnemar(complete).to_dict(),
        "mcnemar_task_level": mcnemar(complete, task_level=True).to_dict(),
        "mcnemar_normal_only": mcnemar(normal).to_dict(),
        # Основная страта: база есть в PRISM и эталон не применялся прежде.
        "mcnemar_prism_covered": mcnemar(covered).to_dict(),
        "mcnemar_prism_covered_normal": mcnemar(covered_normal).to_dict(),
        "prior_twin_tasks": sorted(twins),
        "prior_twin_pairs": len(twin_pairs),
        "prism_missing_tasks": sorted(missing_prism),
        "prism_missing_pairs": len(complete) - len(covered),
        "bare": aggregate(bare_records),
        "graph": aggregate(graph_records),
    }

@beartype
def write_report(data: Path | None = None, section: str = "holdout") -> Path:
    """Пишет отчёт в ``data/reports/pairwise-<section>.json``."""
    report = build_report(data, section)
    target = paths.reports_dir(data) / f"pairwise-{section}.json"
    target.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return target
