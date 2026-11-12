"""Пер-прогонный агрегатор причин провала: ``runs`` + ``oracle`` → ``cause``.

Диагностика прогонов (З.6) до сих пор была ручной: причины читались по полям
run record глазами. Здесь то же правило сводится в один детерминированный
классификатор, который ставит **одну** причину на прогон и складывает сводку
``mode × outcome × cause``.

Причина ставится по полям run record и приватного oracle (``data/benchmark/
postgres/oracle/*.json``), поэтому модуль работает на уже сохранённых JSON: без
Docker, сети и перепрогона. Категории завершения (``completion.outcome``) не
смешиваются с причинами качества: инфраструктурные отказы получают причину,
равную самому outcome, и в разбор «почему не решил» не попадают.

Ключ к «доводить улучшением графа»: различать «граф не помог» и «модель не
смогла» можно по ``mcp_calls`` — поэтому у graph-рук отдельные причины
``graph_not_called`` (граф не звали) и ``graph_useless`` (звали, правки не
последовало).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import completion, convert, paths
from db_pipeline.benchmark.eval import (
    gate_summary,
    record_step_limit,
)
from db_pipeline.benchmark.runner import GRAPH_MODES

# Итоговые причины провала. Инфраструктурные отказы берут имя из
# ``completion.outcome`` и в этот список не входят: они не про качество решения.
FAIL_CAUSES = (
    "edited_tests",
    "integration_failure",
    "graph_not_called",
    "graph_useless",
    "empty_diff",
    "broke_regress",
    "wrong_file",
    "not_localized",
)

# Гейты, нарушение которых означает «правил или снёс тесты».
_TEST_GATES = ("no_test_edits", "no_recreated_tests")


@beartype
@dataclass(frozen=True)
class Cause:
    """Причина одного прогона: категория завершения, причина и пояснение."""

    outcome: str
    cause: str
    detail: str = ""

    def to_dict(self) -> dict[str, object]:
        return {"outcome": self.outcome, "cause": self.cause, "detail": self.detail}


@beartype
def cause_of(
    record: dict[str, object],
    oracle: dict[str, object] | None = None,
    step_limit: int = 0,
) -> Cause:
    """Ставит одну причину на прогон по правилам З.6.

    Порядок проверок — от инфраструктуры и явных нарушений к размытым:
    негодный outcome → pass → правки тестов → сбой интеграции → граф не звали
    → граф звали без правки → пустой diff → сломанный регресс → правка не туда
    → «модель не локализовала».
    """
    result = completion.classify(record, step_limit)
    outcome = result.outcome
    if outcome not in completion.OK_OUTCOMES:
        return Cause(outcome=outcome, cause=outcome, detail=result.detail)
    if bool(record.get("pass")):
        return Cause(outcome=outcome, cause="pass")

    failed_test_gates = [name for name in _TEST_GATES if not gate_summary(record).get(name, True)]
    if len(failed_test_gates) > 0:
        return Cause(
            outcome=outcome,
            cause="edited_tests",
            detail=", ".join(failed_test_gates),
        )
    if bool(record.get("integration_failure")):
        return Cause(
            outcome=outcome,
            cause="integration_failure",
            detail=str(record.get("error") or "")[:200],
        )

    mode = str(record.get("mode") or "")
    mcp_calls = _int(record.get("mcp_calls"))
    edited = _edited(record)
    if mode in GRAPH_MODES and mcp_calls == 0:
        return Cause(outcome=outcome, cause="graph_not_called")
    if mode in GRAPH_MODES and mcp_calls > 0 and not edited:
        return Cause(
            outcome=outcome,
            cause="graph_useless",
            detail=f"mcp_calls={mcp_calls}, правок нет",
        )
    if _int(record.get("diff_size")) == 0:
        return Cause(outcome=outcome, cause="empty_diff")
    if bool(record.get("target_ok")) and not bool(record.get("noregress_ok")):
        return Cause(
            outcome=outcome,
            cause="broke_regress",
            detail=_failed_tests(record),
        )
    if not _touched_gold(record, oracle):
        return Cause(
            outcome=outcome,
            cause="wrong_file",
            detail=", ".join(_changed_source_files(record))[:200],
        )
    return Cause(outcome=outcome, cause="not_localized", detail=_failed_tests(record))


@beartype
def classify_records(
    records: list[dict[str, object]],
    data: Path | None = None,
) -> list[dict[str, object]]:
    """Причины по всем записям: на прогон добавляется ``outcome`` и ``cause``."""
    oracles: dict[str, dict[str, object]] = {}
    found: list[dict[str, object]] = []
    for record in records:
        func = str(record.get("func") or "")
        if func != "" and func not in oracles:
            oracles[func] = _load_oracle(func, data)
        step_limit = record_step_limit(record)
        cause = cause_of(record, oracles.get(func), step_limit)
        found.append(
            {
                "func": func,
                "mode": str(record.get("mode") or ""),
                "started": str(record.get("started") or ""),
                "pass": bool(record.get("pass")),
                **cause.to_dict(),
            }
        )
    return found


@beartype
def aggregate_causes(runs: list[dict[str, object]]) -> dict[str, object]:
    """Сводка ``mode × outcome × cause`` и счётчики причин по рукам.

    ``runs`` — результат ``classify_records``: каждая запись уже несёт
    ``mode``/``outcome``/``cause``.
    """
    cross: dict[str, dict[str, dict[str, int]]] = {}
    by_cause: dict[str, dict[str, int]] = {}
    by_outcome: dict[str, dict[str, int]] = {}
    for run in runs:
        mode = str(run.get("mode") or "")
        outcome = str(run.get("outcome") or "")
        cause = str(run.get("cause") or "")
        _bump(cross.setdefault(mode, {}).setdefault(outcome, {}), cause)
        _bump(by_cause.setdefault(mode, {}), cause)
        _bump(by_outcome.setdefault(mode, {}), outcome)
    return {
        "runs": len(runs),
        "modes": sorted(cross),
        "by_mode_outcome_cause": cross,
        "by_cause": by_cause,
        "by_outcome": by_outcome,
    }


@beartype
def build_cause_report(
    data: Path | None = None,
    records: list[dict[str, object]] | None = None,
    modes: tuple[str, ...] = (),
    prompt_variant: str = "",
    experiment_id: str = "",
) -> dict[str, object]:
    """Отчёт по причинам: пер-прогонные записи и сводка.

    ``modes`` и ``prompt_variant`` необязательно сужают набор, иначе в один
    отчёт попадут записи разных прогонов. ``experiment_id`` отсекает чужие партии.
    """
    from db_pipeline.benchmark import eval as eval_module

    selected = (
        eval_module.load_records(data, experiment_id)
        if records is None
        else list(records)
    )
    if records is not None and experiment_id != "":
        selected = [
            record
            for record in selected
            if eval_module.experiment_of(record) == experiment_id
        ]
    if len(modes) > 0:
        selected = [record for record in selected if str(record.get("mode")) in modes]
    if prompt_variant != "":
        selected = [
            record
            for record in selected
            if str(record.get("prompt_variant") or "") == prompt_variant
        ]
    runs = classify_records(selected, data)
    return {
        "filters": {
            "modes": list(modes),
            "prompt_variant": prompt_variant,
            "experiment_id": experiment_id,
        },
        "summary": aggregate_causes(runs),
        "runs": runs,
    }


@beartype
def write_cause_report(
    data: Path | None = None,
    label: str = "causes",
    modes: tuple[str, ...] = (),
    prompt_variant: str = "",
) -> Path:
    """Пишет отчёт в ``data/reports/<label>.json``."""
    report = build_cause_report(data, modes=modes, prompt_variant=prompt_variant)
    target = paths.reports_dir(data) / f"{label}.json"
    import json

    target.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return target


@beartype
def _load_oracle(func: str, data: Path | None) -> dict[str, object]:
    """Читает приватный oracle задачи; его отсутствие не ломает классификацию."""
    if func == "":
        return {}
    try:
        return convert.load_oracle(func, data)
    except Exception:  # noqa: BLE001 - oracle нужен только для проверки файла
        return {}


@beartype
def _touched_gold(record: dict[str, object], oracle: dict[str, object] | None) -> bool:
    """Правка затронула файл эталона: diff пересекается с исходниками gold-патча.

    Без oracle проверка невозможна, поэтому неизвестность трактуется как
    «затронул»: причина ``wrong_file`` ставится только по доказанному
    расхождению, а не по отсутствию данных.
    """
    if not isinstance(oracle, dict):
        return True
    gold_text = str(oracle.get("reverse_patch") or "")
    if gold_text.strip() == "":
        return True
    gold = set(convert.source_paths(gold_text))
    changed = set(_changed_source_files(record))
    if len(changed) == 0:
        return True
    return len(gold & changed) > 0


@beartype
def _changed_source_files(record: dict[str, object]) -> list[str]:
    """Не-тестовые файлы из ``diff`` записи (по заголовкам ``diff --git``)."""
    return convert.source_paths(str(record.get("diff") or ""))


@beartype
def _edited(record: dict[str, object]) -> bool:
    """Правка была: в ``tool_events`` есть изменяющий инструмент."""
    raw = record.get("tool_events")
    if not isinstance(raw, list):
        return False
    for item in cast("list[object]", raw):
        if not isinstance(item, dict):
            continue
        name = str(cast("dict[str, object]", item).get("tool") or "")
        if name in completion.EDIT_TOOLS or name.rsplit("_", 1)[-1] in completion.EDIT_TOOLS:
            return True
    return False


@beartype
def _failed_tests(record: dict[str, object]) -> str:
    check = record.get("check")
    if not isinstance(check, dict):
        return ""
    failed = cast("dict[str, object]", check).get("failed")
    if not isinstance(failed, list):
        return ""
    return ", ".join(str(name) for name in cast("list[object]", failed))[:200]


@beartype
def _bump(counter: dict[str, int], key: str) -> None:
    counter[key] = counter.get(key, 0) + 1


@beartype
def _int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


__all__ = [
    "FAIL_CAUSES",
    "Cause",
    "aggregate_causes",
    "build_cause_report",
    "cause_of",
    "classify_records",
    "write_cause_report",
]
