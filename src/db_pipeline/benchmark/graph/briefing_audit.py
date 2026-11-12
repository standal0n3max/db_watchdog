"""Офлайн-проверка сводки: recall файлов относительно эталона.

Эталон читается только здесь, для оценки. Сама сводка строится из публичных
полей. Пилотные задачи в этот отчёт не входят: их список передаётся явно.
"""

from __future__ import annotations

import hashlib
import json
import random
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark import convert, paths
from db_pipeline.benchmark.graph import briefing as briefing_module
from db_pipeline.benchmark.graph import leakguard
from db_pipeline.benchmark.graph import view as graph_view
from db_pipeline.benchmark.graph.gitgraph import graph_db
from db_pipeline.benchmark.graph.leakguard import scan_responses

PILOT_SEED_TEXT = "pilot-2026-11-13"


@beartype
def pilot_seed() -> int:
    """Seed пилота: sha256 строки ``pilot-2026-11-13`` по модулю 2^32."""
    return int(hashlib.sha256(PILOT_SEED_TEXT.encode()).hexdigest(), 16) % (2**32)


@beartype
def choose_pilot(names: list[str], count: int = 5) -> list[str]:
    """Пять задач из уже отсортированного списка, без подбора."""
    if len(names) < count:
        return list(names)
    return random.Random(pilot_seed()).sample(sorted(names), count)


@beartype
def audit_tasks(
    names: list[str],
    data: Path | None = None,
) -> dict[str, object]:
    """Recall@файлы сводки против файлов ``reverse_patch`` эталона."""
    db = graph_db(data)
    per_task: list[dict[str, object]] = []
    for name in names:
        public = convert.load_public(name, data)
        oracle = convert.load_oracle(name, data)
        opened = graph_view.open_view(db, str(oracle["base"]), str(oracle["head"]))
        try:
            built = briefing_module.build_briefing(opened, public)
        finally:
            graph_view.close_view(opened)
        gold = set(convert.source_paths(str(oracle.get("reverse_patch") or "")))
        found = set(briefing_module.mentioned_paths(built.text))
        hit = sorted(gold & found)
        recall = round(len(hit) / len(gold), 4) if len(gold) > 0 else None
        per_task.append(
            {
                "task": name,
                "gold_files": sorted(gold),
                "briefing_files": sorted(found),
                "hit_files": hit,
                "recall": recall,
                "tokens": built.tokens,
                "sha256": built.sha256,
                "calls": [dict(item) for item in built.calls],
            }
        )
    recalls = [
        float(item["recall"])
        for item in per_task
        if isinstance(item["recall"], (int, float))
    ]
    mean = round(sum(recalls) / len(recalls), 4) if len(recalls) > 0 else None
    return {
        "tasks": len(per_task),
        "mean_recall": mean,
        "results": per_task,
    }


@beartype
def canary_briefings(names: list[str], data: Path | None = None) -> list[dict[str, object]]:
    """Канарейки leak-guard по тексту сводки. Эталон нужен только для строк-канареек."""
    repo = paths.pg_clone(data)
    db = graph_db(data)
    reports: list[dict[str, object]] = []
    for name in names:
        public = convert.load_public(name, data)
        task = leakguard.load_task(name, data)
        opened = graph_view.open_view(db, task.base, task.head)
        try:
            built = briefing_module.build_briefing(opened, public)
        finally:
            graph_view.close_view(opened)
        canaries = leakguard.canaries(task, repo)
        violations = scan_responses({"briefing": built.text}, canaries, task.head)
        reports.append(
            {
                "task": name,
                "canaries": len(canaries),
                "violations": violations,
                "sha256": built.sha256,
                "ok": len(violations) == 0,
            }
        )
    return reports


@beartype
def write_audit(report: dict[str, object], data: Path | None = None) -> Path:
    target = paths.reports_dir(data) / "briefing-audit.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target
