"""Проверка данных PRISM перед построением графа v2 (шаг A).

Скрипт ничего не меняет: он читает архив PRISM только для чтения и локальный
git-клон, печатает отчёт и пишет его в ``data/reports/prism-verify.json``.

Проверки:

1. единицы ``hunks`` — байты ли это (по диапазонам и по файлу-эталону),
2. покрытие предков каждой из пяти ``base_sha``: сколько предков клона есть в
   ``processed_commits`` и сколько попало в ``failures``,
3. сверка ``oid``/``tree_oid``/``authored_at`` на выборке коммитов против git,
4. merge-состав пяти баз: ``git rev-list --merges``,
5. распределение ``symbols.role`` на клонах баз,
6. recall имён символов PRISM против нашего сканера по пяти базам,
7. доли hunk-сторон по исходам и хвост overlaps на сторону.

Запуск::

    uv run python -m db_pipeline.benchmark.graph.verify_a
"""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import convert, paths
from db_pipeline.benchmark.graph import prism
from db_pipeline.benchmark.graph.gitgraph import list_tree
from db_pipeline.benchmark.graph.symbols import scan_symbols

SMOKE_TASKS = (
    "d764d61af316",
    "d8444163d22e",
    "96ab9a990eed",
    "3b120b1e94dd",
    "9b917a93116e",
)

# Файлы для проверки единиц ханков: маленькие и стабильные.
UNIT_SAMPLE_PATHS = (
    "src/include/catalog/catversion.h",
    "src/include/pg_config_manual.h",
)

MAX_JOIN_COMMITS = 400

# Максимум hunk-сторон, по которым считается распределение: выборка не должна
# тянуть всю таблицу на 2,8 млн строк.
MAX_SIDE_SAMPLE = 40_000


@beartype
@dataclass(frozen=True)
class BaseInfo:
    task: str
    base: str
    head: str
    area: str
    in_corpus: bool
    base_time: str
    clone_ancestors: int
    clone_merges: int


@beartype
def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout


@beartype
def _bases(data: Path | None) -> list[BaseInfo]:
    found: list[BaseInfo] = []
    for task in SMOKE_TASKS:
        name = f"postgres__{task}"
        try:
            public = convert.load_public(name, data)
            oracle = convert.load_oracle(name, data)
        except Exception:  # noqa: BLE001 - отсутствие задачи не роняет отчёт
            continue
        found.append(
            BaseInfo(
                task=task,
                base=str(oracle["base"]),
                head=str(oracle["head"]),
                area=str(public.get("area", "")),
                in_corpus=False,
                base_time="",
                clone_ancestors=0,
                clone_merges=0,
            )
        )
    return found


@beartype
def _ancestor_counts(repo: Path, bases: list[BaseInfo]) -> list[BaseInfo]:
    """Число предков и merge-коммитов каждой базы по локальному клону."""
    out: list[BaseInfo] = []
    for item in bases:
        sha = item.base
        exists = _git(repo, "cat-file", "-t", sha).strip() == "commit"
        ancestors = 0
        merges = 0
        base_time = ""
        if exists:
            ancestors = int(_git(repo, "rev-list", "--count", sha).strip() or "0")
            merges = int(_git(repo, "rev-list", "--count", "--merges", sha).strip() or "0")
            base_time = _git(repo, "show", "-s", "--format=%cI", sha).strip()
        out.append(
            BaseInfo(
                task=item.task,
                base=sha,
                head=item.head,
                area=item.area,
                in_corpus=item.in_corpus,
                base_time=base_time,
                clone_ancestors=ancestors,
                clone_merges=merges,
            )
        )
    return out


@beartype
def _corpus_coverage(
    transitions_path: Path,
    git_path: Path,
    bases: list[BaseInfo],
) -> tuple[list[BaseInfo], dict[str, object]]:
    """Покрытие предков базы: есть ли sha в корпусе и в processed_commits."""
    covered: list[BaseInfo] = []
    details: dict[str, object] = {}
    signals = prism.open_readonly(transitions_path)
    corpus = prism.open_readonly(git_path)
    try:
        for item in bases:
            in_git = int(
                corpus.execute(
                    "SELECT count(*) FROM commits WHERE oid = ?", (item.base,)
                ).fetchone()[0]
            )
            in_processed = int(
                signals.execute(
                    "SELECT count(*) FROM processed_commits WHERE oid = ?", (item.base,)
                ).fetchone()[0]
            )
            failed = int(
                signals.execute(
                    "SELECT count(*) FROM failures WHERE oid = ?", (item.base,)
                ).fetchone()[0]
            )
            covered.append(
                BaseInfo(
                    task=item.task,
                    base=item.base,
                    head=item.head,
                    area=item.area,
                    in_corpus=in_git == 1 and in_processed == 1,
                    base_time=item.base_time,
                    clone_ancestors=item.clone_ancestors,
                    clone_merges=item.clone_merges,
                )
            )
            details[item.task] = {
                "in_git_corpus": in_git,
                "in_processed_commits": in_processed,
                "in_failures": failed,
            }
    finally:
        signals.close()
        corpus.close()
    return covered, details


@beartype
def _instant(value: str) -> float:
    """Момент времени из ISO-строки независимо от смещения пояса."""
    from datetime import datetime

    return datetime.fromisoformat(value).timestamp()


@beartype
def _oid_crosscheck(repo: Path, git_path: Path, bases: list[BaseInfo]) -> dict[str, object]:
    """Сверяет oid/tree_oid/даты образца коммитов корпуса с git-клоном.

    Даты сравниваются как моменты времени: корпус хранит UTC, ``git show %aI``
    печатает локальное смещение автора, поэтому строки различаются при равном
    инстанте. ``authored_at`` — дата автора, ``committed_at`` — коммиттера.
    """
    corpus = prism.open_readonly(git_path)
    report: dict[str, object] = {"checked": 0, "mismatches": [], "format_only_diffs": 0}
    try:
        rows = corpus.execute(
            "SELECT c.oid, c.tree_oid, c.authored_at, c.committed_at FROM commits c "
            "ORDER BY c.committed_at DESC LIMIT 20"
        ).fetchall()
        mismatches: list[dict[str, str]] = []
        format_only = 0
        checked = 0
        for row in rows:
            oid = str(row["oid"])
            exists = _git(repo, "cat-file", "-t", oid).strip()
            if exists != "commit":
                mismatches.append({"oid": oid, "problem": f"git type={exists or 'missing'}"})
                continue
            checked += 1
            git_tree = _git(repo, "show", "-s", "--format=%T", oid).strip()
            git_author = _git(repo, "show", "-s", "--format=%aI", oid).strip()
            git_committer = _git(repo, "show", "-s", "--format=%cI", oid).strip()
            if git_tree != str(row["tree_oid"]):
                mismatches.append(
                    {
                        "oid": oid,
                        "problem": "tree_oid",
                        "corpus": str(row["tree_oid"]),
                        "git": git_tree,
                    }
                )
            for problem, git_value, corpus_value in (
                ("authored_at", git_author, str(row["authored_at"])),
                ("committed_at", git_committer, str(row["committed_at"])),
            ):
                if _instant(git_value) != _instant(corpus_value):
                    mismatches.append(
                        {
                            "oid": oid,
                            "problem": problem,
                            "corpus": corpus_value,
                            "git": git_value,
                        }
                    )
                elif git_value != corpus_value:
                    format_only += 1
        report["checked"] = checked
        report["mismatches"] = mismatches[:20]
        report["format_only_diffs"] = format_only
    finally:
        corpus.close()
    return report


@beartype
def _corpus_membership(git_path: Path, transitions_path: Path) -> tuple[set[str], set[str]]:
    """Множества oid корпуса и обработанных PRISM-переходов (по 105 тыс. строк)."""
    corpus = prism.open_readonly(git_path)
    try:
        in_corpus = {str(row[0]) for row in corpus.execute("SELECT oid FROM commits")}
    finally:
        corpus.close()
    signals = prism.open_readonly(transitions_path)
    try:
        processed = {str(row[0]) for row in signals.execute("SELECT oid FROM processed_commits")}
        failed = {str(row[0]) for row in signals.execute("SELECT oid FROM failures")}
    finally:
        signals.close()
    return in_corpus | failed, processed


@beartype
def _ancestor_coverage(
    repo: Path,
    bases: list[BaseInfo],
    in_corpus: set[str],
    processed: set[str],
) -> dict[str, object]:
    """Точная доля предков базы, покрытых корпусом и ``processed_commits``.

    Пересечение считается по полному множеству предков ``git rev-list base``,
    а не по последним N коммитам: усечённая выборка исказила бы долю.
    """
    report: dict[str, object] = {}
    for item in bases:
        ancestors = set(_git(repo, "rev-list", item.base).split())
        if not ancestors:
            report[item.task] = {"ancestors": 0}
            continue
        corpus_hit = ancestors & in_corpus
        processed_hit = ancestors & processed
        report[item.task] = {
            "ancestors": len(ancestors),
            "in_corpus": len(corpus_hit),
            "in_processed": len(processed_hit),
            "in_failures": len(ancestors & (in_corpus - processed)),
            "corpus_ratio": round(len(corpus_hit) / len(ancestors), 4),
            "processed_ratio": round(len(processed_hit) / len(ancestors), 4),
        }
    return report


@beartype
def _master_after_snapshot(repo: Path, git_path: Path) -> dict[str, object]:
    """Сколько коммитов мастера появилось после снимка корпуса (диагностика)."""
    corpus = prism.open_readonly(git_path)
    try:
        row = corpus.execute(
            "SELECT oid, committed_at FROM commits ORDER BY committed_at DESC LIMIT 1"
        ).fetchone()
    finally:
        corpus.close()
    if row is None:
        return {"corpus_tip": "", "commits_after_tip": 0}
    tip = str(row["oid"])
    count = 0
    if _git(repo, "cat-file", "-t", tip).strip() == "commit":
        count = int(_git(repo, "rev-list", "--count", f"{tip}..origin/master").strip() or "0")
    return {
        "corpus_tip": tip,
        "corpus_tip_at": str(row["committed_at"]),
        "commits_after_tip": count,
    }


@beartype
def _hunk_units(repo: Path, transitions_path: Path, bases: list[BaseInfo]) -> dict[str, object]:
    """Проверяет, что ``old_end``/``new_end`` — байты, а не номера строк."""
    conn = prism.open_readonly(transitions_path)
    paths_report: dict[str, dict[str, object]] = {}
    report_max_end = 0
    try:
        in_corpus = [item for item in bases if item.in_corpus]
        if not in_corpus:
            return {"paths": {}, "max_end": 0}
        base = in_corpus[0].base
        for path in UNIT_SAMPLE_PATHS:
            blob = _git(repo, "rev-parse", f"{base}:{path}").strip()
            raw = _git_bytes(repo, "cat-file", "blob", blob)
            if raw == b"":
                continue
            total_bytes = len(raw)
            total_lines = raw.count(b"\n") + 1
            rows = conn.execute(
                "SELECT old_start, old_end, new_start, new_end FROM hunks "
                "WHERE path = ? ORDER BY ordinal LIMIT 20",
                (path,),
            ).fetchall()
            max_end = 0
            for row in rows:
                max_end = max(max_end, int(row["old_end"]), int(row["new_end"]))
            paths_report[path] = {
                "total_bytes": total_bytes,
                "total_lines": total_lines,
                "hunks": len(rows),
                "max_end": max_end,
                "end_exceeds_lines": max_end > total_lines,
                "end_within_bytes": max_end <= total_bytes,
            }
            report_max_end = max(report_max_end, max_end)
        return {"paths": paths_report, "max_end": report_max_end}
    finally:
        conn.close()


@beartype
def _git_bytes(repo: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        check=False,
    )
    return result.stdout


@beartype
def _role_distribution(exact_path: Path, repo: Path, bases: list[BaseInfo]) -> dict[str, object]:
    """Доля определений против прототипов среди символов на деревьях баз."""
    conn = prism.open_readonly(exact_path)
    report: dict[str, object] = {}
    try:
        for item in bases:
            blob_shas = _git(repo, "ls-tree", "-r", "--format=%(objectname)", item.base).split()
            sample = blob_shas[:5000]
            if not sample:
                continue
            placeholders = ",".join("?" for _ in sample)
            rows = conn.execute(
                f"SELECT role, count(*) AS n FROM symbols WHERE blob_oid IN ({placeholders}) "
                "GROUP BY role",
                sample,
            ).fetchall()
            counts = {str(row["role"]): int(row["n"]) for row in rows}
            total = sum(counts.values())
            report[item.task] = {
                "blobs": len(sample),
                "total": total,
                "definition_ratio": round(counts.get("parsed_definition", 0) / total, 4)
                if total
                else 0.0,
                "by_role": counts,
            }
    finally:
        conn.close()
    return report


@beartype
def _recall(exact_path: Path, repo: Path, bases: list[BaseInfo]) -> dict[str, object]:
    """Recall имён: сколько наших символов с базы видит exact-symbols."""
    conn = prism.open_readonly(exact_path)
    report: dict[str, object] = {}
    try:
        for item in bases:
            entries = [
                entry
                for entry in list_tree(repo, item.base)
                if entry.path.endswith((".c", ".h")) and entry.kind == "blob"
            ]
            sample = entries[:60]
            ours: set[str] = set()
            theirs: set[str] = set()
            for entry in sample:
                raw = _git_bytes(repo, "cat-file", "blob", entry.blob_sha)
                text = raw.decode("utf-8", "replace")
                for symbol in scan_symbols(entry.path, text.splitlines()):
                    ours.add(symbol.name)
                rows = conn.execute(
                    "SELECT name FROM symbols WHERE blob_oid = ?", (entry.blob_sha,)
                ).fetchall()
                for row in rows:
                    theirs.add(str(row["name"]))
            missing = sorted(ours - theirs)
            report[item.task] = {
                "blobs": len(sample),
                "ours": len(ours),
                "theirs": len(theirs),
                "missing_in_prism": len(missing),
                "recall": round(len(ours - set(missing)) / len(ours), 4) if ours else 0.0,
                "missing_sample": missing[:20],
            }
    finally:
        conn.close()
    return report


@beartype
def _overlap_shares(transitions_path: Path) -> dict[str, object]:
    """Считает доли исходов hunk-сторон и хвост overlaps на сторону."""
    conn = prism.open_readonly(transitions_path)
    try:
        outcomes = conn.execute(
            "SELECT outcome, count(*) AS n FROM hunk_sides GROUP BY outcome ORDER BY n DESC"
        ).fetchall()
        total = sum(int(row["n"]) for row in outcomes)
        per_hunk = conn.execute(
            "SELECT cnt, count(*) AS n FROM ("
            " SELECT hunk_id, side, count(*) AS cnt FROM symbol_overlaps "
            " GROUP BY hunk_id, side HAVING cnt > 1 LIMIT ?"
            ") GROUP BY cnt ORDER BY cnt LIMIT 10",
            (MAX_SIDE_SAMPLE,),
        ).fetchall()
        ops = conn.execute(
            "SELECT operation, count(*) AS n FROM hunks GROUP BY operation ORDER BY n DESC"
        ).fetchall()
        hunk_total = int(conn.execute("SELECT count(*) FROM hunks").fetchone()[0])
        side_total = int(conn.execute("SELECT count(*) FROM hunk_sides").fetchone()[0])
        return {
            "hunk_sides_total": side_total,
            "hunks_total": hunk_total,
            "outcomes": [
                {
                    "outcome": str(row["outcome"]),
                    "n": int(row["n"]),
                    "share": round(int(row["n"]) / total, 4) if total else 0.0,
                }
                for row in outcomes
            ],
            "multi_overlap_tail": [
                {"symbols": int(row["cnt"]), "sides": int(row["n"])} for row in per_hunk
            ],
            "operations": [
                {"operation": str(row["operation"]), "n": int(row["n"])} for row in ops
            ],
        }
    finally:
        conn.close()


@beartype
def run(data: Path | None = None) -> dict[str, object]:
    archive = prism.load_archive()
    missing = archive.present()
    repo = paths.pg_clone(data)
    bases = _bases(data)
    bases = _ancestor_counts(repo, bases)

    report: dict[str, object] = {
        "archive": {
            "root": str(archive.root),
            "missing": missing,
        },
        "clone": {
            "path": str(repo),
            "head": _git(repo, "rev-parse", "HEAD").strip(),
            "origin_master": _git(repo, "rev-parse", "origin/master").strip(),
            "commits": int(_git(repo, "rev-list", "--count", "HEAD").strip() or "0"),
            "is_shallow": _git(repo, "rev-parse", "--is-shallow-repository").strip(),
        },
        "bases": [item.__dict__ for item in bases],
    }
    if len(missing) != 0:
        return report

    bases, coverage = _corpus_coverage(archive.transitions, archive.git_corpus, bases)
    report["bases"] = [item.__dict__ for item in bases]
    report["coverage"] = coverage
    in_corpus, processed = _corpus_membership(archive.git_corpus, archive.transitions)
    report["ancestor_coverage"] = _ancestor_coverage(repo, bases, in_corpus, processed)
    report["master_after_snapshot"] = _master_after_snapshot(repo, archive.git_corpus)
    report["oid_crosscheck"] = _oid_crosscheck(repo, archive.git_corpus, bases)
    report["hunk_units"] = _hunk_units(repo, archive.transitions, bases)
    report["role_distribution"] = _role_distribution(archive.exact_symbols, repo, bases)
    report["recall"] = _recall(archive.exact_symbols, repo, bases)
    report["overlaps"] = _overlap_shares(archive.transitions)
    corpus = prism.open_readonly(archive.git_corpus)
    try:
        report["corpus"] = {
            "commits": int(corpus.execute("SELECT count(*) FROM commits").fetchone()[0]),
            "parents": int(corpus.execute("SELECT count(*) FROM parents").fetchone()[0]),
            "changed_paths": int(
                corpus.execute("SELECT count(*) FROM changed_paths").fetchone()[0]
            ),
            "with_patch_id": int(
                corpus.execute("SELECT count(patch_id) FROM commits").fetchone()[0]
            ),
            "distinct_patch_id": int(
                corpus.execute("SELECT count(DISTINCT patch_id) FROM commits").fetchone()[0]
            ),
            "min_date": str(
                corpus.execute("SELECT min(authored_at) FROM commits").fetchone()[0]
            ),
            "max_date": str(
                corpus.execute("SELECT max(authored_at) FROM commits").fetchone()[0]
            ),
        }
    finally:
        corpus.close()
    return report


@beartype
def main(argv: list[str]) -> int:
    data = Path(argv[0]) if argv else None
    report = run(data)
    target = paths.reports_dir(data) / "prism-verify.json"
    target.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\nотчёт записан: {target}", file=sys.stderr)
    archive = report.get("archive")
    missing: list[object] = []
    if isinstance(archive, dict):
        raw_missing = cast("dict[str, object]", archive).get("missing", [])
        if isinstance(raw_missing, list):
            missing = list(cast("list[object]", raw_missing))
    return 0 if len(missing) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
