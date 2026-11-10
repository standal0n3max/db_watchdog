"""Детерминированный split dev/tune/holdout для оценки графа v2.

Делится **до** построения графа. Правило детерминированное и воспроизводимое:
внутри страты (подсистема ``area``) задачи сортируются по ``(дата базы,
sha256(имя))``, и ``tune`` получает равномерно распределённые позиции. Квоты
стратам раздаются методом наибольших остатков, поэтому сумма точно равна
целевому размеру. ``dev`` — пять smoke-задач, они только интеграционные;
``tune`` — рабочая часть, по ней разрешены правки; ``holdout`` — замороженный
список, который нельзя менять после начала работ над графом.

Замороженный ``holdout`` пишется в ``constraints/benchmark-holdout.txt`` — файл
в git, в отличие от ``data/``, поэтому состав проверяем коммитом.

``prior_twin`` в страте не вычисляется здесь: это делает leak-guard шага D;
split лишь резервирует страту, чтобы «повторные применения» не игнорировались.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark import paths
from db_pipeline.benchmark.graph import prism

DEV_TASKS = (
    "postgres__d764d61af316",
    "postgres__d8444163d22e",
    "postgres__96ab9a990eed",
    "postgres__3b120b1e94dd",
    "postgres__9b917a93116e",
)

# Целевой размер tune: остальное уходит в holdout. При 300 задачах tune 15,
# holdout 280 — с большим запасом к минимуму плана (30+).
TUNE_SIZE = 15

HOLDOUT_FILE = "benchmark-holdout.txt"
SECTIONS = ("dev", "tune", "holdout")


@beartype
@dataclass(frozen=True)
class SplitTask:
    """Задача с данными, нужными для стратификации."""

    name: str
    area: str
    base_sha: str
    base_date: str

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.name.encode("utf-8")).hexdigest()


@beartype
def _base_dates(data: Path | None) -> dict[str, str]:
    """Дата базы по каждому sha: сначала корпус PRISM, иначе локальный git."""
    archive = prism.load_archive()
    dates: dict[str, str] = {}
    if archive.git_corpus.is_file():
        conn = prism.open_readonly(archive.git_corpus)
        try:
            for row in conn.execute("SELECT oid, committed_at FROM commits"):
                dates[str(row["oid"])] = str(row["committed_at"])
        finally:
            conn.close()
    return dates


@beartype
def _git_date(repo: Path, sha: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), "show", "-s", "--format=%cI", sha],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


@beartype
def _load(data: Path | None) -> list[SplitTask]:
    """Читает 300 задач и подтягивает дату базы для стратификации по времени."""
    corpus_dates = _base_dates(data)
    repo = paths.pg_clone(data)
    found: list[SplitTask] = []
    for path in sorted(paths.tasks_dir(data).glob("*.json")):
        name = path.stem
        raw = json.loads(path.read_text(encoding="utf-8"))
        area = str(raw.get("area", "")).split(",")[0].strip() if isinstance(raw, dict) else ""
        oracle_path = paths.oracle_dir(data) / f"{name}.json"
        base_sha = ""
        if oracle_path.is_file():
            oracle = json.loads(oracle_path.read_text(encoding="utf-8"))
            if isinstance(oracle, dict):
                base_sha = str(oracle.get("base", ""))
        base_date = corpus_dates.get(base_sha, "")
        if base_date == "" and base_sha != "" and (repo / ".git").exists():
            base_date = _git_date(repo, base_sha)
        found.append(
            SplitTask(name=name, area=area, base_sha=base_sha, base_date=base_date)
        )
    return found


@beartype
def _spread(total: int, want: int) -> set[int]:
    """Равномерно распределяет ``want`` позиций по ``total`` (центры отрезков).

    Индекс выбирается как ``floor((k + 0.5) * total / want)``: выбор идёт по всей
    длине страты, а не по началу, поэтому tune покрывает весь диапазон дат.
    """
    if want <= 0 or total <= 0:
        return set()
    if want >= total:
        return set(range(total))
    return {min(total - 1, int((k + 0.5) * total / want)) for k in range(want)}


@beartype
def _quotas(strata: dict[str, list[SplitTask]], target: int) -> dict[str, int]:
    """Квоты стратам методом наибольших остатков: сумма ровно ``target``."""
    total = sum(len(group) for group in strata.values())
    if total == 0 or target <= 0:
        return {area: 0 for area in strata}
    target = min(target, total)
    exact = {area: target * len(group) / total for area, group in strata.items()}
    quota = {area: int(value) for area, value in exact.items()}
    remain = target - sum(quota.values())
    if remain > 0:
        order = sorted(
            strata,
            key=lambda area: (-(exact[area] - quota[area]), area),
        )
        for area in order[:remain]:
            quota[area] += 1
    return quota


@beartype
def derive_split(data: Path | None = None) -> dict[str, list[str]]:
    """Делит задачи на ``dev``/``tune``/``holdout`` без случайности."""
    tasks = _load(data)
    by_name = {task.name: task for task in tasks}
    dev = [name for name in DEV_TASKS if name in by_name]
    dev_set = set(dev)
    rest = [task for task in tasks if task.name not in dev_set]

    strata: dict[str, list[SplitTask]] = {}
    for task in rest:
        strata.setdefault(task.area, []).append(task)

    quota = _quotas(strata, TUNE_SIZE)
    tune: list[str] = []
    for area in sorted(strata):
        group = sorted(strata[area], key=lambda item: (item.base_date, item.digest))
        for index in sorted(_spread(len(group), quota[area])):
            tune.append(group[index].name)

    tune_set = set(tune)
    holdout = sorted(task.name for task in rest if task.name not in tune_set)
    return {"dev": sorted(dev), "tune": sorted(tune_set), "holdout": holdout}


@beartype
def flatten(split: dict[str, list[str]]) -> dict[str, str]:
    """Отображает имя задачи в раздел split."""
    mapping: dict[str, str] = {}
    for section in SECTIONS:
        for name in split.get(section, []):
            mapping[name] = section
    return mapping


@beartype
def assert_membership(split: dict[str, list[str]], name: str) -> str:
    """Возвращает раздел задачи или падает, если задача вне split.

    Leak-guard использует этот assert, чтобы ни один прогон не прошёл по задаче,
    не закреплённой ни в dev, ни в tune, ни в holdout.
    """
    section = flatten(split).get(name)
    if section is None:
        raise ValueError(f"задача {name} не входит ни в один раздел split")
    return section


@beartype
def write_holdout(split: dict[str, list[str]], root: Path | None = None) -> Path:
    """Заморозить holdout: пишет список в git-версионируемый файл."""
    base = paths.project_root() if root is None else root
    target = base / "constraints" / HOLDOUT_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("".join(f"{name}\n" for name in split["holdout"]), encoding="utf-8")
    return target


@beartype
def read_holdout(root: Path | None = None) -> list[str]:
    """Читает замороженный holdout и падает, если файла нет."""
    base = paths.project_root() if root is None else root
    target = base / "constraints" / HOLDOUT_FILE
    if not target.is_file():
        raise FileNotFoundError(f"holdout не заморожен: {target}")
    return [
        line.strip()
        for line in target.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


@beartype
def holdout_sha256(names: list[str]) -> str:
    """Хэш замороженного списка: попадает в manifest прогона."""
    joined = "".join(f"{name}\n" for name in sorted(names))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
