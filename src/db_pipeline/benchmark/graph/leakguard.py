"""Leak-guard графа: membership, prior-twin, канарейки, негативный контроль.

Четыре независимых проверки, каждая обязана пройти до любого прогона v2:

1. **Membership** — каждый ``oid``/``revision`` в любом ответе инструмента обязан
   быть предком ``base_sha`` задачи. Проверяется явно, а не «доверяем Cutoff».
2. **Prior-twin** — эталон задачи мог уже применяться в истории базы (revert и
   повторное применение). Быстрый путь — точное совпадение ``patch_id`` эталона с
   ``patch_id`` предка; медленный — доля совпавших нетривиальных добавленных
   строк ≥ порога. Задача с совпадением **не скрывается**, а помечается
   ``prior_twin=true`` и уходит в отдельную страту отчёта.
3. **Канарейки** — добавленные эталоном строки и ``head_sha`` не должны
   встречаться ни в одном ответе инструмента.
4. **Негативный контроль** — запрос по ``head_sha`` и по потомкам ``base_sha``
   отклоняется.

Модуль не зависит от Docker: он принимает «вызыватель» инструментов, поэтому
одинаково проверяет v1 и v2. ``patch_id`` считается локально из текста патча
(``git patch-id --stable``), потому что ``gold_patch`` + ``test_patch``
воспроизводят ``patch_id`` коммита бит в бит.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import paths
from db_pipeline.benchmark.graph import split as split_module

# Порог совпадения нетривиальных добавленных строк для prior-twin.
PRIOR_TWIN_THRESHOLD = 0.8

# Минимальная длина строки-канарейки.
CANARY_MIN_LEN = 40

# Сколько ближайших предков, тронувших те же пути, проверять медленным путём.
PRIOR_TWIN_CANDIDATES = 200

_TRIVIAL_RE = re.compile(r"^[\s{}();,*&|+\-/=<>\[\]]*$")
_IDENT_RE = re.compile(r"[A-Za-z_]\w{2,}")


@beartype
@dataclass(frozen=True)
class TaskRef:
    """Задача и её эталон, нужные leak-guard'у."""

    name: str
    base: str
    head: str
    gold_patch: str
    test_patch: str
    reference: str
    paths_touched: tuple[str, ...] = ()


@beartype
@dataclass
class LeakReport:
    """Итог проверок одной задачи."""

    task: str
    section: str
    patch_id: str = ""
    prior_twin: bool = False
    prior_twin_kind: str = ""
    prior_twin_evidence: list[str] = field(default_factory=list[str])
    canaries: int = 0
    violations: list[str] = field(default_factory=list[str])
    membership_checked: int = 0

    @property
    def ok(self) -> bool:
        return len(self.violations) == 0

    def to_dict(self) -> dict[str, object]:
        return {
            "task": self.task,
            "section": self.section,
            "patch_id": self.patch_id,
            "prior_twin": self.prior_twin,
            "prior_twin_kind": self.prior_twin_kind,
            "prior_twin_evidence": list(self.prior_twin_evidence),
            "canaries": self.canaries,
            "membership_checked": self.membership_checked,
            "violations": list(self.violations),
            "ok": self.ok,
        }


@beartype
def load_task(name: str, data: Path | None = None) -> TaskRef:
    """Собирает эталон задачи и список тронутых source-путей."""
    from db_pipeline.benchmark import convert

    oracle = convert.load_oracle(name, data)
    gold = str(oracle.get("reverse_patch") or "")
    test = str(oracle.get("test") or "")
    return TaskRef(
        name=name,
        base=str(oracle["base"]),
        head=str(oracle["head"]),
        gold_patch=gold,
        test_patch=test,
        reference=str(oracle.get("reference") or gold),
        paths_touched=tuple(patch_paths(gold)),
    )


@beartype
def patch_paths(patch: str) -> list[str]:
    """Пути из заголовков ``diff --git a/<path> b/<path>``."""
    found: list[str] = []
    for chunk in patch.split("diff --git ")[1:]:
        lines = chunk.splitlines()
        if not lines:
            continue
        header = lines[0]
        rest = header[2:] if header.startswith("a/") else header
        path, separator, _ = rest.partition(" b/")
        if separator == "":
            continue
        if path != "" and path not in found:
            found.append(path)
    return found


@beartype
def added_lines(patch: str) -> list[str]:
    """Добавленные строки патча без заголовков ``+++``."""
    return [
        line[1:]
        for line in patch.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    ]


@beartype
def nontrivial_lines(patch: str) -> list[str]:
    """Нетривиальные добавленные строки: длинные и с идентификаторами."""
    found: list[str] = []
    for line in added_lines(patch):
        stripped = line.strip()
        if len(stripped) < CANARY_MIN_LEN:
            continue
        if _TRIVIAL_RE.match(stripped):
            continue
        if _IDENT_RE.search(stripped) is None:
            continue
        if line not in found:
            found.append(line)
    return found


@beartype
def _run_git(args: list[str], stdin: str | None = None) -> str:
    """Запускает git и возвращает stdout, терпимый к не-UTF-8 байтам.

    Корпус PostgreSQL содержит файлы в latin-1/cp1251, поэтому строгий
    ``text=True`` падал на ``git show`` с ``UnicodeDecodeError``. Декодирование
    с ``errors="replace"`` сохраняет вывод и не ломает аудит.
    """
    result = subprocess.run(
        args,
        input=stdin,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return result.stdout


@beartype
def patch_id(patch: str, repo: Path | None = None) -> str:
    """``git patch-id --stable`` от текста патча (работает и без репозитория)."""
    if patch.strip() == "":
        return ""
    args = ["git"]
    if repo is not None:
        args += ["-C", str(repo)]
    args += ["patch-id", "--stable"]
    parts = _run_git(args, stdin=patch).split()
    return parts[0] if parts else ""


@beartype
def task_patch_id(task: TaskRef, repo: Path | None = None) -> str:
    """``patch_id`` эталона: gold + test воспроизводят diff коммита целиком."""
    return patch_id(task.gold_patch + task.test_patch, repo)


@beartype
def canaries(task: TaskRef, repo: Path) -> list[str]:
    """Строки-канарейки: нетривиальные и отсутствующие в дереве базы.

    Строка, которая уже есть в базе, канарейкой быть не может: агент вправе её
    увидеть, не нарушая отсечку.
    """
    candidates = nontrivial_lines(task.gold_patch)
    if not candidates:
        return []
    present = _grep_in_tree(repo, task.base, candidates)
    return [line for line in candidates if line not in present]


@beartype
def _grep_in_tree(repo: Path, sha: str, lines: list[str]) -> set[str]:
    """Возвращает те строки, что встречаются в дереве ``sha`` (один ``git grep``)."""
    if not lines:
        return set()
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as handle:
        handle.write("".join(f"{line}\n" for line in lines))
        pattern_path = handle.name
    try:
        output = _run_git(["git", "-C", str(repo), "grep", "-F", "-f", pattern_path, sha])
    finally:
        Path(pattern_path).unlink(missing_ok=True)
    # Формат строки: ``<sha>:<path>:<text>``; путь отделяется вторым двоеточием.
    found: set[str] = set()
    for row in output.splitlines():
        first = row.find(":")
        if first < 0:
            continue
        second = row.find(":", first + 1)
        if second < 0:
            continue
        text = row[second + 1 :]
        if text in lines:
            found.add(text)
    return found


@beartype
def _graph_db(data: Path | None = None) -> Path:
    return paths.graph_dir(data) / "graph.sqlite"


@beartype
def _open_graph(data: Path | None = None) -> sqlite3.Connection | None:
    path = _graph_db(data)
    if not path.is_file():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# Кэш аудита: сотни задач читали бы одну и ту же таблицу родителей.
_ANCESTORS_CACHE: dict[str, frozenset[str]] = {}
_PATCH_ID_CACHE: dict[str, dict[str, str]] = {}
_PARENTS_CACHE: dict[str, dict[str, list[str]]] = {}


@beartype
def _parent_map(data: Path | None = None) -> dict[str, list[str]]:
    key = str(_graph_db(data))
    cached = _PARENTS_CACHE.get(key)
    if cached is not None:
        return cached
    conn = _open_graph(data)
    parents: dict[str, list[str]] = {}
    if conn is None:
        _PARENTS_CACHE[key] = parents
        return parents
    try:
        for row in conn.execute("SELECT oid, parent_oid FROM parents"):
            parents.setdefault(str(row[0]), []).append(str(row[1]))
    finally:
        conn.close()
    _PARENTS_CACHE[key] = parents
    return parents


@beartype
def _ancestors(repo: Path, base: str, data: Path | None = None) -> set[str]:
    """Предки ``base`` по ``parents`` графа, иначе ``git rev-list``."""
    conn = _open_graph(data)
    if conn is not None:
        try:
            present = conn.execute("SELECT 1 FROM commits WHERE oid = ?", (base,)).fetchone()
        finally:
            conn.close()
        if present is not None:
            return _ancestors_in_graph(base, data)
    output = _run_git(["git", "-C", str(repo), "rev-list", base])
    return set(output.split())


@beartype
def _ancestors_in_graph(base: str, data: Path | None = None) -> set[str]:
    parents = _parent_map(data)
    seen: set[str] = set()
    stack = [base]
    while stack:
        oid = stack.pop()
        if oid in seen:
            continue
        seen.add(oid)
        stack.extend(parents.get(oid, []))
    return seen


@beartype
def _reachable(repo: Path, base: str, data: Path | None = None) -> frozenset[str]:
    """``_ancestors`` с кэшем: одна и та же база проверяется многократно."""
    key = f"{repo}::{base}"
    cached = _ANCESTORS_CACHE.get(key)
    if cached is None:
        cached = frozenset(_ancestors(repo, base, data))
        _ANCESTORS_CACHE[key] = cached
    return cached


@beartype
def _patch_ids(data: Path | None = None) -> dict[str, str]:
    """``oid → patch_id`` из ``commits`` единого графа."""
    key = str(_graph_db(data))
    cached = _PATCH_ID_CACHE.get(key)
    if cached is not None:
        return cached
    found: dict[str, str] = {}
    conn = _open_graph(data)
    if conn is not None:
        try:
            for row in conn.execute("SELECT oid, patch_id FROM commits WHERE patch_id != ''"):
                found[str(row["oid"])] = str(row["patch_id"])
        finally:
            conn.close()
    _PATCH_ID_CACHE[key] = found
    return found


@beartype
def prior_twin(
    task: TaskRef,
    repo: Path,
    archive: object | None = None,
    threshold: float = PRIOR_TWIN_THRESHOLD,
    data: Path | None = None,
) -> dict[str, object]:
    """Ищет повторное применение эталона среди предков ``base``.

    Быстрый путь — точное совпадение ``patch_id`` в ``commits``. Медленный —
    доля совпавших нетривиальных добавленных строк у предков, тронувших те же
    пути (``file_change``).
    """
    del archive
    target_id = task_patch_id(task, repo)
    evidence: list[str] = []
    ancestors = _reachable(repo, task.base, data)
    if not ancestors:
        return {
            "prior_twin": False,
            "kind": "no_ancestors",
            "patch_id": target_id,
            "evidence": [],
        }

    ids = _patch_ids(data)
    if target_id != "":
        exact = [oid for oid in ancestors if ids.get(oid, "") == target_id]
        if exact:
            for oid in exact[:5]:
                evidence.append(f"patch_id {target_id[:12]} совпал с предком {oid[:12]}")
            return {
                "prior_twin": True,
                "kind": "patch_id",
                "patch_id": target_id,
                "evidence": evidence,
            }

    wanted = nontrivial_lines(task.gold_patch)
    if not wanted:
        return {
            "prior_twin": False,
            "kind": "no_nontrivial_lines",
            "patch_id": target_id,
            "evidence": [],
        }
    candidates = _path_candidates(task, ancestors, data)
    shown_by_commit = added_lines_many(repo, candidates)
    for oid in candidates:
        shown = shown_by_commit.get(oid, set())
        if not shown:
            continue
        hits = sum(1 for line in wanted if line in shown)
        ratio = hits / len(wanted)
        if ratio >= threshold:
            evidence.append(
                f"предок {oid[:12]} содержит {hits}/{len(wanted)} нетривиальных строк"
            )
            return {
                "prior_twin": True,
                "kind": "line_overlap",
                "patch_id": target_id,
                "ratio": round(ratio, 4),
                "evidence": evidence,
            }
    return {
        "prior_twin": False,
        "kind": "none",
        "patch_id": target_id,
        "evidence": [],
    }


@beartype
def added_lines_many(repo: Path, oids: list[str]) -> dict[str, set[str]]:
    """Добавленные строки для пачки коммитов одним ``git show``.

    ``git show`` принимает несколько коммитов и печатает их подряд; границы
    восстанавливаются по маркеру ``__COMMIT__<oid>``. Маркер уникален, поэтому
    добавленная строка из одних hex-символов не может быть принята за заголовок.
    """
    if not oids:
        return {}
    output = _run_git(
        [
            "git",
            "-C",
            str(repo),
            "show",
            "--format=__COMMIT__%H",
            "-U0",
            "--no-renames",
            *oids,
        ]
    )
    result: dict[str, set[str]] = {}
    current = ""
    for line in output.splitlines():
        if line.startswith("__COMMIT__"):
            current = line[len("__COMMIT__") :].strip()
            result.setdefault(current, set())
            continue
        if current != "" and line.startswith("+") and not line.startswith("+++"):
            result[current].add(line[1:])
    return result


@beartype
def _path_candidates(
    task: TaskRef,
    ancestors: set[str] | frozenset[str],
    data: Path | None = None,
) -> list[str]:
    """Ближайшие предки, менявшие те же пути, по ``file_change``."""
    if not task.paths_touched:
        return []
    conn = _open_graph(data)
    if conn is None:
        return []
    try:
        placeholders = ",".join("?" for _ in task.paths_touched)
        rows = conn.execute(
            f"SELECT DISTINCT oid FROM file_change WHERE path IN ({placeholders})",
            list(task.paths_touched),
        ).fetchall()
    finally:
        conn.close()
    return [str(row["oid"]) for row in rows if str(row["oid"]) in ancestors][
        :PRIOR_TWIN_CANDIDATES
    ]


@beartype
def show_added_lines(repo: Path, oid: str) -> set[str]:
    """Добавленные строки одного коммита (эталон для сравнения с пачкой)."""
    output = _run_git(["git", "-C", str(repo), "show", "--format=", "-U0", oid])
    return {
        line[1:]
        for line in output.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    }


@beartype
def scan_responses(
    responses: dict[str, str],
    canary_list: list[str],
    head_sha: str,
) -> list[str]:
    """Ищет канарейки и ``head_sha`` в текстах ответов инструментов."""
    violations: list[str] = []
    for tool, text in responses.items():
        for canary in canary_list:
            if canary in text:
                violations.append(f"{tool}: канарейка «{canary[:60]}»")
        if head_sha != "" and head_sha in text:
            violations.append(f"{tool}: найден head_sha {head_sha[:12]}")
    return violations


@beartype
def check_membership(
    responses: dict[str, str],
    allowed: set[str],
) -> list[str]:
    """Проверяет, что каждый 40-символьный hex-oid в ответах входит в предков."""
    violations: list[str] = []
    seen: set[str] = set()
    for tool, text in responses.items():
        for oid in _OID_RE.findall(text):
            if oid in seen:
                continue
            seen.add(oid)
            if oid not in allowed:
                violations.append(f"{tool}: oid {oid[:12]} вне предков base_sha")
    return violations


_OID_RE = re.compile(r"\b[0-9a-f]{40}\b")


@beartype
def negative_control(
    caller: "ToolCaller",
    task: TaskRef,
    repo: Path,
) -> list[str]:
    """Проверяет отказ по ``head_sha`` и по потомкам ``base``.

    Потомок берётся как первый коммит после ``base`` на мастер-линии, если он
    есть в клоне; иначе проверяется только ``head_sha``.
    """
    violations: list[str] = []
    head_text = caller("read_versioned_source", {"revision": task.head, "path": _first_path(task)})
    if "cutoff" not in head_text and "error" not in head_text:
        violations.append("head_sha не отклонён")
    descendant = _first_descendant(repo, task.base)
    if descendant != "":
        text = caller(
            "read_versioned_source", {"revision": descendant, "path": _first_path(task)}
        )
        if "cutoff" not in text and "error" not in text:
            violations.append(f"потомок {descendant[:12]} не отклонён")
    return violations


@beartype
def _first_path(task: TaskRef) -> str:
    return task.paths_touched[0] if task.paths_touched else "src/backend/x.c"


@beartype
def _first_descendant(repo: Path, base: str) -> str:
    output = _run_git(
        ["git", "-C", str(repo), "rev-list", "--all", "--children", "--max-count=1", base]
    )
    parts = output.split()
    return parts[1] if len(parts) > 1 else ""


# Протокол вызывателя: имя инструмента + аргументы → текст ответа.
class ToolCaller:
    def __call__(self, tool: str, args: dict[str, object]) -> str:  # pragma: no cover
        raise NotImplementedError

@beartype
def audit_task(
    name: str,
    data: Path | None = None,
    split: dict[str, list[str]] | None = None,
) -> LeakReport:
    """Полный leak-аудит одной задачи без вызова инструментов.

    Проверяет membership задачи в split, считает ``patch_id``, ищет prior-twin и
    строит список канареек. Проверка ответов инструментов делается отдельно
    (``scan_responses``/``check_membership``) на живом сервере.
    """
    repo = paths.pg_clone(data)
    active_split = split_module.derive_split(data) if split is None else split
    section = split_module.assert_membership(active_split, name)
    task = load_task(name, data)
    report = LeakReport(task=name, section=section)
    report.patch_id = task_patch_id(task, repo)
    twin = prior_twin(task, repo, data=data)
    report.prior_twin = bool(twin["prior_twin"])
    report.prior_twin_kind = str(twin["kind"])
    evidence = twin.get("evidence")
    if isinstance(evidence, list):
        report.prior_twin_evidence = [str(item) for item in cast("list[object]", evidence)]
    report.canaries = len(canaries(task, repo))
    return report


@beartype
def prism_coverage(names: list[str], data: Path | None = None) -> dict[str, object]:
    """Какие базы задач есть в ``graph.sqlite``.

    Имя функции сохранено: отчёт eval читает ключ ``prism_missing_tasks``.
    Пустой граф помечает все задачи как непокрытые.
    """
    conn = _open_graph(data)
    if conn is None:
        return {
            "covered": 0,
            "missing": len(names),
            "covered_tasks": [],
            "missing_tasks": sorted(names),
        }
    covered: list[str] = []
    missing: list[str] = []
    try:
        for name in names:
            base = load_task(name, data).base
            row = conn.execute("SELECT 1 FROM commits WHERE oid = ? LIMIT 1", (base,)).fetchone()
            (covered if row is not None else missing).append(name)
    finally:
        conn.close()
    return {
        "covered": len(covered),
        "missing": len(missing),
        "covered_tasks": sorted(covered),
        "missing_tasks": sorted(missing),
    }


@beartype
def audit_split(
    data: Path | None = None,
    section: str = "holdout",
    progress_path: Path | None = None,
) -> dict[str, object]:
    """Аудит всей секции split: membership, prior-twin, канарейки и покрытие.

    Результат по каждой задаче дописывается в ``progress_path`` (JSON Lines).
    Полный holdout идёт десятки минут, а ``prior-twin`` не зависит от кода
    инструментов: без этого кэша правка отчёта заставляла бы перечитывать 280
    задач заново.
    """
    active_split = split_module.derive_split(data)
    names = active_split[section]
    done = read_progress(progress_path)
    reports: list[LeakReport] = []
    for name in names:
        cached = done.get(name)
        if cached is not None:
            reports.append(cached)
            continue
        report = audit_task(name, data, active_split)
        reports.append(report)
        append_progress(progress_path, report)
    twins = [report for report in reports if report.prior_twin]
    coverage = prism_coverage(names, data)
    return {
        "section": section,
        "tasks": len(reports),
        "prior_twin": len(twins),
        "prior_twin_tasks": [report.task for report in twins],
        "canaries_total": sum(report.canaries for report in reports),
        "canaries_min": min((report.canaries for report in reports), default=0),
        "prism_covered": coverage["covered"],
        "prism_missing": coverage["missing"],
        "prism_missing_tasks": coverage["missing_tasks"],
        "violations": [item for report in reports for item in report.violations],
        "ok": all(report.ok for report in reports),
    }


@beartype
def progress_file(data: Path | None, section: str) -> Path:
    """Путь кэша аудита секции: ``data/reports/leak-progress-<section>.jsonl``."""
    return paths.reports_dir(data) / f"leak-progress-{section}.jsonl"


@beartype
def read_progress(path: Path | None) -> dict[str, LeakReport]:
    """Читает кэш аудита; битые строки пропускаются, а не роняют прогон."""
    if path is None or not path.is_file():
        return {}
    found: dict[str, LeakReport] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line == "":
            continue
        try:
            raw: object = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(raw, dict):
            continue
        item = cast("dict[str, object]", raw)
        report = LeakReport(
            task=str(item.get("task", "")),
            section=str(item.get("section", "")),
        )
        report.patch_id = str(item.get("patch_id", ""))
        report.prior_twin = bool(item.get("prior_twin"))
        report.prior_twin_kind = str(item.get("prior_twin_kind", ""))
        evidence = item.get("prior_twin_evidence")
        if isinstance(evidence, list):
            report.prior_twin_evidence = [str(x) for x in cast("list[object]", evidence)]
        report.canaries = int(cast("int", item.get("canaries", 0)))
        violations = item.get("violations")
        if isinstance(violations, list):
            report.violations = [str(x) for x in cast("list[object]", violations)]
        if report.task != "":
            found[report.task] = report
    return found


@beartype
def append_progress(path: Path | None, report: LeakReport) -> None:
    """Дописывает одну строку кэша; ``None`` означает «кэш не вести»."""
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(report.to_dict(), ensure_ascii=False) + "\n")


@beartype
def holdout_fingerprint(names: list[str]) -> str:
    """Хэш замороженного holdout: печатается в manifest прогона."""
    return hashlib.sha256(
        "".join(f"{name}\n" for name in sorted(names)).encode("utf-8")
    ).hexdigest()
