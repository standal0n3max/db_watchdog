"""Инструменты графа v2: история, которой нет в дереве.

Четыре инструмента работают поверх двух баз одновременно:

- ``history.sqlite`` (наш производный индекс из PRISM) — история символов,
  совместные изменения, полнотекстовый поиск по сообщениям и полная цепочка
  родителей;
- ``postgres.sqlite`` (индекс v1) — источник аудита: каждый отданный ответ
  пишется в его ``audit``, как и у v1-инструментов.

Почему arbiter — ``parents`` из ``history.sqlite``, а не окно v1: план требует
глубину «полная по first-parent от union баз, не 250». Корпус PRISM знает всю
историю (105k коммитов), поэтому членство считается по нему. Клон не нужен:
``Cutoff`` собирается из той же таблицы ``parents``.

Отсечка не ослабляется: каждый ``oid`` проходит ``Cutoff.allows``, поэтому
``head_sha`` и его потомки не выдаются никогда.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark.graph import history_store, store
from db_pipeline.benchmark.graph.cutoff import Cutoff, refusal

DEFAULT_LIMIT = 20

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")

# Диапазон символа: только определения, не прототипы (роль parsed_declaration).
_DEFINITION_ROLE = "parsed_definition"


@beartype
@dataclass(frozen=True)
class HistoryIndex:
    """Производная история, соединение аудита и полная отсечка по ``parents``."""

    history: sqlite3.Connection
    store_conn: sqlite3.Connection
    cutoff: Cutoff

    @beartype
    def allowed(self, oid: str) -> bool:
        """Коммит допущен, если он предок базы по полной цепочке ``parents``."""
        return self.cutoff.allows(oid, 0)


@beartype
def open_index(history_db: Path, store_db: Path, cutoff: Cutoff) -> HistoryIndex:
    """Открывает историю только для чтения и индекс v1 для аудита."""
    return HistoryIndex(
        history=history_store.open_history_readonly(history_db),
        store_conn=store.open_store(store_db),
        cutoff=cutoff,
    )


@beartype
def cutoff_for_base(history_db: Path, base_sha: str, forbidden_head: str = "") -> Cutoff:
    """Строит отсечку по полной истории: предки ``base_sha`` из ``parents``.

    ``base_time`` берётся из ``commits``, чтобы ответы несли время базы; само
    разрешение опирается только на предковство, потому что часы коммитов не
    монотонны.
    """
    conn = history_store.open_history_readonly(history_db)
    try:
        reachable = history_store.ancestors(conn, base_sha)
        row = conn.execute(
            "SELECT committed_at FROM commits WHERE oid = ?", (base_sha,)
        ).fetchone()
    finally:
        conn.close()
    base_time = _parse_time(str(row[0])) if row is not None else 0
    return _build(base_sha, base_time, reachable, forbidden_head)


@beartype
def _build(base_sha: str, base_time: int, reachable: set[str], forbidden_head: str) -> Cutoff:
    from db_pipeline.benchmark.graph.cutoff import build_cutoff

    allowed = {sha for sha in reachable if sha != forbidden_head}
    return build_cutoff(base_sha, base_time, allowed, forbidden_head=forbidden_head)


@beartype
def _parse_time(raw: str) -> int:
    """Переводит ISO-время PRISM в epoch-секунды; неразобранное — 0."""
    text = raw.strip()
    if text == "":
        return 0
    candidate = text.replace(" ", "T")
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp())


@beartype
def close_index(index: HistoryIndex) -> None:
    index.history.close()
    index.store_conn.close()


@beartype
def _now() -> str:
    return datetime.now(UTC).isoformat()


@beartype
def _audit(index: HistoryIndex, tool: str, args: str, empty: bool) -> None:
    store.log_audit(index.store_conn, tool, args, empty, ts=_now())


@beartype
def _cutoff_meta(cutoff: Cutoff) -> dict[str, object]:
    return {"base_sha": cutoff.base_sha, "base_time": cutoff.base_time, "ancestry_enforced": True}


@beartype
def symbol_history(
    index: HistoryIndex,
    path: str,
    name: str,
    limit: int = DEFAULT_LIMIT,
) -> dict[str, object]:
    """История символа ``name`` в файле ``path`` до отсечки.

    Возвращает коммиты от свежих к старым с subject, ролью символа и строками
    самого символа (``sym_line``), а не диапазоном ханка. Прототипы
    (``parsed_declaration``) не отбрасываются, но помечаются ролью.
    """
    rows = index.history.execute(
        "SELECT s.oid, s.path, s.name, s.kind, s.role, s.line, s.end_line, "
        "s.sym_line, s.sym_end_line, s.side, c.subject, c.authored_at "
        "FROM symbol_history s LEFT JOIN commits c ON c.oid = s.oid "
        "WHERE s.path = ? AND s.name = ? LIMIT ?",
        (path, name, 4000),
    ).fetchall()
    best: dict[str, sqlite3.Row] = {}
    for row in rows:
        oid = str(row["oid"])
        if not index.allowed(oid):
            continue
        current = best.get(oid)
        if current is None or _prefer(row, current):
            best[oid] = row
    # План требует порядок «от свежих к старым»: сортируем по времени автора,
    # а не по oid — хэш не отражает хронологию.
    ordered = sorted(
        best.values(),
        key=lambda row: (str(row["authored_at"]), str(row["oid"])),
        reverse=True,
    )
    results: list[dict[str, object]] = []
    for row in ordered[:limit]:
        oid = str(row["oid"])
        results.append(
            {
                "revision": oid,
                "path": str(row["path"]),
                "name": str(row["name"]),
                "kind": str(row["kind"]),
                "role": str(row["role"]),
                "is_definition": str(row["role"]) == _DEFINITION_ROLE,
                "line": int(row["sym_line"]),
                "end_line": int(row["sym_end_line"]),
                "hunk_line": int(row["line"]),
                "hunk_end_line": int(row["end_line"]),
                "side": str(row["side"]),
                "subject": str(row["subject"] or ""),
                "authored_at": str(row["authored_at"] or ""),
                "cutoff": _cutoff_meta(index.cutoff),
            }
        )
    _audit(index, "symbol_history", f"{path}:{name}", len(results) == 0)
    return {"results": results, "cutoff": _cutoff_meta(index.cutoff)}


@beartype
def _prefer(candidate: sqlite3.Row, current: sqlite3.Row) -> bool:
    """Определение предпочтительнее прототипа при равных прочих."""
    candidate_def = str(candidate["role"]) == _DEFINITION_ROLE
    current_def = str(current["role"]) == _DEFINITION_ROLE
    if candidate_def != current_def:
        return candidate_def
    return int(candidate["sym_line"]) < int(current["sym_line"])


@beartype
def tests_to_code(
    index: HistoryIndex,
    test: str,
    limit: int = DEFAULT_LIMIT,
) -> dict[str, object]:
    """Файлы кода, исторически менявшиеся вместе с тестом ``test``.

    Связь берётся из ``co_change``, но показывается только если она подтверждена
    окном отсечки: у теста должен быть хотя бы один **тожественный** коммит-предок,
    менявший и тест, и файл кода. Коммит, задавший пару, в ``co_change`` не
    сохранялся, поэтому пересечение считается по ``path_commits``.

    Без этой сверки инструмент отдавал бы связи, выученные на коммитах после
    ``base_sha``: если PRISM вообще не знает базы задачи, любой файл выглядел бы
    «исторически связанным». Это и есть утечка, а не полезный ответ.
    """
    rows = index.history.execute(
        "SELECT code_path, weight, commits FROM co_change WHERE test_path = ? "
        "ORDER BY weight DESC LIMIT ?",
        (test, max(limit * 8, 80)),
    ).fetchall()
    test_oids = _allowed_path_oids(index, test)
    if not test_oids:
        _audit(index, "tests_to_code", test, True)
        return {"results": [], "cutoff": _cutoff_meta(index.cutoff)}
    results: list[dict[str, object]] = []
    for row in rows:
        code_path = str(row["code_path"])
        shared = _shared_allowed_oid(index, code_path, test_oids)
        if shared == "":
            continue
        results.append(
            {
                "test_path": test,
                "code_path": code_path,
                "weight": float(row["weight"]),
                "commits": int(row["commits"]),
                "sample_revision": shared,
                "cutoff": _cutoff_meta(index.cutoff),
            }
        )
        if len(results) >= limit:
            break
    _audit(index, "tests_to_code", test, len(results) == 0)
    return {"results": results, "cutoff": _cutoff_meta(index.cutoff)}


@beartype
def _allowed_path_oids(index: HistoryIndex, path: str) -> set[str]:
    """Коммиты, тронувшие ``path`` и допущенные отсечкой (по ``parents``)."""
    rows = index.history.execute(
        "SELECT oid FROM path_commits WHERE path = ? LIMIT 20000", (path,)
    ).fetchall()
    return {str(row["oid"]) for row in rows if index.allowed(str(row["oid"]))}


@beartype
def _shared_allowed_oid(
    index: HistoryIndex,
    code_path: str,
    test_oids: set[str],
) -> str:
    """Допустимый коммит, менявший и тест, и файл кода: подтверждение связи.

    Пустая строка означает, что пара ``тест → код`` ничем в окне не подтверждена,
    и её нельзя показывать.
    """
    rows = index.history.execute(
        "SELECT oid FROM path_commits WHERE path = ? LIMIT 20000", (code_path,)
    ).fetchall()
    for row in rows:
        oid = str(row["oid"])
        if oid in test_oids:
            return oid
    return ""


@beartype
def similar_fixes(
    index: HistoryIndex,
    query: str,
    limit: int = DEFAULT_LIMIT,
) -> dict[str, object]:
    """Поиск похожих правок по subject/сообщениям, ограниченный предками.

    FTS по ``commits_fts`` (унаследовано из корпуса PRISM), затем membership по
    v1-окну. Токены объединяются через OR с префиксным совпадением, чтобы
    естественно-языковой запрос не давал ноль из-за строгого AND.
    """
    match = _fts_query(query)
    rows = index.history.execute(
        "SELECT f.oid AS oid, c.subject AS subject, c.authored_at AS authored_at "
        "FROM commits_fts f JOIN commits c ON c.oid = f.oid "
        "WHERE commits_fts MATCH ? ORDER BY bm25(commits_fts) LIMIT ?",
        (match, max(limit * 10, 100)),
    ).fetchall()
    results: list[dict[str, object]] = []
    for row in rows:
        oid = str(row["oid"])
        if not index.allowed(oid):
            continue
        results.append(
            {
                "revision": oid,
                "subject": str(row["subject"]),
                "authored_at": str(row["authored_at"]),
                "cutoff": _cutoff_meta(index.cutoff),
            }
        )
        if len(results) >= limit:
            break
    _audit(index, "similar_fixes", query, len(results) == 0)
    return {"results": results, "cutoff": _cutoff_meta(index.cutoff)}


@beartype
def _fts_query(query: str) -> str:
    tokens = [token for token in _TOKEN_RE.findall(query) if token != ""]
    if len(tokens) == 0:
        return '"__no_match__"'
    return " OR ".join(f'"{token}"*' for token in tokens)


@beartype
def find_error_origin(
    index: HistoryIndex,
    message: str,
    limit: int = DEFAULT_LIMIT,
) -> dict[str, object]:
    """Ищет источник сообщения ``ereport``/``errmsg`` по нормализованному тексту.

    Единственный собственный индекс графа v2: шаблоны сообщений PostgreSQL с
    номерами строк. Нужен для вывода ``expected/*.out`` из тестов: агент видит
    текст ошибки и должен понять, какой код его печатает.

    Отдаются только записи текущей ``base_sha``. Это принципиально: индекс
    собран из деревьев всех 300 баз, а базы задач разнесены по времени более чем
    на год, поэтому дерево «будущей» задачи содержит строки, которых в базе
    текущей ещё нет.
    """
    tokens = _TOKEN_RE.findall(message)
    if len(tokens) == 0:
        _audit(index, "find_error_origin", message, True)
        return {"results": [], "cutoff": _cutoff_meta(index.cutoff)}
    needle = tokens[0]
    rows = index.history.execute(
        "SELECT message, path, line FROM error_messages "
        "WHERE message LIKE ? AND base_sha = ? LIMIT ?",
        (f"%{needle}%", index.cutoff.base_sha, 4000),
    ).fetchall()
    wanted = _normalize(message)
    scored: list[tuple[float, sqlite3.Row]] = []
    for row in rows:
        stored = str(row["message"])
        score = _similarity(wanted, stored)
        if score <= 0:
            continue
        scored.append((score, row))
    scored.sort(key=lambda item: (-item[0], str(item[1]["path"])))
    head = scored[:limit]
    cutoff_paths = _paths_touched_in_cutoff(
        index, sorted({str(row["path"]) for _, row in head})
    )
    results: list[dict[str, object]] = []
    for score, row in head:
        path = str(row["path"])
        results.append(
            {
                "message": str(row["message"]),
                "path": path,
                "line": int(row["line"]),
                "score": round(score, 4),
                "in_cutoff_window": path in cutoff_paths,
                "cutoff": _cutoff_meta(index.cutoff),
            }
        )
        if len(results) >= limit:
            break
    _audit(index, "find_error_origin", message, len(results) == 0)
    return {"results": results, "cutoff": _cutoff_meta(index.cutoff)}


@beartype
def _normalize(message: str) -> str:
    """Нормализует запрос: формат-спецификаторы → ``%``, пробелы сжаты."""
    from db_pipeline.benchmark.graph.history import normalize_message

    return normalize_message(message)


@beartype
def _paths_touched_in_cutoff(index: HistoryIndex, paths: list[str]) -> set[str]:
    """Подмножество ``paths``, тронутое хотя бы одним предком базы.

    Проверка идёт по ``path_commits`` (полная история PRISM) и множеству
    ``cutoff.reachable``. Это дешёвый признак «файл жив в окне задачи»: у
    сообщения из дерева базы он почти всегда ``True``, а вот путь, известный
    только из более позднего дерева, сюда не попадёт.
    """
    reachable = index.cutoff.reachable
    found: set[str] = set()
    for path in paths:
        rows = index.history.execute(
            "SELECT oid FROM path_commits WHERE path = ? LIMIT 20000", (path,)
        ).fetchall()
        if any(str(row["oid"]) in reachable for row in rows):
            found.add(path)
    return found


@beartype
def _similarity(left: str, right: str) -> float:
    """Доля общих слов: дешёвая мера похожести двух сообщений.

    ``message`` хранится нормализованным, поэтому сравнение слов устойчиво к
    ``%s``/``%d`` и кавычкам. Возвращает 0, если совпало меньше трети.
    """
    left_words = {word for word in left.split() if len(word) >= 3}
    right_words = {word for word in right.split() if len(word) >= 3}
    if not left_words or not right_words:
        return 0.0
    common = left_words & right_words
    ratio = len(common) / max(1, min(len(left_words), len(right_words)))
    if ratio < 0.34:
        return 0.0
    return ratio


@beartype
def empty_response(reason: str, cutoff: Cutoff) -> dict[str, object]:
    """Единый отказ: сохраняет форму ответа инструментов."""
    payload = refusal(reason)
    payload["cutoff"] = _cutoff_meta(cutoff)
    return payload
