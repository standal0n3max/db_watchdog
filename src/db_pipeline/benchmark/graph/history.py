"""Билдер производного ``history.sqlite`` из проверенных данных PRISM.

Читает архив PRISM строго read-only и локальный git-клон (байты исходников и
номера строк) и наполняет таблицы истории:

- ``commits`` — из корпуса PRISM (даты, subject, message, ``patch_id``);
- ``symbol_history`` — ``hunks ⋈ symbol_overlaps``: для каждой стороны ханка
  переводит байтовые смещения в 1-based номера строк по blob'у стороны;
- ``co_change`` — из ``paths``: матрица «тест → код», вес ``1/число_путей``,
  массовые коммиты отброшены;
- ``error_messages`` — шаблоны ``errmsg`` по ``.c`` файлам деревьев баз.

Ресурсный контур: blob'ы читаются **потоково** через один долгоживущий
``git cat-file --batch``; строки записей сортируются по ``blob_oid``, поэтому
содержимое каждого blob'а держится в памяти лишь пока идут его записи. Номера
строк считаются по байтам (``bytes.count(b"\\n", 0, offset)``) без разбиения
файла на список строк — иначе 160k blob'ов не помещаются в память.

SECURITY NOTE (это не абстракция, а реальная утечка): индексировать ``errmsg``
по деревьям **всех** 300 баз нельзя. Базы задач разнесены по времени более чем
на год (2025-09-15 … 2026-10-10), поэтому дерево базы «будущей» задачи содержит
строки правки, которой в базе текущей задачи ещё нет. Поэтому каждая запись
привязана к ``base_sha`` той базы, из дерева которой она прочитана, и инструмент
отдаёт только записи текущей ``base_sha``.

Билдер идемпотентен: ``--rebuild`` очищает таблицы, иначе записи докидываются
через ``INSERT OR REPLACE``. В manifest пишутся версия билдера, ``sha256``
архивных баз, хэш схемы и число строк.
"""

from __future__ import annotations

import re
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import paths
from db_pipeline.benchmark.graph import history_store, prism
from db_pipeline.errors import PipelineError

BUILDER_VERSION = "history-builder-3"

# Массовые коммиты (pgindent, переводы, catversion) искажают co-change.
BULK_PATH_LIMIT = 8

# Размер пачки записей перед ``executemany``.
FLUSH_ROWS = 20000

# Шаблон ereport/errmsg: имя функции + строковый литерал.
_ERRMSG_RE = re.compile(
    r"\b(?:errmsg|errmsg_plural|errhint|errdetail|errdetail_log|errcontext)\s*\(\s*"
    r'"((?:[^"\\]|\\.)*)"',
)

# Нормализация формата printf внутри сообщения.
_FORMAT_SPEC_RE = re.compile(r"%[#0\- +']*[\d.*]*(?:hh|h|ll|l|L|z|j|t)?[a-zA-Z]")

_TEST_MARKERS = ("/expected/", "/sql/", "/specs/", "/test/")
_TEST_SUFFIXES = (".out", ".sql", ".spec")


@beartype
@dataclass(frozen=True)
class BuildReport:
    """Итог сборки: счётчики, manifest и путь к базе."""

    db: str
    rows: dict[str, int]
    manifest: dict[str, str]

    def to_dict(self) -> dict[str, object]:
        return {"db": self.db, "rows": dict(self.rows), "manifest": dict(self.manifest)}


# --------------------------------------------------------------------------
# Потоковое чтение blob'ов
# --------------------------------------------------------------------------
class BlobReader:
    """Один долгоживущий ``git cat-file --batch`` с кэшем последнего blob'а.

    Записи о символах приходят упорядоченными по ``blob_oid``, поэтому кэш из
    одного элемента даёт 100% попаданий и держит в памяти лишь текущий файл.
    """

    def __init__(self, repo: Path) -> None:
        self._repo = repo
        self._proc: subprocess.Popen[bytes] = subprocess.Popen(
            ["git", "-C", str(repo), "cat-file", "--batch"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        self._current: str = ""
        self._cached: bytes = b""

    def read(self, oid: str) -> bytes:
        """Возвращает содержимое blob'а; повторный вызов того же oid — из кэша."""
        if oid == self._current:
            return self._cached
        if oid == "":
            return b""
        stdin = self._proc.stdin
        stdout = self._proc.stdout
        if stdin is None or stdout is None:
            raise PipelineError("history", "cat-file без stdin/stdout")
        stdin.write(f"{oid}\n".encode("ascii"))
        stdin.flush()
        header = stdout.readline()
        if header == b"":
            raise PipelineError("history", "cat-file не вернул заголовок")
        parts = header.decode("utf-8", "replace").split()
        if len(parts) < 3:
            self._current = oid
            self._cached = b""
            return b""
        try:
            size = int(parts[2])
        except ValueError as exc:
            raise PipelineError("history", f"битый размер blob'а {oid[:12]}") from exc
        content = stdout.read(size)
        stdout.read(1)
        self._current = oid
        self._cached = content
        return content

    def close(self) -> None:
        stdin = self._proc.stdin
        if stdin is not None and not stdin.closed:
            stdin.close()
        self._proc.wait(timeout=30)


# --------------------------------------------------------------------------
# Нормализация и разбор
# --------------------------------------------------------------------------
@beartype
def normalize_message(message: str) -> str:
    """Нормализует сообщение: формат-спецификаторы → ``%``, пробелы сжаты."""
    without_specs = _FORMAT_SPEC_RE.sub("%", message)
    unescaped = without_specs.replace('\\"', '"').replace("\\\\", "\\")
    return re.sub(r"\s+", " ", unescaped).strip()


@beartype
def is_test_path(path: str) -> bool:
    """Путь считается тестовым по маркеру каталога либо суффиксу."""
    if any(marker in path for marker in _TEST_MARKERS):
        return True
    return path.endswith(_TEST_SUFFIXES)


@beartype
def byte_to_line(blob: bytes, offset: int) -> int:
    """Номер строки (1-based) для байтового смещения внутри blob."""
    if offset <= 0:
        return 1
    return blob.count(b"\n", 0, min(offset, len(blob))) + 1


@beartype
def byte_span_to_lines(blob: bytes, start: int, end: int) -> tuple[int, int]:
    """Диапазон строк для байтового среза ``[start, end)`` внутри blob.

    Семантика полей PRISM: ``old_sha256`` — хэш среза ``blob[start:end]``,
    значит ``end`` указывает на байт сразу за последним байтом среза.
    """
    total = len(blob)
    if total == 0 or end <= start:
        line = byte_to_line(blob, max(start, 0))
        return line, line
    cut = min(end, total)
    first = byte_to_line(blob, max(start, 0))
    last = byte_to_line(blob, cut)
    if cut > 0 and blob[cut - 1 : cut] == b"\n":
        last -= 1
    return first, max(first, last)


# --------------------------------------------------------------------------
# Сборка
# --------------------------------------------------------------------------
@beartype
def build(
    data: Path | None = None,
    archive: prism.PrismArchive | None = None,
    rebuild: bool = True,
    index_errors: bool = True,
    base_shas: list[str] | None = None,
) -> BuildReport:
    """Собирает ``history.sqlite`` целиком."""
    active = prism.load_archive() if archive is None else archive
    missing = active.present()
    if missing:
        raise FileNotFoundError(f"нет баз PRISM: {missing}")
    repo = paths.pg_clone(data)
    if not (repo / ".git").exists():
        raise FileNotFoundError(f"нет клона {repo}")
    target = paths.graph_dir(data) / "history.sqlite"
    conn = history_store.open_history(target)
    try:
        if rebuild:
            _drop(conn)
        _build_commits(conn, active)
        _build_parents(conn, active)
        _build_symbol_history(conn, active, repo)
        _build_co_change(conn, active)
        if index_errors:
            shas = base_shas if base_shas is not None else _task_bases(data)
            _build_error_messages(conn, repo, shas)
        manifest = {
            "builder_version": BUILDER_VERSION,
            "prism_root": str(active.root),
            "transitions_sha256": prism.file_sha256(active.transitions),
            "exact_symbols_sha256": prism.file_sha256(active.exact_symbols),
            "git_corpus_sha256": prism.file_sha256(active.git_corpus),
            "bulk_path_limit": str(BULK_PATH_LIMIT),
            "error_bases": str(len(base_shas) if base_shas is not None else len(_task_bases(data))),
        }
        rows = {
            table: int(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in ("commits", "symbol_history", "co_change", "path_commits", "parents", "error_messages")
        }
        history_store.write_manifest(conn, manifest)
        conn.commit()
        stored = history_store.read_manifest(conn)
    finally:
        conn.close()
    return BuildReport(db=str(target), rows=rows, manifest=stored)


@beartype
def _drop(conn: sqlite3.Connection) -> None:
    """Полная пересборка: пересоздаёт таблицы со свежей схемой.

    ``DROP`` обязателен: схема v2 добавила колонки символа, а ``CREATE TABLE IF
    NOT EXISTS`` их в существующую таблицу не добавит.
    """
    history_store.reset_schema(conn)


@beartype
def _build_parents(conn: sqlite3.Connection, archive: prism.PrismArchive) -> None:
    """Переносит цепочку родителей корпуса: она даёт полную глубину истории.

    Без ``parents`` v2 упирался бы в окно v1-индекса (250 коммитов), и
    ``symbol_history`` почти всегда был бы пуст.
    """
    source = prism.open_readonly(archive.git_corpus)
    try:
        rows = source.execute("SELECT oid, parent_oid, ordinal FROM parents").fetchall()
        conn.executemany(
            "INSERT OR REPLACE INTO parents(oid, parent_oid, ordinal) VALUES (?, ?, ?)",
            [(str(row[0]), str(row[1]), int(row[2])) for row in rows],
        )
        conn.commit()
    finally:
        source.close()


@beartype
def _task_bases(data: Path | None) -> list[str]:
    """Уникальные ``base_sha`` всех задач бенчмарка."""
    import json

    found: list[str] = []
    for path in sorted(paths.oracle_dir(data).glob("*.json")):
        raw: object = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            base = str(cast("dict[str, object]", raw).get("base", ""))
            if base != "" and base not in found:
                found.append(base)
    return found


@beartype
def _build_commits(conn: sqlite3.Connection, archive: prism.PrismArchive) -> None:
    """Переносит метаданные коммитов корпуса и наполняет FTS."""
    source = prism.open_readonly(archive.git_corpus)
    try:
        rows = source.execute(
            "SELECT oid, authored_at, committed_at, subject, message, "
            "coalesce(patch_id, '') AS patch_id FROM commits"
        ).fetchall()
        conn.executemany(
            "INSERT OR REPLACE INTO commits"
            "(oid, authored_at, committed_at, subject, message, patch_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]))
                for row in rows
            ],
        )
        conn.executemany(
            "INSERT INTO commits_fts(oid, subject, message) VALUES (?, ?, ?)",
            [(str(row[0]), str(row[3]), str(row[4])) for row in rows],
        )
        conn.commit()
    finally:
        source.close()


# Запрос пар «сторона ханка ↔ символ» с сортировкой по blob'у стороны.
_SYMBOL_QUERY = """
SELECT h.oid AS oid, h.path AS path, h.old_start AS old_start,
       h.old_end AS old_end, h.new_start AS new_start, h.new_end AS new_end,
       so.side AS side, so.name AS name, so.symbol_kind AS kind,
       so.role AS role, so.blob_oid AS blob_oid,
       so.start_byte AS sym_start, so.end_byte AS sym_end
FROM symbol_overlaps so
JOIN hunks h ON h.id = so.hunk_id
JOIN hunk_sides hs ON hs.hunk_id = so.hunk_id AND hs.side = so.side
ORDER BY so.blob_oid
"""


@beartype
def _build_symbol_history(
    conn: sqlite3.Connection,
    archive: prism.PrismArchive,
    repo: Path,
) -> None:
    """``hunks ⋈ symbol_overlaps`` с переводом байтов в номера строк."""
    source = prism.open_readonly(archive.transitions)
    reader = BlobReader(repo)
    try:
        cursor = source.execute(_SYMBOL_QUERY)
        batch: list[tuple[object, ...]] = []
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            blob = str(row["blob_oid"])
            content = reader.read(blob)
            side = str(row["side"])
            if side == "old":
                start, end = int(row["old_start"]), int(row["old_end"])
            else:
                start, end = int(row["new_start"]), int(row["new_end"])
            line, end_line = byte_span_to_lines(content, start, end)
            sym_start = int(row["sym_start"])
            sym_end = int(row["sym_end"])
            sym_line, sym_end_line = byte_span_to_lines(content, sym_start, sym_end)
            batch.append(
                (
                    str(row["oid"]),
                    str(row["path"]),
                    str(row["name"]),
                    str(row["kind"]),
                    str(row["role"]),
                    line,
                    end_line,
                    sym_line,
                    sym_end_line,
                    int(row["old_start"]),
                    int(row["old_end"]),
                    int(row["new_start"]),
                    int(row["new_end"]),
                    sym_start,
                    sym_end,
                    side,
                )
            )
            if len(batch) >= FLUSH_ROWS:
                _flush_symbol_history(conn, batch)
                batch = []
        if batch:
            _flush_symbol_history(conn, batch)
        conn.commit()
    finally:
        reader.close()
        source.close()


@beartype
def _flush_symbol_history(conn: sqlite3.Connection, batch: list[tuple[object, ...]]) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO symbol_history"
        "(oid, path, name, kind, role, line, end_line, sym_line, sym_end_line, "
        "old_start, old_end, new_start, new_end, sym_start, sym_end, side) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        batch,
    )


@beartype
def _build_co_change(conn: sqlite3.Connection, archive: prism.PrismArchive) -> None:
    """Строит ``co_change`` и ``path_commits`` одним проходом по ``paths``.

    ``path_commits`` нужен серверу для проверки membership: коммиты, тронувшие
    конкретный путь, индексируются рядом с кэшем и сверяются с индексом v1.
    Массовые коммиты (> ``BULK_PATH_LIMIT`` путей) в co-change не попадают:
    pgindent и переводы дают ложную связь «любой тест ↔ любой файл».
    """
    source = prism.open_readonly(archive.transitions)
    try:
        cursor = source.execute("SELECT oid, path, status FROM paths")
        by_commit: dict[str, list[tuple[str, str]]] = {}
        path_batch: list[tuple[str, str, str]] = []
        for row in cursor:
            oid = str(row["oid"])
            path = str(row["path"])
            status = str(row["status"])
            by_commit.setdefault(oid, []).append((path, status))
            path_batch.append((path, oid, status))
            if len(path_batch) >= FLUSH_ROWS:
                conn.executemany(
                    "INSERT OR REPLACE INTO path_commits(path, oid, status) VALUES (?, ?, ?)",
                    path_batch,
                )
                path_batch = []
        if path_batch:
            conn.executemany(
                "INSERT OR REPLACE INTO path_commits(path, oid, status) VALUES (?, ?, ?)",
                path_batch,
            )
        weights: dict[tuple[str, str], list[float]] = {}
        for touched in by_commit.values():
            if len(touched) > BULK_PATH_LIMIT:
                continue
            names = [path for path, _ in touched]
            tests = [path for path in names if is_test_path(path)]
            codes = [path for path in names if not is_test_path(path)]
            if not tests or not codes:
                continue
            weight = 1.0 / len(names)
            for test in tests:
                for code in codes:
                    weights.setdefault((test, code), []).append(weight)
        conn.executemany(
            "INSERT OR REPLACE INTO co_change(test_path, code_path, weight, commits) "
            "VALUES (?, ?, ?, ?)",
            [
                (test, code, round(sum(values), 6), len(values))
                for (test, code), values in weights.items()
            ],
        )
        conn.commit()
    finally:
        source.close()


@beartype
def _build_error_messages(
    conn: sqlite3.Connection,
    repo: Path,
    base_shas: list[str],
) -> None:
    """Индексирует шаблоны ``errmsg`` по ``.c`` файлам деревьев баз.

    Каждая запись привязана к ``base_sha``: инструмент отдаёт только записи
    текущей базы, поэтому дерево другой (возможно, более поздней) задачи не
    становится источником утечки.
    """
    reader = BlobReader(repo)
    seen: set[tuple[str, str]] = set()
    batch: list[tuple[str, str, str, int]] = []
    try:
        for base in base_shas:
            for blob_oid, path in _c_blobs(repo, base):
                key = (base, blob_oid)
                if key in seen:
                    continue
                seen.add(key)
                content = reader.read(blob_oid)
                if content == b"":
                    continue
                for index, line_bytes in enumerate(content.splitlines(), start=1):
                    text = line_bytes.decode("utf-8", "replace")
                    for match in _ERRMSG_RE.finditer(text):
                        message = normalize_message(match.group(1))
                        if message == "":
                            continue
                        batch.append((message, base, path, index))
                if len(batch) >= FLUSH_ROWS:
                    _flush_errors(conn, batch)
                    batch = []
        if batch:
            _flush_errors(conn, batch)
        conn.commit()
    finally:
        reader.close()


@beartype
def _flush_errors(conn: sqlite3.Connection, batch: list[tuple[str, str, str, int]]) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO error_messages(message, base_sha, path, line) "
        "VALUES (?, ?, ?, ?)",
        batch,
    )


@beartype
def _c_blobs(repo: Path, base: str) -> list[tuple[str, str]]:
    """``.c`` файлы дерева: ``(blob_oid, path)``."""
    result = subprocess.run(
        ["git", "-C", str(repo), "ls-tree", "-r", "--format=%(objectname) %(path)", base],
        capture_output=True,
        text=True,
        check=False,
    )
    found: list[tuple[str, str]] = []
    for line in result.stdout.splitlines():
        parts = line.split(" ", 1)
        if len(parts) != 2:
            continue
        blob_oid, path = parts[0], parts[1]
        if path.endswith(".c"):
            found.append((blob_oid, path))
    return found
