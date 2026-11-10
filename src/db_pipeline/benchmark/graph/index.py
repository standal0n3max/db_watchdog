"""Построение локального индекса графа.

Индексируется публичная git-история PostgreSQL до cutoff: commits/messages,
изменённые ревизии файлов, hunks и символы C-кода. Revert-коммиты сохраняются
как отдельные записи. ``head_sha`` и его потомки в индекс не попадают: обход
идёт от ``base_sha`` по первому родителю.

Чтение Git идёт batch-вызовами: ``log --stdin -z`` отдаёт метаданные коммитов,
``diff-tree --name-status`` — изменённые пути, ``cat-file --batch`` — bytes
исходников. Байты хранятся в нормализованной blob-таблице и дедуплицируются по
SHA-256, символы дедуплицируются по ``(path, sha256, name, line)``, а ревизии
пишутся только для изменившихся путей. Поэтому graph-container не получает mount
полного клона, а индекс не растёт линейно по размеру дерева. Повторный запуск
продолжается идемпотентно: уже проиндексированные коммиты пропускаются.

Union-индекс собирает дедуплицированное объединение последних предков каждого из
пяти ``base_sha``: это даёт локальный навигационный слой без многократной
обработки неизменившихся файлов.

Полный mirror и SQLite-индекс лежат в ``data/graph/`` и в git не попадают.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path

from beartype import beartype

from db_pipeline.benchmark import paths
from db_pipeline.benchmark.graph import store

# Объявление (прототип): имя перед ``(`` и завершающая ``;`` без ``{``.
_DECL_RE = re.compile(
    r"^[A-Za-z_][\w \t*]*\b([A-Za-z_]\w*)\s*\([^;{}]*\)\s*;",
    re.MULTILINE,
)

# Определение в стиле PostgreSQL: имя стоит в начале строки (возможно с табом),
# а тип — на предыдущей строке, тело открывается ``{`` на той же строке.
_DEF_RE = re.compile(
    r"^\s*(?!#)([A-Za-z_]\w*)\s*\([^;{}]*\)\s*\{",
    re.MULTILINE,
)

# Ключевые слова C и управляющие конструкции: строки вида ``if (...) {`` не
# должны попадать в символы.
_RESERVED = frozenset(
    {
        "if", "else", "for", "while", "do", "switch", "case", "default", "return",
        "sizeof", "typeof", "foreach", "foreach_current_index", "goto", "break",
        "continue", "typedef", "struct", "union", "enum", "static", "inline",
        "const", "volatile", "extern", "register", "auto", "signed", "unsigned",
        "void", "char", "short", "int", "long", "float", "double", "bool",
    }
)

# Тело typedef-типа: ``typedef struct Foo { ... } Foo;`` (в т.ч. анонимный тег).
_TYPEDEF_BLOCK_RE = re.compile(
    r"^typedef\s+(struct|union|enum)(?:\s+[A-Za-z_]\w*)?\s*\{.*?\}\s*([A-Za-z_]\w*)\s*;",
    re.MULTILINE | re.DOTALL,
)

# Алиас без тела: ``typedef struct Foo Bar;``.
_TYPEDEF_ALIAS_RE = re.compile(
    r"^typedef\s+(?:struct|union|enum)\s+[A-Za-z_]\w*\s+([A-Za-z_]\w*)\s*;",
    re.MULTILINE,
)

_STRUCT_RE = re.compile(r"^struct\s+([A-Za-z_]\w*)\s*(?:\{)?", re.MULTILINE)
_ENUM_RE = re.compile(r"^enum\s+([A-Za-z_]\w*)\s*(?:\{)?", re.MULTILINE)
_DEFINE_RE = re.compile(r"^#\s*define\s+([A-Za-z_]\w*)", re.MULTILINE)

_HUNK_HEADER_RE = re.compile(
    r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@",
)

BATCH_CHUNK = 200


@beartype
@dataclass(frozen=True)
class Symbol:
    """Найденный в исходнике символ с диапазоном строк и признаком определения."""

    name: str
    kind: str
    line: int
    end_line: int
    definition: bool


@beartype
@dataclass(frozen=True)
class TreeEntry:
    path: str
    mode: str
    kind: str
    blob_sha: str
    size: int


@beartype
@dataclass(frozen=True)
class ChangedPath:
    path: str
    status: str
    blob_sha: str


@beartype
@dataclass(frozen=True)
class CommitMeta:
    sha: str
    parents: list[str]
    author_time: int
    message: str


@beartype
def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout


@beartype
def git_input(repo: Path, args: list[str], data: bytes) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        input=data,
        capture_output=True,
        check=False,
    )
    return result.stdout


@beartype
def reachable_commits(repo: Path, base_sha: str, limit: int) -> list[str]:
    output = git(repo, "rev-list", f"--max-count={limit}", base_sha)
    return [line.strip() for line in output.splitlines() if line.strip()]


@beartype
def union_commits(repo: Path, base_shas: list[str], per_base: int) -> list[str]:
    """Объединение последних ``per_base`` предков каждого ``base_sha``."""
    seen: list[str] = []
    known: set[str] = set()
    for base in base_shas:
        for sha in reachable_commits(repo, base, per_base):
            if sha in known:
                continue
            known.add(sha)
            seen.append(sha)
    return seen


@beartype
def is_source(path: str, mode: str, kind: str) -> bool:
    if kind != "blob":
        return False
    if mode.startswith("120000"):
        return False
    return _is_source_path(path)


@beartype
def _is_source_path(path: str) -> bool:
    return path.startswith(("src/", "contrib/")) and path.endswith(
        (".c", ".h", ".sql", ".out")
    )


@beartype
def _has_symbols(path: str) -> bool:
    """Символы C-кода индексируются только для ``.c`` и ``.h``."""
    return path.endswith((".c", ".h"))


@beartype
def list_tree(repo: Path, sha: str) -> list[TreeEntry]:
    """Ревизии файлов коммита одним вызовом ``ls-tree --long -r``."""
    output = git(repo, "ls-tree", "--long", "-r", sha)
    entries: list[TreeEntry] = []
    for line in output.splitlines():
        left, separator, path = line.partition("\t")
        if separator == "":
            continue
        fields = left.split()
        if len(fields) < 4:
            continue
        mode, kind, blob, size_text = fields[0], fields[1], fields[2], fields[3]
        try:
            size = int(size_text)
        except ValueError:
            size = 0
        entries.append(TreeEntry(path=path, mode=mode, kind=kind, blob_sha=blob, size=size))
    return entries


@beartype
def changed_paths(repo: Path, meta: CommitMeta) -> list[ChangedPath]:
    """Изменённые относительно первого родителя source-пути (``diff-tree``).

    Первый коммит (без родителей) обрабатывается через ``ls-tree`` целиком.
    Хранится только дельта, поэтому индекс не растёт линейно по размеру дерева.
    """
    if not meta.parents:
        return [
            ChangedPath(path=entry.path, status="A", blob_sha=entry.blob_sha)
            for entry in list_tree(repo, meta.sha)
            if is_source(entry.path, entry.mode, entry.kind)
        ]
    output = git(
        repo,
        "diff-tree",
        "--no-commit-id",
        "--name-status",
        "-r",
        "-z",
        meta.parents[0],
        meta.sha,
    )
    return _parse_name_status(repo, meta.sha, output)


@beartype
def _parse_name_status(repo: Path, sha: str, output: str) -> list[ChangedPath]:
    tokens = [token for token in output.split("\x00") if token != ""]
    found: list[ChangedPath] = []
    index = 0
    while index < len(tokens):
        status = tokens[index]
        index += 1
        if status.startswith(("R", "C")):
            index += 1  # старый путь при rename/copy
        if index >= len(tokens):
            break
        path = tokens[index]
        index += 1
        if not _is_source_path(path):
            continue
        found.append(
            ChangedPath(path=path, status=status[:1], blob_sha=_blob_at(repo, sha, path))
        )
    return found


@beartype
def _blob_at(repo: Path, sha: str, path: str) -> str:
    return git(repo, "rev-parse", f"{sha}:{path}").strip()


@beartype
def read_blobs(repo: Path, blob_shas: list[str]) -> dict[str, bytes]:
    """Читает bytes объектов одним процессом ``cat-file --batch``."""
    if not blob_shas:
        return {}
    payload = ("\n".join(blob_shas) + "\n").encode("utf-8")
    raw = git_input(repo, ["cat-file", "--batch"], payload)
    found: dict[str, bytes] = {}
    position = 0
    while position < len(raw):
        newline = raw.find(b"\n", position)
        if newline == -1:
            break
        header = raw[position:newline].decode("utf-8", "replace").split()
        position = newline + 1
        if len(header) < 3 or header[1] == "missing":
            continue
        sha = header[0]
        try:
            size = int(header[2])
        except ValueError:
            continue
        content = raw[position : position + size]
        position += size + 1
        found[sha] = content
    return found


@beartype
def read_commits(repo: Path, shas: list[str]) -> dict[str, CommitMeta]:
    """Метаданные коммитов одним вызовом ``log --stdin -z``."""
    if not shas:
        return {}
    payload = ("\n".join(shas) + "\n").encode("utf-8")
    raw = git_input(
        repo,
        ["log", "--no-walk=unsorted", "--stdin", "-z", "--format=%H%x09%P%x09%ct%x09%B"],
        payload,
    )
    found: dict[str, CommitMeta] = {}
    for chunk in raw.split(b"\x00"):
        text = chunk.decode("utf-8", "replace")
        if text == "":
            continue
        parts = text.split("\t", 3)
        if len(parts) < 4:
            continue
        sha, parents, time_text, message = parts
        try:
            author_time = int(time_text)
        except ValueError:
            author_time = 0
        found[sha] = CommitMeta(
            sha=sha,
            parents=parents.split(),
            author_time=author_time,
            message=message.strip(),
        )
    return found


@beartype
def indexed_commits(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT sha FROM commits").fetchall()
    return {str(row["sha"]) for row in rows}


@beartype
def build_index(
    data: Path | None = None,
    limit: int = 2000,
    base_sha: str = "",
) -> dict[str, object]:
    repo = paths.pg_clone(data)
    if not (repo / ".git").exists():
        return {"ok": False, "error": f"нет клона {repo}"}
    head = base_sha or git(repo, "rev-parse", "HEAD").strip()
    if head == "":
        return {"ok": False, "error": "пустой base_sha"}
    commits = reachable_commits(repo, head, limit)
    db = paths.graph_dir(data) / "postgres.sqlite"
    conn = store.open_store(db)
    try:
        added = index_commits(repo, conn, commits)
        conn.commit()
    finally:
        conn.close()
    return {
        "ok": True,
        "db": str(db),
        "base_sha": head,
        "commits": len(commits),
        "added": added,
        "rows": store.count_rows(db),
    }


@beartype
def snapshot_tree(
    repo: Path,
    conn: sqlite3.Connection,
    base_sha: str,
    chunk: int = BATCH_CHUNK,
) -> int:
    """Индексирует полное source-дерево базы: revision, blob и символы.

    Дельта по 50 предкам пропускает файлы, которые в этих коммитах не менялись
    (например, ``relnode.c``), а агенту нужно читать именно их. Снимок базы
    закрывает пробел: ревизии всех source-файлов пишутся для ``base_sha``,
    bytes дедуплицируются по SHA-256.
    """
    entries = [entry for entry in list_tree(repo, base_sha) if is_source(entry.path, entry.mode, entry.kind)]
    added = 0
    for start in range(0, len(entries), chunk):
        batch = entries[start : start + chunk]
        contents = read_blobs(repo, [entry.blob_sha for entry in batch])
        for entry in batch:
            data = contents.get(entry.blob_sha, b"")
            digest = hashlib.sha256(data).hexdigest()
            store.add_revision(conn, base_sha, entry.path, digest)
            store.add_blob(conn, digest, data)
            if _has_symbols(entry.path):
                index_symbols(conn, entry.path, digest, base_sha, data.decode("utf-8", "replace"))
            added += 1
        conn.commit()
    return added


@beartype
def build_union_index(
    data: Path | None = None,
    base_shas: list[str] | None = None,
    per_base: int = 50,
) -> dict[str, object]:
    """Строит объединённый smoke-индекс по пяти ``base_sha``."""
    repo = paths.pg_clone(data)
    if not (repo / ".git").exists():
        return {"ok": False, "error": f"нет клона {repo}"}
    bases = list(base_shas or [])
    if len(bases) == 0:
        return {"ok": False, "error": "нужен хотя бы один base_sha"}
    commits = union_commits(repo, bases, per_base)
    db = paths.graph_dir(data) / "postgres.sqlite"
    conn = store.open_store(db)
    try:
        added = index_commits(repo, conn, commits)
        snapshots = 0
        for base in bases:
            snapshots += snapshot_tree(repo, conn, base)
        conn.commit()
    finally:
        conn.close()
    return {
        "ok": True,
        "db": str(db),
        "bases": bases,
        "per_base": per_base,
        "commits": len(commits),
        "added": added,
        "snapshot_revisions": snapshots,
        "rows": store.count_rows(db),
    }


@beartype
def index_commits(
    repo: Path,
    conn: sqlite3.Connection,
    commits: list[str],
    chunk: int = BATCH_CHUNK,
) -> int:
    """Индексирует коммиты пачками, пропуская уже добавленные (идемпотентно)."""
    known = indexed_commits(conn)
    todo = [sha for sha in commits if sha not in known]
    added = 0
    for start in range(0, len(todo), chunk):
        batch = todo[start : start + chunk]
        metas = read_commits(repo, batch)
        for sha in batch:
            meta = metas.get(sha)
            if meta is None:
                continue
            _index_commit(repo, conn, meta)
            added += 1
        conn.commit()
    return added


@beartype
def _index_commit(repo: Path, conn: sqlite3.Connection, meta: CommitMeta) -> None:
    store.add_commit(conn, meta.sha, meta.parents, meta.message, meta.author_time)
    changes = changed_paths(repo, meta)
    blob_shas = sorted({change.blob_sha for change in changes if change.blob_sha != ""})
    contents = read_blobs(repo, blob_shas)
    for change in changes:
        data = contents.get(change.blob_sha, b"")
        digest = hashlib.sha256(data).hexdigest()
        store.add_revision(conn, meta.sha, change.path, digest)
        store.add_blob(conn, digest, data)
        if _has_symbols(change.path):
            index_symbols(conn, change.path, digest, meta.sha, data.decode("utf-8", "replace"))
    _index_hunks(repo, conn, meta)


@beartype
def _line_at(source: str, offset: int) -> int:
    """Номер строки (1-based) для байтового смещения ``offset``."""
    return source.count("\n", 0, offset) + 1


@beartype
def _find_body_end(lines: list[str], open_index: int) -> int:
    """Ищет закрывающую ``}`` определения по балансу скобок.

    Начальная глубина 0: фигурная скобка открытия тела приходит на строке
    ``open_index``. Возвращает индекс строки с закрывающей ``}`` или последнюю
    строку, если баланс не сошёлся (битый исходник).
    """
    depth = 0
    index = open_index
    while index < len(lines):
        for char in lines[index]:
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return index
        index += 1
    return len(lines) - 1


@beartype
def _brace_block_end(lines: list[str], start_index: int) -> int:
    """Конец блока ``{ ... }``, первая ``{`` которого на строке ``start_index``."""
    depth = 0
    opened = False
    index = start_index
    while index < len(lines):
        for char in lines[index]:
            if char == "{":
                depth += 1
                opened = True
            elif char == "}" and opened:
                depth -= 1
                if depth == 0:
                    return index
        index += 1
    return len(lines) - 1


@beartype
def _match_end(lines: list[str], start_line: int) -> int:
    """Индекс строки, на которой заканчивается определение, начавшееся на ``start_line``.

    Прототип заканчивается ``;``; определение — сбалансированной ``}``.
    """
    open_index = start_line - 1
    if open_index < 0 or open_index >= len(lines):
        return open_index
    if "{" in lines[open_index]:
        return _find_body_end(lines, open_index)
    index = open_index
    while index < len(lines):
        if "{" in lines[index]:
            return _find_body_end(lines, index)
        if ";" in lines[index]:
            return index
        index += 1
    return open_index


@beartype
def index_symbols(
    conn: sqlite3.Connection,
    path: str,
    digest: str,
    revision: str,
    source: str,
) -> None:
    """Индексирует определения и объявления функций, типы, структуры и макросы.

    Определения предпочтительнее прототипов: если имя найдено и как прототип, и
    как определение, в индекс попадает только определение с телом.
    """
    lines = source.splitlines()
    found = scan_symbols(path, lines)
    for symbol in found:
        end_line = symbol.end_line if symbol.end_line >= symbol.line else symbol.line
        body = "\n".join(lines[symbol.line - 1 : end_line])
        store.add_symbol(
            conn,
            path,
            digest,
            symbol.name,
            symbol.kind,
            symbol.line,
            revision,
            end_line=end_line,
            body=body,
        )


@beartype
def scan_symbols(path: str, lines: list[str]) -> list[Symbol]:
    """Сканирует исходник и возвращает символы, схлопывая определение и прототип."""
    text = "\n".join(lines)
    found: list[Symbol] = []
    definitions: dict[str, Symbol] = {}
    for match in _DEF_RE.finditer(text):
        name = match.group(1)
        if name in _RESERVED:
            continue
        line = _line_at(text, match.start())
        end_line = _match_end(lines, line) + 1
        definitions[name] = Symbol(name, "function", line, end_line, True)
    for match in _DECL_RE.finditer(text):
        name = match.group(1)
        if name in _RESERVED or name in definitions:
            continue
        line = _line_at(text, match.start())
        found.append(Symbol(name, "function", line, line, False))
    found.extend(definitions.values())
    found.extend(_scan_types(text, lines))
    found.extend(_scan_macros(text, lines))
    return _dedupe_symbols(found)


@beartype
def _scan_types(text: str, lines: list[str]) -> list[Symbol]:
    found: list[Symbol] = []
    for match in _TYPEDEF_BLOCK_RE.finditer(text):
        keyword, name = match.group(1), match.group(2)
        start_line = _line_at(text, match.start())
        end_line = _line_at(text, match.end() - 1)
        found.append(Symbol(name, _kind_for(keyword), start_line, end_line, True))
    for match in _TYPEDEF_ALIAS_RE.finditer(text):
        name = match.group(1)
        line = _line_at(text, match.start())
        found.append(Symbol(name, "typedef", line, line, True))
    for match in _STRUCT_RE.finditer(text):
        name = match.group(1)
        line = _line_at(text, match.start())
        end_line = line
        if "{" in lines[line - 1]:
            end_line = _brace_block_end(lines, line - 1) + 1
        found.append(Symbol(name, "struct", line, end_line, True))
    for match in _ENUM_RE.finditer(text):
        name = match.group(1)
        line = _line_at(text, match.start())
        end_line = line
        if "{" in lines[line - 1]:
            end_line = _brace_block_end(lines, line - 1) + 1
        found.append(Symbol(name, "enum", line, end_line, True))
    return found


@beartype
def _kind_for(keyword: str) -> str:
    if keyword == "struct":
        return "struct"
    if keyword == "union":
        return "union"
    return "enum"


@beartype
def _scan_macros(text: str, lines: list[str]) -> list[Symbol]:
    found: list[Symbol] = []
    for match in _DEFINE_RE.finditer(text):
        name = match.group(1)
        start_line = _line_at(text, match.start())
        end_line = start_line
        index = start_line - 1
        while lines[index].rstrip().endswith("\\") and index + 1 < len(lines):
            index += 1
            end_line = index + 1
        found.append(Symbol(name, "macro", start_line, end_line, True))
    return found


@beartype
def _dedupe_symbols(found: list[Symbol]) -> list[Symbol]:
    """Убирает дубли по ``(name, line)``, предпочитая определения."""
    best: dict[tuple[str, int], Symbol] = {}
    for symbol in found:
        key = (symbol.name, symbol.line)
        current = best.get(key)
        if current is None or (symbol.definition and not current.definition):
            best[key] = symbol
    return sorted(best.values(), key=lambda item: (item.line, item.name))


@beartype
def _index_hunks(repo: Path, conn: sqlite3.Connection, meta: CommitMeta) -> None:
    if not meta.parents:
        return
    diff = git(
        repo,
        "diff",
        "--unified=0",
        meta.parents[0],
        meta.sha,
        "--",
        "src/",
        "contrib/",
    )
    current_path = ""
    old_start = old_count = new_start = new_count = 0
    added: list[str] = []
    removed: list[str] = []
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            _flush_hunk(
                conn, meta.sha, current_path, added, removed,
                old_start, old_count, new_start, new_count,
            )
            added, removed = [], []
            current_path = line[len("+++ b/") :]
            old_start = old_count = new_start = new_count = 0
        elif line.startswith("@@"):
            _flush_hunk(
                conn, meta.sha, current_path, added, removed,
                old_start, old_count, new_start, new_count,
            )
            added, removed = [], []
            old_start, old_count, new_start, new_count = parse_hunk_header(line)
        elif line.startswith("+") and not line.startswith("+++"):
            added.append(line[1:])
        elif line.startswith("-") and not line.startswith("---"):
            removed.append(line[1:])
    _flush_hunk(
        conn, meta.sha, current_path, added, removed,
        old_start, old_count, new_start, new_count,
    )


@beartype
def parse_hunk_header(line: str) -> tuple[int, int, int, int]:
    """Разбирает ``@@ -a,b +c,d @@``; пропущенные counts считаются единицей."""
    match = _HUNK_HEADER_RE.match(line)
    if match is None:
        return 0, 0, 0, 0
    old_start = int(match.group(1))
    old_count = int(match.group(2)) if match.group(2) is not None else 1
    new_start = int(match.group(3))
    new_count = int(match.group(4)) if match.group(4) is not None else 1
    return old_start, old_count, new_start, new_count


@beartype
def _flush_hunk(
    conn: sqlite3.Connection,
    sha: str,
    path: str,
    added: list[str],
    removed: list[str],
    old_start: int = 0,
    old_count: int = 0,
    new_start: int = 0,
    new_count: int = 0,
) -> None:
    if path == "" or (not added and not removed):
        return
    store.add_hunk(
        conn,
        sha,
        path,
        "\n".join(added),
        "\n".join(removed),
        old_start,
        old_count,
        new_start,
        new_count,
    )
