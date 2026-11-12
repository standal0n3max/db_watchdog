"""Сканер символов C и разбор хунков.

Сканер перенесён из прежнего индекса без изменения правил: определения
предпочтительнее прототипов, типы и макросы остаются. Его вызывает сборка
``graph.sqlite`` и разовая сверка ``verify_a``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from beartype import beartype


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


_FORMAT_SPEC_RE = re.compile(r"%[#0\- +']*[\d.*]*(?:hh|h|ll|l|L|z|j|t)?[a-zA-Z]")
_ERRMSG_RE = re.compile(
    r"\b(?:errmsg|errmsg_plural|errhint|errdetail|errdetail_log|errcontext)\s*\(\s*"
    r'"((?:[^"\\]|\\.)*)"',
)
_TEST_MARKERS = ("/expected/", "/sql/", "/specs/", "/test/")
_TEST_SUFFIXES = (".out", ".sql", ".spec")
_SOURCE_SUFFIXES = (".c", ".h", ".sql", ".out")


@beartype
def is_source(path: str, mode: str, kind: str) -> bool:
    """Исходник ядра: blob под ``src/`` или ``contrib/`` нужного суффикса."""
    if kind != "blob" or mode.startswith("120000"):
        return False
    return is_source_path(path)


@beartype
def is_source_path(path: str) -> bool:
    """Путь дерева базы, который попадает в ``tree_entries``."""
    return path.startswith(("src/", "contrib/")) and path.endswith(_SOURCE_SUFFIXES)


@beartype
def has_symbols(path: str) -> bool:
    """Сканер C-символов применяется только к ``.c`` и ``.h``."""
    return path.endswith((".c", ".h"))


@beartype
def is_test_path(path: str) -> bool:
    """Путь считается тестовым по каталогу или суффиксу."""
    if any(marker in path for marker in _TEST_MARKERS):
        return True
    return path.endswith(_TEST_SUFFIXES)


@beartype
def normalize_message(message: str) -> str:
    """Нормализует сообщение: формат-спецификаторы становятся ``%``."""
    without_specs = _FORMAT_SPEC_RE.sub("%", message)
    unescaped = without_specs.replace('\\"', '"').replace("\\\\", "\\")
    return re.sub(r"\s+", " ", unescaped).strip()


@beartype
def error_messages(text: str) -> list[tuple[str, int]]:
    """Пары ``(сообщение, номер строки)`` из вызовов ``errmsg`` и соседних."""
    found: list[tuple[str, int]] = []
    for index, line in enumerate(text.splitlines(), start=1):
        for match in _ERRMSG_RE.finditer(line):
            message = normalize_message(match.group(1))
            if message != "":
                found.append((message, index))
    return found
