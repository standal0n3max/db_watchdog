"""Протокол commit-suite: очистка statement, area, contract, промпт.

Публичная задача строится детерминированно из commit-сообщения watchdog.
Из statement вырезаются trailers (``Reported-by:``, ``Author:``, ``Discussion:``,
``Backpatch-through:`` и подобные) и служебные ссылки. ``area`` выводится из
subject и изменённых source-путей. ``contract`` извлекается только для новых
пользовательских сообщений об ошибках, которые проверяет скрытый тест.

Промпт повторяет форму commit-suite: правила, ``Area``, ``goal``, при наличии
``contract`` и ``graded_by``.
"""

from __future__ import annotations

import re

from beartype import beartype

from db_pipeline.benchmark.models import as_str_list

SYSTEM_RULES = """\
You are working in a real PostgreSQL 17 source tree, at an upstream commit with \
one bug still in it. Fix the bug in the C code.

The tree builds and tests itself with its own machinery: `make` at the root, \
`make -C src/test/regress check` for the regression suite. Read the code around \
the area you are changing and follow its conventions.

You are graded by the upstream regression test for this bug. That test is off \
disk while you work and is put back afterwards, so you cannot read it, and \
writing anything under src/test/ is detected and scores zero. Everything the \
test checks is in the task below; make the code do that and the test passes.

The rest of the regression suite runs too. A fix that breaks something else is \
not a fix.
"""

TRAILER_RE = re.compile(
    r"^(?:Reported-by|Author|Reviewed-by|Co-authored-by|Discussion|Backpatch-through"
    r"|Bug|Fixes|Closes|Signed-off-by|Tested-by|Acked-by|Committer|Release-note)\s*:",
    re.IGNORECASE,
)

MESSAGE_CALLS = (
    "errmsg(",
    "errmsg_plural(",
    "errhint(",
    "errdetail(",
)

TEST_SUFFIXES = (".sql", ".out", ".spec")
TEST_DIRS = ("/sql/", "/expected/", "/specs/")

_AREA_BY_PATH = (
    ("src/backend/optimizer/", "query planner"),
    ("src/backend/parser/", "parser"),
    ("src/backend/executor/", "executor"),
    ("src/backend/utils/adt/", "data types"),
    ("src/backend/utils/mb/", "multibyte encoding"),
    ("src/backend/utils/", "backend utilities"),
    ("src/backend/catalog/", "system catalog"),
    ("src/backend/commands/", "utility commands"),
    ("src/backend/replication/", "replication"),
    ("src/backend/access/", "access methods"),
    ("src/backend/storage/", "storage"),
    ("src/backend/libpq/", "client protocol"),
    ("src/backend/statistics/", "extended statistics"),
    ("src/backend/", "backend"),
    ("src/bin/", "client programs"),
    ("src/pl/", "procedural languages"),
    ("src/interfaces/", "client interfaces"),
    ("contrib/", "contrib module"),
)


@beartype
def clean_statement(statement: str) -> str:
    lines: list[str] = []
    for line in statement.splitlines():
        if TRAILER_RE.match(line.strip()):
            continue
        lines.append(line.rstrip())
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


@beartype
def statement_subject(statement: str) -> str:
    for line in clean_statement(statement).splitlines():
        stripped = line.strip()
        if stripped != "":
            return stripped
    return ""


@beartype
def derive_area(subject: str, paths: list[str]) -> str:
    for prefix, label in _AREA_BY_PATH:
        if any(path.startswith(prefix) for path in paths):
            return f"{label}, {subject}" if subject != "" else label
    if subject != "":
        return subject
    return "postgres core"


@beartype
def _texts(lines: list[str]) -> set[str]:
    found: set[str] = set()
    for line in lines:
        if any(call in line for call in MESSAGE_CALLS):
            found.update(re.findall(r'"((?:[^"\\]|\\.)*)"', line))
    return found


@beartype
def _added(text: str) -> list[str]:
    return [
        line[1:]
        for line in text.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    ]


@beartype
def _removed(text: str) -> list[str]:
    return [
        line[1:]
        for line in text.splitlines()
        if line.startswith("-") and not line.startswith("---")
    ]


@beartype
def _literal_parts(message: str) -> list[str]:
    parts = re.split(r"%[#0\- +']*[\d.*]*(?:hh|h|ll|l|L|z|j|t)?[a-zA-Z]", message)
    return sorted((p.strip(' "\\.,;:') for p in parts), key=len, reverse=True)


@beartype
def introduced_messages(reference: str) -> list[str]:
    new = sorted(_texts(_added(reference)) - _texts(_removed(reference)))
    return [_unescape(message) for message in new]


@beartype
def _unescape(message: str) -> str:
    return message.replace('\\"', '"').replace("\\\\", "\\")


@beartype
def extract_contract(reference: str, test_patch: str) -> list[str]:
    contract: list[str] = []
    for message in introduced_messages(reference):
        anchor = next(
            (part for part in _literal_parts(message) if len(part) >= 8),
            None,
        )
        if anchor is None:
            continue
        if anchor in test_patch and message not in contract:
            contract.append(message)
    return contract


@beartype
def is_test_source(rel: str) -> bool:
    return rel.endswith(TEST_SUFFIXES) and any(part in rel for part in TEST_DIRS)


@beartype
def graded_files(test_files: list[str]) -> list[str]:
    return [rel for rel in test_files if is_test_source(rel)]


@beartype
def build_prompt(task: dict[str, object]) -> str:
    lines = [SYSTEM_RULES, "", f"Area: {task.get('area', '')}", "", str(task.get("goal", ""))]
    contract = as_str_list(task.get("contract"))
    if len(contract) > 0:
        lines += [
            "",
            (
                "The test compares output as text. Where your fix produces "
                "these, use them exactly as written:"
            ),
        ]
        lines += [f"  {message}" for message in contract]
    graded = as_str_list(task.get("graded_by"))
    if len(graded) > 0:
        lines += [
            "",
            "The hidden test that grades this is " + ", ".join(graded) + ".",
        ]
    return "\n".join(lines)


@beartype
def specification(task: dict[str, object]) -> str:
    return f"Area: {task.get('area', '')}\n\n{task.get('goal', '')}"
