"""Решение, годится ли изменение в датасет.

``filter_candidate(change, core, known_shas, solution_paths=None, test_paths=None, sql_only=False)``. Три ветки не смешиваются. Если переданы ``solution_paths`` или ``test_paths``, решение идёт по спискам образца PostgreSQL. Иначе при ``sql_only`` код и тест берутся из ``sql_code_prefixes`` и ``sql_test_prefixes``; у PostgreSQL ещё обязателен изменённый ``expected/*.out``, иначе код ``no_oracle_test``. Иначе граница — ``code_prefixes`` и ``test_prefixes``.

До выбора ветки отсекаются: sha из ``known_shas``, merge (тема начинается с ``Merge``; коммит с двумя родителями сам по себе мусором не считается), revert, правка только амальгамы ``sqlite3.c`` или ``sqlite3.h``, массовая генерация, форматирование или переименование без тестов, только документация.

Причины повторного разбора harvest вынесены в константы: ``нет правки SQL-кода`` для всех ядер и ``нет правки SQL-тестов`` для DuckDB после расширения тестовых префиксов.
"""

from __future__ import annotations

import re

from beartype import beartype

from db_pipeline.config import CoreConfig
from db_pipeline.models import ChangeView, FileChange, FilterDecision

_FORMAT_SUBJECT = re.compile(
    r"(?i)(\bclang-format\b|\bformatting\b|\bformat the\b|\breformat\b|\bwhitespace\b|\brename\b)"
)
_GENERATED_MIN_LINES = 500
_GENERATED_MIN_SHARE = 0.5
_DOC_LINE_CAP = 6
MISSING_SQL_CODE_REASON = "нет правки SQL-кода"
MISSING_SQL_TEST_REASON = "нет правки SQL-тестов"


@beartype
def filter_candidate(
    change: ChangeView,
    core: CoreConfig,
    known_shas: set[str],
    solution_paths: list[str] | None = None,
    test_paths: list[str] | None = None,
    sql_only: bool = False,
) -> FilterDecision:
    if change.sha in known_shas:
        return FilterDecision(False, "already_in_journal", "sha уже есть в журнале")
    if solution_paths is not None or test_paths is not None:
        return _filter_sample_lists(change, solution_paths or [], test_paths or [])
    if _is_merge(change):
        return FilterDecision(False, "merge_commit", "merge-коммит")
    if _is_revert(change):
        return FilterDecision(False, "revert", "откат изменения")
    if _amalgam_without_code(change, core):
        return FilterDecision(
            False,
            "generated_files",
            "правка только амальгамы sqlite3.c или sqlite3.h",
        )
    if _is_generated_mass(change, core):
        return FilterDecision(
            False,
            "generated_files",
            "массовое обновление сгенерированных файлов",
        )
    if _is_formatting(change, core):
        return FilterDecision(False, "formatting", "форматирование или переименование без тестов")
    if _is_documentation(change):
        return FilterDecision(False, "documentation", "только документация")
    if sql_only:
        return _filter_sql(change, core)
    missing = _missing_code_or_tests(change, core)
    if missing is not None:
        return missing
    return FilterDecision(True, "accepted", "есть правка кода ядра и правка тестов")


@beartype
def _filter_sql(change: ChangeView, core: CoreConfig) -> FilterDecision:
    # Префиксы ядра из sql_code_prefixes. Исключение амальгамы остаётся в _is_code_path.
    has_code = any(_is_harvest_code(item.path, core) for item in change.files)
    has_test = any(_is_sql_test(item.path, core) for item in change.files)
    if not has_code or not has_test:
        return _missing_sql(has_code, has_test)
    if core.id == "postgres" and not any(_is_regress_expected(item.path) for item in change.files):
        return FilterDecision(False, "no_oracle_test", "нет изменённого expected/*.out")
    return FilterDecision(True, "accepted", "есть правка SQL-кода и правка SQL-тестов")


@beartype
def _missing_sql(has_code: bool, has_test: bool) -> FilterDecision:
    if not has_code and not has_test:
        reason = "нет правки SQL-кода и нет правки SQL-тестов"
    elif not has_code:
        reason = MISSING_SQL_CODE_REASON
    else:
        reason = MISSING_SQL_TEST_REASON
    return FilterDecision(False, "missing_code_and_tests", reason)


@beartype
def _matches_sql_prefix(path: str, prefixes: list[str]) -> bool:
    for prefix in prefixes:
        if prefix.endswith("/"):
            if path.startswith(prefix):
                return True
        elif path == prefix:
            return True
    return False


@beartype
def _is_sql_test(path: str, core: CoreConfig) -> bool:
    if not _matches_sql_prefix(path, core.sql_test_prefixes):
        return False
    if len(core.sql_test_suffixes) == 0:
        return True
    return any(path.endswith(suffix) for suffix in core.sql_test_suffixes)


@beartype
def _is_regress_expected(path: str) -> bool:
    return path.startswith("src/test/regress/expected/") and path.endswith(".out")


@beartype
def has_prefix(path: str, prefixes: list[str]) -> bool:
    return any(path.startswith(prefix) for prefix in prefixes)


@beartype
def _filter_sample_lists(
    change: ChangeView,
    solution_paths: list[str],
    test_paths: list[str],
) -> FilterDecision:
    solution = set(solution_paths)
    tests = set(test_paths)
    paths = [item.path for item in change.files]
    if len(solution_paths) == 0 or len(test_paths) == 0:
        return _missing_sides(len(solution_paths) > 0, len(test_paths) > 0)
    outside = [path for path in paths if path not in solution and path not in tests]
    if len(outside) > 0:
        return FilterDecision(
            False,
            "paths_outside_sample",
            "diff содержит пути вне solution и test_files",
        )
    has_code = any(path in solution for path in paths)
    has_test = any(path in tests for path in paths)
    if has_code and has_test:
        return FilterDecision(True, "accepted", "есть solution и test_files, diff внутри списков")
    return _missing_sides(has_code, has_test)


@beartype
def _missing_sides(has_code: bool, has_test: bool) -> FilterDecision:
    if not has_code and not has_test:
        reason = "нет правки кода ядра и нет правки тестов"
    elif not has_code:
        reason = "нет правки кода ядра"
    else:
        reason = "нет правки тестов"
    return FilterDecision(False, "missing_code_and_tests", reason)


@beartype
def _is_merge(change: ChangeView) -> bool:
    # У DuckDB слитый PR на main — это merge-коммит с обычным заголовком PR.
    # Мусорный merge узнаём по тексту, а не по числу родителей.
    subject = change.subject.lstrip()
    return subject.startswith("Merge ") or subject.startswith("Merge pull request")


@beartype
def _is_revert(change: ChangeView) -> bool:
    subject = change.subject.lstrip()
    if subject.startswith("Revert ") or subject.startswith('Revert"'):
        return True
    return change.body.lstrip().startswith("This reverts commit")


@beartype
def _amalgam_without_code(change: ChangeView, core: CoreConfig) -> bool:
    if len(core.generated_names) == 0:
        return False
    has_generated = any(_is_generated_name(item.path, core) for item in change.files)
    if not has_generated:
        return False
    return not _has_code(change.files, core)


@beartype
def _is_generated_mass(change: ChangeView, core: CoreConfig) -> bool:
    total = 0
    generated = 0
    for item in change.files:
        weight = item.additions + item.deletions
        total += weight
        if has_prefix(item.path, core.generated_prefixes):
            generated += weight
    if total == 0 or generated < _GENERATED_MIN_LINES:
        return False
    return (generated / total) >= _GENERATED_MIN_SHARE


@beartype
def _is_formatting(change: ChangeView, core: CoreConfig) -> bool:
    if _FORMAT_SUBJECT.search(change.subject) is None:
        return False
    return not _has_test(change.files, core)


@beartype
def _is_documentation(change: ChangeView) -> bool:
    if len(change.files) == 0:
        return False
    if all(_is_doc_path(item.path) for item in change.files):
        return True
    subject = change.subject.lower()
    mentions_docs = (
        "documentation" in subject or subject.startswith("docs:") or subject.startswith("docs ")
    )
    if not mentions_docs:
        return False
    code_or_test = [item for item in change.files if not _is_doc_path(item.path)]
    if len(code_or_test) == 0:
        return True
    return all(
        item.additions == item.deletions and item.additions <= _DOC_LINE_CAP for item in code_or_test
    )


@beartype
def _missing_code_or_tests(change: ChangeView, core: CoreConfig) -> FilterDecision | None:
    has_code = _has_code(change.files, core)
    has_test = _has_test(change.files, core)
    if has_code and has_test:
        return None
    return _missing_sides(has_code, has_test)


@beartype
def _has_code(files: list[FileChange], core: CoreConfig) -> bool:
    return any(_is_code_path(item.path, core) for item in files)


@beartype
def _has_test(files: list[FileChange], core: CoreConfig) -> bool:
    return any(_is_test_path(item.path, core) for item in files)


@beartype
def _is_harvest_code(path: str, core: CoreConfig) -> bool:
    if not _matches_sql_prefix(path, core.sql_code_prefixes):
        return False
    return _is_code_path(path, core)


@beartype
def _is_code_path(path: str, core: CoreConfig) -> bool:
    if _basename(path) in core.code_exclude or _basename(path) in core.generated_names:
        return False
    return has_prefix(path, core.code_prefixes)


@beartype
def _is_test_path(path: str, core: CoreConfig) -> bool:
    if not has_prefix(path, core.test_prefixes):
        return False
    if len(core.test_suffixes) == 0:
        return True
    return any(path.endswith(suffix) for suffix in core.test_suffixes)


@beartype
def _is_generated_name(path: str, core: CoreConfig) -> bool:
    return _basename(path) in core.generated_names


@beartype
def _basename(path: str) -> str:
    return path.rsplit("/", 1)[-1]


@beartype
def _is_doc_path(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    return path.startswith("docs/") or name.endswith(".md") or name.endswith(".rst")
