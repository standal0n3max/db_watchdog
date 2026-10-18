from __future__ import annotations

from beartype import beartype

from db_pipeline.filter_rules import has_prefix


@beartype
def split_unified_diff(
    diff_text: str,
    code_prefixes: list[str],
    test_prefixes: list[str],
    test_suffixes: list[str] | None = None,
    code_exclude: list[str] | None = None,
) -> tuple[str, str]:
    suffixes = [] if test_suffixes is None else test_suffixes
    excluded = [] if code_exclude is None else code_exclude
    test_chunks: list[str] = []
    gold_chunks: list[str] = []
    for chunk in _chunks(diff_text):
        path = path_from_header(chunk.splitlines()[0] if chunk.splitlines() else "")
        if _excluded_name(path, excluded):
            continue
        if _matches_test(path, test_prefixes, suffixes):
            test_chunks.append(chunk)
        elif has_prefix(path, code_prefixes):
            gold_chunks.append(chunk)
    return "".join(test_chunks), "".join(gold_chunks)


@beartype
def split_by_paths(
    diff_text: str,
    gold_paths: list[str],
    test_paths: list[str],
) -> tuple[str, str]:
    gold = set(gold_paths)
    tests = set(test_paths)
    test_chunks: list[str] = []
    gold_chunks: list[str] = []
    for chunk in _chunks(diff_text):
        path = path_from_header(chunk.splitlines()[0] if chunk.splitlines() else "")
        if path in tests:
            test_chunks.append(chunk)
        elif path in gold:
            gold_chunks.append(chunk)
    return "".join(test_chunks), "".join(gold_chunks)


@beartype
def _matches_test(path: str, test_prefixes: list[str], test_suffixes: list[str]) -> bool:
    if not has_prefix(path, test_prefixes):
        return False
    if len(test_suffixes) == 0:
        return True
    return any(path.endswith(suffix) for suffix in test_suffixes)


@beartype
def _excluded_name(path: str, excluded: list[str]) -> bool:
    name = path.rsplit("/", 1)[-1]
    return name in excluded


REGRESS_PREFIX = "src/test/regress/"


@beartype
def split_regress_diff(diff_text: str) -> tuple[str, str]:
    # Код и регресс PostgreSQL оба лежат под src/. Граница harvest — каталог регресса,
    # а не списки образца: всё вне src/test/regress/ остаётся в скрытом эталоне.
    test_chunks: list[str] = []
    gold_chunks: list[str] = []
    for chunk in _chunks(diff_text):
        path = path_from_header(chunk.splitlines()[0] if chunk.splitlines() else "")
        if path.startswith(REGRESS_PREFIX):
            test_chunks.append(chunk)
        elif path != "":
            gold_chunks.append(chunk)
    return "".join(test_chunks), "".join(gold_chunks)


@beartype
def regress_test_names(test_patch: str) -> list[str]:
    names: list[str] = []
    for path in paths_in_diff(test_patch):
        if not path.startswith(f"{REGRESS_PREFIX}expected/") or not path.endswith(".out"):
            continue
        base = path.rsplit("/", 1)[-1]
        names.append(base[: -len(".out")])
    return names


@beartype
def paths_in_diff(diff_text: str) -> list[str]:
    paths: list[str] = []
    for chunk in _chunks(diff_text):
        header = chunk.splitlines()[0] if chunk.splitlines() else ""
        path = path_from_header(header)
        if path != "":
            paths.append(path)
    return paths


@beartype
def path_from_header(header: str) -> str:
    marker = "diff --git a/"
    if not header.startswith(marker):
        return ""
    rest = header[len(marker) :]
    path, separator, _ = rest.partition(" b/")
    if separator == "":
        return rest
    return path


@beartype
def _chunks(diff_text: str) -> list[str]:
    if diff_text.strip() == "":
        return []
    if not diff_text.endswith("\n"):
        diff_text = diff_text + "\n"
    chunks: list[str] = []
    current: list[str] = []
    for line in diff_text.splitlines(keepends=True):
        if line.startswith("diff --git ") and len(current) > 0:
            chunks.append("".join(current))
            current = [line]
        else:
            current.append(line)
    if len(current) > 0:
        chunks.append("".join(current))
    return chunks
