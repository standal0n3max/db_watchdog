"""Публичная выгрузка задач: стабильные id, лицензии, sha256-манифест.

Релиз закрывает главную дыру трека Datasets and Benchmarks — доступность
датасета. Наружу кладутся только те поля, которые уже прошли проверку
``validate_public``: ``area``, ``goal``, ``contract``, ``graded_by``. Эталонный
патч, тестовый патч и ``reference`` в публичный слой не попадают никогда.

Каждая задача получает стабильный ``id`` вида ``postgres__<head[:12]>``: он
выведен из содержимого коммита-эталона, а не из номера строки, поэтому не
меняется при пересборке. Полный ``head`` (40 hex) лежит рядом, поэтому id можно
однозначно разрешить. Совпадение коротких префиксов — ошибка, а не молчаливая
перезапись.

Дерево релиза:

- ``tasks/<id>.json`` — публичная задача: идентификация, метаданные и четыре
  публичных поля;
- ``oracle/<id>.json`` — приватный слой (только с ``--with-oracle``): reverse
  патч, тест, эталон, ожидаемые тесты и команда оракула;
- ``split.json`` — разбиение dev/tune/holdout и ``sha256`` замороженного holdout;
- ``licenses.md`` — лицензии источников из ``repos.toml``;
- ``README.md`` — инструкция: как собрать, как проверить одной командой;
- ``manifest.json`` — счётчики, лицензия, ``sha256`` каждого файла и сводный
  ``content_sha256``.

Манифест байт-детерминирован: ни времени сборки, ни путей хоста в нём нет,
поэтому два ребилда подряд дают одинаковый ``content_sha256``. Проверка
распакованного релиза — ``release-verify``: она сверяет каждый ``sha256``,
сводный хэш, состав файлов и ключевые инварианты (нет diff в публичном слое).
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.benchmark import convert, paths, security
from db_pipeline.benchmark.graph import split as split_module
from db_pipeline.config import load_cores
from db_pipeline.errors import PipelineError
from db_pipeline.parsing import as_dict, load_json_file

FORMAT = "db-pipeline-release/1"
DEFAULT_VERSION = "0.1.0"
CORE = paths.CORE
UNASSIGNED = "unassigned"
PUBLIC_FIELDS = ("area", "goal", "contract", "graded_by")
SECTIONS = ("dev", "tune", "holdout")


@beartype
@dataclass(frozen=True)
class ReleaseTask:
    """Одна задача релиза: идентификация, метаданные и публичные поля."""

    id: str
    core: str
    source: str
    license: str
    head: str
    base: str
    oracle_name: str
    split: str
    area: str
    goal: str
    contract: list[str]
    graded_by: list[str]

    def file_name(self) -> str:
        return f"{self.id}.json"

    def public_dict(self) -> dict[str, object]:
        # Только то, что видит агент в промпте, плюс идентификация задачи.
        return {
            "id": self.id,
            "core": self.core,
            "source": self.source,
            "license": self.license,
            "head": self.head,
            "base": self.base,
            "oracle": self.oracle_name,
            "split": self.split,
            "area": self.area,
            "goal": self.goal,
            "contract": list(self.contract),
            "graded_by": list(self.graded_by),
        }


@beartype
def file_sha256(path: Path, chunk: int = 1 << 20) -> str:
    """SHA-256 файла одной линейной проходкой."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk)
            if block == b"":
                break
            digest.update(block)
    return digest.hexdigest()


@beartype
def _json_text(payload: dict[str, object]) -> str:
    """Канонический JSON: сортировка ключей, фиксированный отступ, перевод строки."""
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


@beartype
def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload), encoding="utf-8")


@beartype
def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@beartype
def _cards_by_head(data: Path | None) -> dict[str, dict[str, object]]:
    """Карточки источника по полному ``head_sha``: оттуда берутся лицензия и оракул."""
    folder = paths.resolve_data(data) / "tasks"
    found: dict[str, dict[str, object]] = {}
    if not folder.exists():
        return found
    for path in sorted(folder.glob(f"{CORE}__*.json")):
        if "reject" in path.stem:
            continue
        raw = as_dict(load_json_file(path), "card")
        head = str(raw.get("head_sha", ""))
        if head == "":
            continue
        found.setdefault(head, raw)
    return found


@beartype
def _split_map(
    data: Path | None,
    split_map: dict[str, str] | None,
) -> dict[str, str]:
    if split_map is not None:
        return dict(split_map)
    return split_module.flatten(split_module.derive_split(data))


@beartype
def collect(
    data: Path | None = None,
    split_map: dict[str, str] | None = None,
) -> list[ReleaseTask]:
    """Собирает публичные задачи датасета, не читая патчи для записи.

    ``split_map`` позволяет подставить разбиение в тестах и не зависеть от
    наличия архива PRISM и локального клона ядра.
    """
    names = sorted(path.stem for path in paths.tasks_dir(data).glob("*.json"))
    if len(names) == 0:
        raise PipelineError("release", "в датасете нет публичных задач")
    mapping = _split_map(data, split_map)
    cards = _cards_by_head(data)
    records: list[ReleaseTask] = []
    seen: set[str] = set()
    for name in names:
        public = convert.load_public(name, data)
        oracle = convert.load_oracle(name, data)
        func = str(public.get("func", name))
        if func != name:
            raise PipelineError("release", f"{name}: func не совпадает с именем файла")
        if func in seen:
            raise PipelineError("release", f"{func}: повтор id в датасете")
        seen.add(func)
        head = str(oracle.get("head", ""))
        base = str(oracle.get("base", ""))
        if len(head) != 40:
            raise PipelineError("release", f"{name}: неполный head sha")
        card = cards.get(head)
        if card is None:
            raise PipelineError("release", f"{name}: нет карточки источника для head {head[:12]}")
        check = security.validate_public(name, data)
        if not check.ok:
            raise PipelineError("release", f"{name}: публичный слой не прошёл проверку: {check.detail}")
        records.append(
            ReleaseTask(
                id=func,
                core=CORE,
                source=str(card.get("repository", "")),
                license=str(card.get("license", "")),
                head=head,
                base=base,
                oracle_name=str(card.get("oracle", "")),
                split=mapping.get(func, UNASSIGNED),
                area=str(public.get("area", "")),
                goal=str(public.get("goal", "")),
                contract=_str_list(public.get("contract")),
                graded_by=_str_list(public.get("graded_by")),
            )
        )
    return records


@beartype
def _str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    items = cast("list[object]", value)
    return [str(item) for item in items]


@beartype
def _sections(records: list[ReleaseTask]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {section: [] for section in SECTIONS}
    other: list[str] = []
    for record in records:
        if record.split in grouped:
            grouped[record.split].append(record.id)
        else:
            other.append(record.id)
    if len(other) > 0:
        grouped[UNASSIGNED] = other
    for names in grouped.values():
        names.sort()
    return grouped


@beartype
def _licenses_markdown(records: list[ReleaseTask]) -> str:
    lines = [
        "# Лицензии источников",
        "",
        "Датасет собран из публичной истории открытых ядер СУБД. Лицензии взяты из",
        "`repos.toml` и остаются за исходными репозиториями; здесь они приведены",
        "для того, чтобы состав набора был однозначен.",
        "",
        "| Ядро | Репозиторий | Лицензия | Задач в релизе |",
        "|---|---|---|---|",
    ]
    counts: dict[str, int] = {}
    for record in records:
        counts[record.core] = counts.get(record.core, 0) + 1
    cores = load_cores()
    for core in cores:
        exported = counts.get(core.id, 0)
        lines.append(f"| {core.id} | {core.repository} | {core.license} | {exported} |")
    lines += [
        "",
        "Люди в данных не участвуют: источник — сообщения коммитов и diff публичных",
        "репозиториев. Эталонный патч в публичный слой релиза не входит, поэтому",
        "публичные файлы не содержат ответа задачи.",
        "",
    ]
    return "\n".join(lines)


@beartype
def _readme(version: str, name: str, tasks: int, with_oracle: bool) -> str:
    oracle_note = (
        "\n- `oracle/<id>.json` — приватный слой этого релиза: reverse патч, тест, "
        "эталон и ожидаемые тесты. Публиковать его вместе с `tasks/` нельзя: там "
        "лежит ответ задачи.\n"
        if with_oracle
        else "\n- Приватного слоя в этом релизе нет: только публичные задачи.\n"
    )
    return f"""# {name} {version}

Публичная выгрузка задач агрегатора benchmark-задач ядра PostgreSQL. Набор
получен оракулом `pg_regress` из публичной истории `postgres/postgres`.

## Состав

- `tasks/<id>.json` — публичная задача: `id`, `core`, `source`, `license`,
  `head`, `base`, `oracle`, `split`, `area`, `goal`, `contract`, `graded_by`.
  Ровно те поля, что видит агент в промпте, плюс идентификация.
{oracle_note}- `split.json` — разбиение `dev`/`tune`/`holdout` и `holdout_sha256`.
- `licenses.md` — лицензии источников.
- `manifest.json` — счётчики, лицензия и `sha256` каждого файла.

Всего задач: **{tasks}**.

## Проверка одной командой

```bash
uv run python -m db_pipeline.benchmark release-verify --target <каталог релиза>
```

Команда сверяет каждый `sha256`, сводный `content_sha256`, состав файлов и
инвариант «в публичном слое нет фрагментов diff». Ненулевой код возврата —
релиз повреждён или подменён.

## Пересборка

```bash
uv run python -m db_pipeline.benchmark release --version {version}
```

Манифест детерминирован: два ребилда подряд дают одинаковый `content_sha256`,
поэтому расхождение — это изменение данных, а не времени сборки.

## Идентификаторы

`id` = `postgres__<head[:12]>`, где `head` — полный sha коммита-эталона (лежит в
записи целиком). Короткие префиксы проверяются на совпадение при сборке,
поэтому `id` однозначно разрешается в задачу.
"""


@beartype
def _prepare_dir(target: Path, force: bool) -> None:
    if target.exists():
        has_content = any(target.iterdir())
        if has_content and not force:
            raise PipelineError(
                "release", f"{target}: каталог не пуст, передайте --force для перезаписи"
            )
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)


@beartype
def build(
    data: Path | None = None,
    target: Path | None = None,
    version: str = DEFAULT_VERSION,
    with_oracle: bool = False,
    force: bool = False,
    split_map: dict[str, str] | None = None,
) -> dict[str, object]:
    """Собирает каталог релиза и возвращает манифест."""
    records = collect(data, split_map)
    root = paths.release_dir(data, version) if target is None else target
    _prepare_dir(root, force)
    for record in records:
        _write_json(root / "tasks" / record.file_name(), record.public_dict())
    if with_oracle:
        for record in records:
            oracle = convert.load_oracle(record.id, data)
            _write_json(root / "oracle" / record.file_name(), oracle)
    sections = _sections(records)
    _write_json(
        root / "split.json",
        {
            "sections": sections,
            "holdout_sha256": split_module.holdout_sha256(sections.get("holdout", [])),
        },
    )
    _write_text(root / "licenses.md", _licenses_markdown(records))
    name = f"db-pipeline-{CORE}"
    _write_text(root / "README.md", _readme(version, name, len(records), with_oracle))
    listed = _list_files(root)
    files = [{"path": rel, "sha256": file_sha256(root / rel)} for rel in listed]
    manifest: dict[str, object] = {
        "format": FORMAT,
        "name": name,
        "version": version,
        "core": CORE,
        "source": records[0].source,
        "license": records[0].license,
        "tasks": len(records),
        "splits": {key: len(value) for key, value in sorted(sections.items())},
        "holdout_sha256": split_module.holdout_sha256(sections.get("holdout", [])),
        "with_oracle": with_oracle,
        "files": files,
        "content_sha256": _content_sha256(files),
    }
    _write_json(root / "manifest.json", manifest)
    manifest["target"] = str(root)
    return manifest


@beartype
def _list_files(root: Path) -> list[str]:
    """Относительные пути всех файлов релиза, кроме самого манифеста, по алфавиту."""
    found: list[str] = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "manifest.json":
            found.append(path.relative_to(root).as_posix())
    return found


@beartype
def _content_sha256(files: Sequence[dict[str, str]]) -> str:
    lines = [f"{item['path']} {item['sha256']}" for item in files]
    joined = "\n".join(lines)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


@beartype
def verify(target: Path) -> dict[str, object]:
    """Сверяет распакованный релиз с манифестом.

    Проверяются: наличие манифеста, каждый ``sha256``, сводный ``content_sha256``,
    отсутствие лишних файлов и «в публичном слое нет фрагментов diff».
    """
    manifest_path = target / "manifest.json"
    if not manifest_path.is_file():
        raise PipelineError("release", f"{target}: нет manifest.json")
    manifest = as_dict(load_json_file(manifest_path), "manifest")
    files_raw = manifest.get("files")
    if not isinstance(files_raw, list):
        raise PipelineError("release", "manifest.files: ожидался список")
    listed: list[dict[str, str]] = []
    missing: list[str] = []
    mismatched: list[str] = []
    for item in cast("list[object]", files_raw):
        row = as_dict(item, "file")
        rel = str(row.get("path", ""))
        expected = str(row.get("sha256", ""))
        listed.append({"path": rel, "sha256": expected})
        path = target / rel
        if not path.is_file():
            missing.append(rel)
            continue
        if file_sha256(path) != expected:
            mismatched.append(rel)
    listed_paths = {row["path"] for row in listed}
    extra = sorted(set(_list_files(target)) - listed_paths)
    content_ok = _content_sha256(listed) == str(manifest.get("content_sha256", ""))
    leaks = _leaks(target)
    ok = len(missing) == 0 and len(mismatched) == 0 and len(extra) == 0 and content_ok and len(leaks) == 0
    return {
        "target": str(target),
        "tasks": manifest.get("tasks"),
        "files": len(listed),
        "missing": missing,
        "mismatched": mismatched,
        "extra": extra,
        "content_sha256_ok": content_ok,
        "public_leaks": leaks,
        "ok": ok,
    }


@beartype
def _leaks(target: Path) -> list[str]:
    """Ищет фрагменты diff в публичных задачах: их там быть не должно."""
    found: list[str] = []
    folder = target / "tasks"
    if not folder.exists():
        return found
    for path in sorted(folder.glob("*.json")):
        text = path.read_text(encoding="utf-8")
        if "diff --git" in text or "+++ b/" in text or "@@" in text:
            found.append(path.name)
    return found
