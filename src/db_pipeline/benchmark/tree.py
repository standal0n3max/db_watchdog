"""Подготовка дерева ``head_sha`` и его передача в task-container.

Дерево фиксированной точки разворачивается на хосте из локального клона,
затем на хосте же к нему применяется reverse source patch: получается buggy
исходник. Готовое дерево пакуется в архив без ``.git`` и передаётся в контейнер
через ``docker cp``. Bind mounts не используются, эталонный патч в контейнер не
попадает — он остаётся на хосте.
"""

from __future__ import annotations

import io
import subprocess
import tarfile
from pathlib import Path

from beartype import beartype

from db_pipeline.errors import PipelineError

ARCHIVE_NAME = "tree.tar.gz"


@beartype
def materialize_head(repo: Path, head: str, target: Path) -> None:
    """Разворачивает дерево ``head`` из клона ``repo`` в ``target``."""
    if not (repo / ".git").exists():
        raise PipelineError("tree", f"нет клона {repo}")
    target.mkdir(parents=True, exist_ok=True)
    init = subprocess.run(
        ["git", "init", "-q"],
        cwd=target,
        capture_output=True,
        text=True,
        check=False,
    )
    if init.returncode != 0:
        raise PipelineError("tree", init.stderr.strip() or "git init не удался")
    checkout = subprocess.run(
        [
            "git",
            f"--git-dir={repo / '.git'}",
            f"--work-tree={target}",
            "checkout",
            "-f",
            head,
            "--",
            ".",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if checkout.returncode != 0:
        raise PipelineError("tree", checkout.stderr.strip() or f"checkout {head[:12]} не удался")


@beartype
def reverse_source_patch(target: Path, gold_patch: str) -> None:
    """Применяет reverse ``gold_patch``: дерево ``head`` становится buggy."""
    if gold_patch.strip() == "":
        raise PipelineError("tree", "пустой gold_patch")
    result = subprocess.run(
        ["git", "apply", "--whitespace=nowarn", "-R", "-"],
        cwd=target,
        input=gold_patch,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PipelineError("tree", detail or "reverse source patch не применился")


@beartype
def prepare_buggy_tree(repo: Path, head: str, gold_patch: str, target: Path) -> None:
    """Материализует ``head`` и делает исходник buggy на хосте."""
    materialize_head(repo, head, target)
    reverse_source_patch(target, gold_patch)


@beartype
def archive_tree(target: Path) -> bytes:
    """Пакует дерево без ``.git``: история в контейнер не попадает."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for item in sorted(target.iterdir()):
            if item.name == ".git":
                continue
            tar.add(item, arcname=item.name)
    return buffer.getvalue()
