from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

from beartype import beartype

from db_pipeline.config import CoreConfig
from db_pipeline.errors import PipelineError
from db_pipeline.models import FileChange

_PR_IN_SUBJECT = re.compile(r"\(#(\d+)\)")


@beartype
def run_git(repo: Path, args: list[str]) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PipelineError("git", detail or f"git {' '.join(args)} завершился с ошибкой")
    return result.stdout


@beartype
def ensure_sha(core: CoreConfig, clone_path: Path, sha: str, object_source: Path | None = None) -> None:
    _ensure_sha_repo(core, clone_path)
    if _has_commit(clone_path, sha):
        return
    if object_source is not None and _has_commit(object_source, sha):
        _fetch_one(clone_path, str(object_source), sha)
        if _has_commit(clone_path, sha):
            return
    _fetch_one(clone_path, "origin", sha)
    if not _has_commit(clone_path, sha):
        raise PipelineError("git_fetch", f"коммит {sha} не скачан")


@beartype
def _ensure_sha_repo(core: CoreConfig, clone_path: Path) -> None:
    if (clone_path / ".git").exists():
        return
    if clone_path.exists():
        shutil.rmtree(clone_path)
    clone_path.parent.mkdir(parents=True, exist_ok=True)
    init = subprocess.run(
        ["git", "init", "-b", "main", str(clone_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    if init.returncode != 0:
        detail = (init.stderr or init.stdout).strip()
        raise PipelineError("git_clone", detail or "git init не удался")
    remote = subprocess.run(
        ["git", "-C", str(clone_path), "remote", "add", "origin", core.clone_url],
        check=False,
        capture_output=True,
        text=True,
    )
    if remote.returncode != 0:
        detail = (remote.stderr or remote.stdout).strip()
        raise PipelineError("git_clone", detail or "remote add не удался")


@beartype
def _fetch_one(clone_path: Path, remote: str, sha: str) -> None:
    subprocess.run(
        [
            "git",
            "-C",
            str(clone_path),
            "-c",
            "http.version=HTTP/1.1",
            "fetch",
            "--depth=1",
            remote,
            sha,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


@beartype
def _has_commit(repo: Path, sha: str) -> bool:
    git_dir = repo if (repo / "HEAD").exists() and not (repo / ".git").exists() else repo
    result = subprocess.run(
        ["git", "-C", str(git_dir), "cat-file", "-e", f"{sha}^{{commit}}"],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


@beartype
def ensure_clone(core: CoreConfig, clone_path: Path) -> None:
    if _clone_is_usable(clone_path):
        try:
            fetch = subprocess.run(
                ["git", "-C", str(clone_path), "-c", "http.version=HTTP/1.1", "fetch", "--prune", "origin"],
                check=False,
                capture_output=True,
                text=True,
                timeout=90,
            )
        except subprocess.TimeoutExpired:
            return
        if fetch.returncode != 0 and not _clone_is_usable(clone_path):
            detail = (fetch.stderr or fetch.stdout).strip()
            raise PipelineError("git_fetch", detail or "fetch не удался")
        return
    if clone_path.exists():
        shutil.rmtree(clone_path)
    clone_path.parent.mkdir(parents=True, exist_ok=True)
    last_error = "клон не создан"
    for _attempt in range(4):
        if clone_path.exists():
            shutil.rmtree(clone_path)
        result = subprocess.run(
            [
                "git",
                "-c",
                "http.version=HTTP/1.1",
                "-c",
                "http.postBuffer=524288000",
                "clone",
                "--filter=blob:none",
                "--single-branch",
                "--branch",
                core.branch,
                "--depth",
                str(core.clone_depth),
                core.clone_url,
                str(clone_path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and _clone_is_usable(clone_path):
            return
        last_error = (result.stderr or result.stdout).strip() or last_error
    raise PipelineError("git_clone", last_error)


@beartype
def resolve_pr_sha(clone: Path, pr_number: int) -> str:
    needle = f"(#{pr_number})"
    output = run_git(
        clone,
        ["log", "origin/main", "--format=%H%x09%s", f"--grep={needle}", "-n", "40"],
    )
    for line in output.splitlines():
        sha, separator, subject = line.partition("\t")
        if separator == "" or sha == "":
            continue
        if subject.rstrip().endswith(needle):
            return sha
    run_git(clone, ["fetch", "--deepen=400", "origin", "main"])
    output = run_git(
        clone,
        ["log", "origin/main", "--format=%H%x09%s", f"--grep={needle}", "-n", "40"],
    )
    for line in output.splitlines():
        sha, separator, subject = line.partition("\t")
        if separator != "" and subject.rstrip().endswith(needle):
            return sha
    raise PipelineError("pr_not_found", f"PR {pr_number} не найден в origin/main")


@beartype
def _clone_is_usable(clone_path: Path) -> bool:
    if not (clone_path / ".git").exists():
        return False
    result = subprocess.run(
        ["git", "-C", str(clone_path), "rev-parse", "--verify", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and result.stdout.strip() != ""


@beartype
def commit_parents(clone: Path, sha: str) -> list[str]:
    output = run_git(clone, ["log", "-1", "--format=%P", sha])
    return [item for item in output.strip().split(" ") if item != ""]


@beartype
def commit_subject(clone: Path, sha: str) -> str:
    return run_git(clone, ["log", "-1", "--format=%s", sha]).strip()


@beartype
def commit_body(clone: Path, sha: str) -> str:
    return run_git(clone, ["log", "-1", "--format=%b", sha])


@beartype
def file_changes(clone: Path, base_sha: str, sha: str) -> list[FileChange]:
    output = run_git(clone, ["diff", "--numstat", base_sha, sha])
    changes: list[FileChange] = []
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        additions = 0 if parts[0] == "-" else int(parts[0])
        deletions = 0 if parts[1] == "-" else int(parts[1])
        changes.append(FileChange(path=parts[2], additions=additions, deletions=deletions, status="modified"))
    return changes


@beartype
def diff_range(clone: Path, base_sha: str, sha: str) -> str:
    return run_git(clone, ["diff", base_sha, sha])


@beartype
def checkout_local(clone: Path, work: Path, sha: str) -> None:
    if work.exists():
        raise PipelineError("workdir", f"каталог уже есть: {work}")
    # Клон — рабочая копия сборщика, не дерево разработчика. Прошлый прогон
    # мог оставить в нём правку закреплённого PR. Без --force следующий sha
    # не переключается, а copytree уносит чужие файлы в оракул.
    run_git(clone, ["checkout", "--force", "--detach", sha])
    run_git(clone, ["clean", "-fd"])
    work.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(clone, work, symlinks=True, ignore=shutil.ignore_patterns(".git"))
    _commit_snapshot(work, f"base {sha}")


def _commit_snapshot(work: Path, message: str) -> None:
    init = subprocess.run(
        ["git", "init"],
        cwd=work,
        check=False,
        capture_output=True,
        text=True,
    )
    if init.returncode != 0:
        detail = (init.stderr or init.stdout).strip()
        raise PipelineError("git", detail or "git init не удался")
    add = subprocess.run(
        ["git", "add", "-A"],
        cwd=work,
        check=False,
        capture_output=True,
        text=True,
    )
    if add.returncode != 0:
        detail = (add.stderr or add.stdout).strip()
        raise PipelineError("git", detail or "git add не удался")
    env = dict(os.environ)
    env["GIT_AUTHOR_NAME"] = "db-pipeline"
    env["GIT_AUTHOR_EMAIL"] = "db-pipeline@localhost"
    env["GIT_COMMITTER_NAME"] = "db-pipeline"
    env["GIT_COMMITTER_EMAIL"] = "db-pipeline@localhost"
    commit = subprocess.run(
        ["git", "commit", "-m", message],
        cwd=work,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if commit.returncode != 0:
        detail = (commit.stderr or commit.stdout).strip()
        raise PipelineError("git", detail or "снимок base не зафиксирован")


@beartype
def apply_patch(work: Path, patch_text: str) -> None:
    if patch_text.strip() == "":
        return
    result = subprocess.run(
        ["git", "-C", str(work), "apply", "--whitespace=nowarn", "-"],
        input=patch_text,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PipelineError("patch_apply", detail or "патч не применился")


@beartype
def working_diff(work: Path) -> str:
    return run_git(work, ["diff", "--", "."])


@beartype
def pr_number_from_subject(subject: str) -> int | None:
    found = list(_PR_IN_SUBJECT.finditer(subject))
    if len(found) == 0:
        return None
    return int(found[-1].group(1))


@beartype
def select_harvest_rows(
    rows: list[tuple[str, str]],
    known: set[str],
    cursor: str,
    scan: int,
    require_pr: bool,
) -> list[tuple[str, str]]:
    # rows идут от новых к старым. Курсор — последний уже просмотренный sha.
    # Следующее окно начинается сразу после него.
    started = cursor == ""
    picked: list[tuple[str, str]] = []
    for sha, subject in rows:
        if not started:
            if sha == cursor:
                started = True
            continue
        if sha in known:
            continue
        if require_pr and pr_number_from_subject(subject) is None:
            continue
        picked.append((sha, subject))
        if len(picked) >= scan:
            break
    return picked


@beartype
def ensure_origin_branch(core: CoreConfig, clone_path: Path) -> None:
    if not (clone_path / ".git").exists():
        ensure_clone(core, clone_path)
    ref = f"refs/remotes/origin/{core.branch}"
    if _has_ref(clone_path, ref):
        return
    depth = max(core.clone_depth, 80)
    result = subprocess.run(
        [
            "git",
            "-C",
            str(clone_path),
            "-c",
            "http.version=HTTP/1.1",
            "fetch",
            f"--depth={depth}",
            "origin",
            core.branch,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not _has_ref(clone_path, ref):
        detail = (result.stderr or result.stdout).strip()
        raise PipelineError("git_fetch", detail or f"ветка {core.branch} не скачана")


@beartype
def harvest_window(
    clone: Path,
    core: CoreConfig,
    known: set[str],
    cursor: str,
    scan: int,
) -> list[tuple[str, str]]:
    require_pr = core.id == "duckdb"
    limit = max(scan * 20, 400)
    chosen: list[tuple[str, str]] = []
    for _round in range(40):
        rows = _log_rows(clone, core.branch, limit)
        cursor_visible = cursor == "" or any(sha == cursor for sha, _subject in rows)
        if not cursor_visible and _is_shallow(clone):
            if not _deepen(clone, core.branch, 400):
                return select_harvest_rows(rows, known, "", scan, require_pr)
            limit += 400
            continue
        active_cursor = cursor if cursor_visible else ""
        chosen = select_harvest_rows(rows, known, active_cursor, scan, require_pr)
        if len(chosen) >= scan or not _is_shallow(clone):
            return chosen
        before = len(rows)
        if not _deepen(clone, core.branch, 400):
            return chosen
        limit += 400
        if len(_log_rows(clone, core.branch, limit)) <= before:
            return chosen
    return chosen


@beartype
def _log_rows(clone: Path, branch: str, limit: int) -> list[tuple[str, str]]:
    output = run_git(
        clone,
        [
            "log",
            f"origin/{branch}",
            "--first-parent",
            "-n",
            str(limit),
            "--format=%H%x09%s",
        ],
    )
    rows: list[tuple[str, str]] = []
    for line in output.splitlines():
        sha, separator, subject = line.partition("\t")
        if separator == "" or sha == "":
            continue
        rows.append((sha, subject))
    return rows


@beartype
def _has_ref(clone: Path, ref: str) -> bool:
    result = subprocess.run(
        ["git", "-C", str(clone), "rev-parse", "--verify", "--quiet", ref],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


@beartype
def _is_shallow(clone: Path) -> bool:
    return run_git(clone, ["rev-parse", "--is-shallow-repository"]).strip() == "true"


@beartype
def _deepen(clone: Path, branch: str, commits: int) -> bool:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(clone),
            "-c",
            "http.version=HTTP/1.1",
            "fetch",
            f"--deepen={commits}",
            "origin",
            branch,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0
