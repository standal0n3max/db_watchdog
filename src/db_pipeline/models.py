from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from beartype import beartype

from db_pipeline.parsing import as_dict, as_int, as_object_list, as_str, as_str_list, optional_int

StatementSource = Literal["issue", "pr", "commit_message"]


@beartype
@dataclass(frozen=True)
class FileChange:
    path: str
    additions: int
    deletions: int
    status: str


@beartype
@dataclass(frozen=True)
class ChangeView:
    sha: str
    repository_id: str
    subject: str
    body: str
    parent_count: int
    files: list[FileChange]
    pr_number: int | None
    base_sha: str


@beartype
@dataclass(frozen=True)
class FilterDecision:
    accept: bool
    reason_code: str
    reason: str


@beartype
@dataclass(frozen=True)
class TaskInstance:
    instance_id: str
    repository: str
    license: str
    base_sha: str
    statement: str
    statement_source: StatementSource
    test_patch: str
    gold_patch: str
    fail_to_pass: list[str]
    pass_to_pass: list[str]
    oracle: str
    reject_reason: str
    head_sha: str
    pr_number: int | None
    oracle_command: str = ""


@beartype
def change_to_dict(change: ChangeView) -> dict[str, object]:
    files: list[dict[str, object]] = []
    for item in change.files:
        files.append(
            {
                "path": item.path,
                "additions": item.additions,
                "deletions": item.deletions,
                "status": item.status,
            }
        )
    return {
        "sha": change.sha,
        "repository_id": change.repository_id,
        "subject": change.subject,
        "body": change.body,
        "parent_count": change.parent_count,
        "files": files,
        "pr_number": change.pr_number,
        "base_sha": change.base_sha,
    }


@beartype
def change_from_dict(value: object) -> ChangeView:
    raw = as_dict(value, "change")
    files: list[FileChange] = []
    for item in as_object_list(raw.get("files"), "change.files"):
        row = as_dict(item, "file")
        files.append(
            FileChange(
                path=as_str(row.get("path"), "path"),
                additions=as_int(row.get("additions"), "additions"),
                deletions=as_int(row.get("deletions"), "deletions"),
                status=as_str(row.get("status"), "status"),
            )
        )
    return ChangeView(
        sha=as_str(raw.get("sha"), "sha"),
        repository_id=as_str(raw.get("repository_id"), "repository_id"),
        subject=as_str(raw.get("subject"), "subject"),
        body=as_str(raw.get("body"), "body"),
        parent_count=as_int(raw.get("parent_count"), "parent_count"),
        files=files,
        pr_number=optional_int(raw.get("pr_number")),
        base_sha=as_str(raw.get("base_sha"), "base_sha"),
    )


@beartype
def task_to_dict(task: TaskInstance) -> dict[str, object]:
    return {
        "instance_id": task.instance_id,
        "repository": task.repository,
        "license": task.license,
        "base_sha": task.base_sha,
        "statement": task.statement,
        "statement_source": task.statement_source,
        "test_patch": task.test_patch,
        "gold_patch": task.gold_patch,
        "fail_to_pass": task.fail_to_pass,
        "pass_to_pass": task.pass_to_pass,
        "oracle": task.oracle,
        "reject_reason": task.reject_reason,
        "head_sha": task.head_sha,
        "pr_number": task.pr_number,
        "oracle_command": task.oracle_command,
    }


@beartype
def parse_statement_source(value: str) -> StatementSource:
    if value == "issue":
        return "issue"
    if value == "pr":
        return "pr"
    if value == "commit_message":
        return "commit_message"
    raise ValueError(f"неизвестный statement_source: {value}")


@beartype
def task_from_dict(value: object) -> TaskInstance:
    raw = as_dict(value, "task")
    statement_source = parse_statement_source(as_str(raw.get("statement_source"), "statement_source"))
    return TaskInstance(
        instance_id=as_str(raw.get("instance_id"), "instance_id"),
        repository=as_str(raw.get("repository"), "repository"),
        license=as_str(raw.get("license"), "license"),
        base_sha=as_str(raw.get("base_sha"), "base_sha"),
        statement=as_str(raw.get("statement"), "statement"),
        statement_source=statement_source,
        test_patch=as_str(raw.get("test_patch"), "test_patch"),
        gold_patch=as_str(raw.get("gold_patch"), "gold_patch"),
        fail_to_pass=as_str_list(raw.get("fail_to_pass"), "fail_to_pass"),
        pass_to_pass=as_str_list(raw.get("pass_to_pass"), "pass_to_pass"),
        oracle=as_str(raw.get("oracle"), "oracle"),
        reject_reason=as_str(raw.get("reject_reason"), "reject_reason"),
        head_sha=as_str(raw.get("head_sha"), "head_sha"),
        pr_number=optional_int(raw.get("pr_number")),
        oracle_command=_oracle_command(raw.get("oracle_command", "")),
    )


@beartype
def _oracle_command(value: object) -> str:
    if value is None:
        return ""
    return as_str(value, "oracle_command")
