from __future__ import annotations

import json
import re
import subprocess

from beartype import beartype

from db_pipeline.errors import ConfigError
from db_pipeline.models import StatementSource
from db_pipeline.parsing import as_dict

_FIX = re.compile(r"(?i)\b(?:fix(?:es|ed)?|close[sd]?|resolve[sd]?)\s+#(\d+)")
_ISSUE = re.compile(r"#(\d+)")


@beartype
def choose_statement(
    subject: str,
    body: str,
    pr_number: int | None,
) -> tuple[str, StatementSource, int | None]:
    issue_number = linked_issue(subject, body)
    if issue_number is not None:
        issue_text = fetch_issue_text(issue_number)
        if issue_text is not None:
            return issue_text, "issue", issue_number
    if pr_number is not None:
        pr_text = fetch_pr_text(pr_number)
        if pr_text is not None:
            return pr_text, "pr", issue_number
        return f"{subject}\n\n{body}".strip(), "pr", issue_number
    return f"{subject}\n\n{body}".strip(), "commit_message", issue_number


@beartype
def linked_issue(subject: str, body: str) -> int | None:
    match = _FIX.search(f"{subject}\n{body}")
    if match is not None:
        return int(match.group(1))
    title_match = _ISSUE.search(subject)
    if title_match is not None:
        return int(title_match.group(1))
    return None


@beartype
def fetch_issue_text(issue_number: int) -> str | None:
    payload = _github_json(f"https://api.github.com/repos/duckdb/duckdb/issues/{issue_number}")
    if payload is None:
        return None
    title = payload.get("title")
    body = payload.get("body")
    if not isinstance(title, str):
        return None
    text = title if not isinstance(body, str) else f"{title}\n\n{body}"
    return text.strip()


@beartype
def fetch_pr_text(pr_number: int) -> str | None:
    payload = _github_json(f"https://api.github.com/repos/duckdb/duckdb/pulls/{pr_number}")
    if payload is None:
        return None
    title = payload.get("title")
    body = payload.get("body")
    if not isinstance(title, str):
        return None
    text = title if not isinstance(body, str) else f"{title}\n\n{body}"
    return text.strip()


@beartype
def _github_json(url: str) -> dict[str, object] | None:
    try:
        raw = subprocess.check_output(
            [
                "curl",
                "-fsS",
                "--max-time",
                "30",
                "-H",
                "Accept: application/vnd.github+json",
                "-H",
                "User-Agent: db-pipeline",
                url,
            ],
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    try:
        parsed: object = json.loads(raw)
    except json.JSONDecodeError:
        return None
    try:
        return as_dict(parsed, "github")
    except ConfigError:
        return None
