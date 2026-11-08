"""Fail a PR whose "Agents run" section doesn't name the agents its changes need.

The rules mirror the build loop in CLAUDE.md:

- any change outside docs needs `code-reviewer`;
- ops changes (CI workflows, Docker, Compose, Makefile, env examples) also need `devops-engineer`;
- changes that can alter exposure or secrets (CI workflows, which can reach the self-hosted
  runner, Compose files and env examples) also need `security-auditor`;
- a docs-only PR still needs the section filled in, for example "none (docs only)".
  Markdown under `.claude/` (agent files, conventions) counts as docs.

Naming an agent is enough to pass, so a skip with a written reason ("security-auditor:
skipped, because ...") is allowed. A line starting with `skip:` and a reason passes whatever
changed, for small manual PRs. The point is that every skip is visible on the PR.

Usage in CI: PR_BODY="<body>" python3 scripts/check_pr_agents.py <base-sha> <head-sha>
Standard library only, so the job needs no dependencies.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from collections.abc import Iterable

SECTION = "Agents run"

_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
_HEADING = re.compile(r"^##\s+(.+?)\s*$", re.MULTILINE)

_DOCS = re.compile(r"^(docs/.*|[^/]+\.md|\.claude/.*\.md|\.github/[^/]+\.md)$")
_OPS = re.compile(
    r"^(\.github/workflows/.*|docker/.*|docker-compose[^/]*\.ya?ml|Makefile|\.env[^/]*\.example"
    r"|\.dockerignore|scripts/check_pr_agents\.py)$"
)
_SECURITY = re.compile(r"^(\.github/workflows/.*|docker-compose[^/]*\.ya?ml|\.env[^/]*\.example)$")
_SKIP = re.compile(r"^\s*[-*]?\s*skip:\s*\S", re.IGNORECASE | re.MULTILINE)


def agents_section(body: str) -> str | None:
    """Return the text of the "## Agents run" section without comments, or None if absent."""
    text = _COMMENT.sub("", body.replace("\r\n", "\n"))
    headings = list(_HEADING.finditer(text))
    for i, match in enumerate(headings):
        if match.group(1).strip().lower() == SECTION.lower():
            end = headings[i + 1].start() if i + 1 < len(headings) else len(text)
            return text[match.end() : end].strip()
    return None


def required_agents(changed_files: Iterable[str]) -> list[str]:
    """Agents the changed files call for, in the order CLAUDE.md lists them."""
    files = list(changed_files)
    needed: list[str] = []
    if any(not _DOCS.match(f) for f in files):
        needed.append("code-reviewer")
    if any(_OPS.match(f) for f in files):
        needed.append("devops-engineer")
    if any(_SECURITY.match(f) for f in files):
        needed.append("security-auditor")
    return needed


def problems(body: str, changed_files: Iterable[str]) -> list[str]:
    """Everything wrong with the PR body, or an empty list if it passes."""
    section = agents_section(body)
    if section is None:
        return [f'The PR body has no "## {SECTION}" section (see the PR template).']
    if not section:
        return [f'"## {SECTION}" is empty. Name the agents that ran, or "none (reason)".']
    if _SKIP.search(section):
        return []
    return [
        f'"## {SECTION}" does not mention `{agent}`, which these changes need '
        f"(see the build loop in CLAUDE.md). Run it, or say why it was skipped."
        for agent in required_agents(changed_files)
        if agent not in section
    ]


def _changed_files(base: str, head: str) -> list[str]:
    out = subprocess.run(
        ["git", "diff", "--name-only", "--no-renames", f"{base}...{head}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [line for line in out.splitlines() if line]


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: check_pr_agents.py <base-sha> <head-sha>", file=sys.stderr)
        return 2
    files = _changed_files(argv[1], argv[2])
    found = problems(os.environ.get("PR_BODY", ""), files)
    print(f"Changed files: {len(files)}. Agents needed: {required_agents(files) or 'none'}.")
    for problem in found:
        print(f"::error::{problem}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
