"""Tests for scripts/check_pr_agents.py, the CI check on a PR's "Agents run" section."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


def _load() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "check_pr_agents.py"
    spec = importlib.util.spec_from_file_location("check_pr_agents", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cpa = _load()

TEMPLATE_BODY = """## Summary

Something.

## Agents run

<!-- Which agents from .claude/agents/ ran on this PR and what they found, or "none (reason)". -->

## Notes for the reviewer

<!-- Anything to look at closely. -->
"""


def _body(agents: str) -> str:
    return f"## Summary\n\nx\n\n## Agents run\n\n{agents}\n\n## Notes for the reviewer\n\ny\n"


@pytest.mark.parametrize(
    ("files", "expected"),
    [
        (["docs/adr/004-x.md", "README.md", "CLAUDE.md"], []),
        ([".claude/agents/architect.md", ".github/pull_request_template.md"], []),
        (["src/sec_lakehouse/assets/x.py", "docs/x.md"], ["code-reviewer"]),
        (["uv.lock"], ["code-reviewer"]),
        (["scripts/x.py"], ["code-reviewer"]),
        (
            [".github/workflows/ci.yml"],
            ["code-reviewer", "devops-engineer", "security-auditor"],
        ),
        (["scripts/check_pr_agents.py"], ["code-reviewer", "devops-engineer"]),
        (["Makefile"], ["code-reviewer", "devops-engineer"]),
        (["docker/Dockerfile", ".dockerignore"], ["code-reviewer", "devops-engineer"]),
        (
            ["docker-compose.uat.yml"],
            ["code-reviewer", "devops-engineer", "security-auditor"],
        ),
        (
            [".env.uat.example"],
            ["code-reviewer", "devops-engineer", "security-auditor"],
        ),
    ],
)
def test_required_agents(files: list[str], expected: list[str]) -> None:
    assert cpa.required_agents(files) == expected


def test_missing_section_fails() -> None:
    assert cpa.problems("## Summary\n\nx\n", ["docs/x.md"])


def test_untouched_template_section_counts_as_empty() -> None:
    (problem,) = cpa.problems(TEMPLATE_BODY, ["docs/x.md"])
    assert "empty" in problem


def test_docs_only_pr_passes_with_none_and_a_reason() -> None:
    assert cpa.problems(_body("none (docs only)"), ["docs/x.md"]) == []


def test_code_pr_needs_code_reviewer() -> None:
    files = ["src/sec_lakehouse/x.py"]
    assert cpa.problems(_body("none"), files)
    assert cpa.problems(_body("- `code-reviewer`: no blockers"), files) == []


def test_ops_pr_lists_each_missing_agent() -> None:
    found = cpa.problems(_body("- `code-reviewer`: fine"), [".github/workflows/x.yml"])
    assert len(found) == 2
    assert "devops-engineer" in found[0] and "security-auditor" in found[1]


def test_a_named_skip_with_a_reason_passes() -> None:
    body = _body(
        "- code-reviewer: fine\n- devops-engineer: fine\n"
        "- security-auditor: skipped, the change only renames a job"
    )
    assert cpa.problems(body, ["docker-compose.yml"]) == []


def test_a_skip_line_with_a_reason_passes_any_change() -> None:
    assert cpa.problems(_body("skip: one-line Makefile fix by hand"), ["Makefile"]) == []
    assert cpa.problems(_body("- Skip: typo"), [".github/workflows/ci.yml"]) == []


def test_a_skip_line_needs_a_reason() -> None:
    assert cpa.problems(_body("skip:"), ["Makefile"])


def test_subheadings_stay_inside_the_section() -> None:
    body = "## Agents run\n\n### Reviews\n\n- code-reviewer: ok\n\n## Notes\n"
    assert cpa.problems(body, ["src/x.py"]) == []


def test_main_reports_errors_and_exit_code(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cpa, "_changed_files", lambda base, head: ["src/x.py"])
    monkeypatch.setenv("PR_BODY", _body("none"))
    assert cpa.main(["check_pr_agents.py", "base", "head"]) == 1
    assert "::error::" in capsys.readouterr().out

    monkeypatch.setenv("PR_BODY", _body("- code-reviewer: ok"))
    assert cpa.main(["check_pr_agents.py", "base", "head"]) == 0
    assert cpa.main(["check_pr_agents.py"]) == 2


def test_section_heading_is_case_insensitive_and_handles_crlf() -> None:
    body = "## agents run\r\n\r\n- code-reviewer: ok\r\n"
    assert cpa.problems(body, ["src/x.py"]) == []


def test_section_ends_at_the_next_heading() -> None:
    body = "## Agents run\n\n## Notes for the reviewer\n\ncode-reviewer looked at it\n"
    (problem,) = cpa.problems(body, ["src/x.py"])
    assert "empty" in problem
