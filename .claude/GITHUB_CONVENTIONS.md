# GitHub conventions

## Who writes to GitHub

Only the main session writes to GitHub: PRs, issues, comments. Subagents are read-only toward
GitHub and list anything that should become an issue under **Deferred** in their output; the main
session files those. This keeps notifications low and avoids duplicate issues.

## Which tool to use

The rules below describe *what* to do. Use whichever tool the session has:

| Action | Cloud session (GitHub MCP tools) | ThanhPC (`gh` CLI) |
|---|---|---|
| Read a PR and its diff | `mcp__github__pull_request_read` | `gh pr view <n>`, `gh pr diff <n>` |
| List or search issues | `mcp__github__list_issues` | `gh issue list --search "<words>" --state all` |
| Read an issue | `mcp__github__issue_read` | `gh issue view <n>` |
| File or edit an issue | `mcp__github__issue_write` | `gh issue create` / `gh issue edit` |
| Comment | `mcp__github__add_issue_comment` | `gh issue comment` / `gh pr comment` |
| Open a PR | `mcp__github__create_pull_request` | `gh pr create` |
| Milestones | not available: Thanh manages them in the GitHub UI | `gh api /repos/{owner}/{repo}/milestones` |

Before any write, confirm the repo is `hoanghaithanh/sec-holdings-lakehouse`.

## Issues

- File issues only for real work that is deferred: a bug found but not fixed now, tech debt left
  on purpose, a follow-up out of scope. Review findings fixed in the same PR need no issue.
- Search first; comment on an existing issue instead of filing a duplicate.
- Body: what, where (file:line or asset name), why it matters or how to reproduce, suggested next step.
- Attach the milestone (`M1 Bronze ingestion` etc.) only when you know its exact title.
- Never claim an issue was filed without a real URL from the tool's result.

## Labels

- Type: `bug`, `enhancement`, `task`, `tech-debt`, `security`, `question`
- Layer: `bronze`, `silver`, `gold`, `infra`, `docs`

## Security findings

Never put a Critical or High security finding, or anything that reveals secrets or private
infrastructure, in a public issue. Report it to Thanh in the session instead.
