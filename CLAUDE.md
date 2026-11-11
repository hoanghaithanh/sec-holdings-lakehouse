# CLAUDE.md

Guidance for Claude Code (and humans) working in this repository. Read it before starting work.

## What this project is

A lakehouse over public SEC Form 13F filings: **Dagster** orchestrates **PySpark** jobs that write
**Delta Lake** tables in a raw / bronze / silver / gold layout, with entity resolution for managers
and securities. It is a solo portfolio project, built about 6 hours a week until mid-April 2027,
and employers will read the code, the ADRs and the commit history. Keep both the code and the
history clean.

Milestones: M0 Foundations (done), M1 Bronze ingestion, M2 Silver, M3 Entity resolution,
M4 Gold and serving, M5 Polish, write-up and demo. Each milestone is a GitHub milestone.

## Commands

Every change must pass these before it is pushed (`make check` runs all of them, same as CI):

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest
uv run dagster definitions validate -m sec_lakehouse.definitions
```

CI also builds the Docker image and runs a Compose smoke test that materializes an asset into
the object store. Docker cannot be built in Claude's cloud sandbox, so CI is the check for it.

## Where things go

| What | Where |
|---|---|
| Milestone specs (data contracts + acceptance criteria) | `docs/specs/<milestone>-<slug>.md` |
| Architecture decisions | `docs/adr/NNN-<slug>.md`, numbered after the last one |
| Runbooks (how to run, backfill, recover) | `docs/runbooks/` |
| Acceptance reports | `docs/qa/<milestone>-acceptance.md` |
| Retros (3 lines per milestone) | `docs/retrospectives.md` |
| Assets / resources | `src/sec_lakehouse/assets/`, `src/sec_lakehouse/resources/` |
| Test fixtures (small real 13F samples) | `tests/fixtures/` |
| Feature walkthroughs (explain a feature to a junior, with diagrams) | `docs/guides/<feature>.md` |

## Environments

- **dev**: ThanhPC (Windows), `uv run dagster dev` or `docker compose up`.
- **uat**: homelab server, Docker Compose with `docker-compose.uat.yml` and `.env.uat` (secrets
  live only on the server). Spark standalone: 1 master, 2 workers. Data lives on `/mnt/md0`.
- Config comes only from env vars (`LAKEHOUSE_ENV`, `SPARK_MASTER`, `LAKEHOUSE_ROOT`, `S3_*`).
  No code path may be specific to one machine.
- Homelab rules: never touch port 8080, ports 4040 to 4042, port 8888, or the existing `sparkpb`
  app. Never run commands against the homelab without Thanh's explicit OK for that step.

## SEC and data rules

- **EDGAR fair access**: at most 10 requests per second, and every request sends a `User-Agent`
  naming the app and a contact email (from config, never hardcoded). Raw downloads are stored once
  in the raw/bronze layer and never fetched twice.
- **13F quirks to design and test for**: amendments (13F-HR/A, which either restate or add
  holdings), confidential treatment, the `value` unit change from thousands of dollars to dollars
  for filings from January 2023, CUSIP as the security key, and filer CIK vs manager name.
- **Pipeline rules**: every asset is idempotent and re-runnable per quarter partition. Bronze is
  raw and append-only. Silver writes with Delta `MERGE` on declared keys or `replaceWhere` on the
  partition. Schema changes are explicit, never inferred silently. New tables get Dagster asset
  checks (row count > 0, key uniqueness, not-null keys).
- **Spark**: no `collect()` or `toPandas()` on unbounded data, prefer built-in functions to Python
  UDFs, filter on partition columns so reads prune.

## How work flows (lean SDLC)

Subagents live in `.claude/agents/`. Use them as described here, not more. Thanh asked for this
workflow, so invoking these agents at the steps below is expected.

**Milestone start (once per milestone)**
1. `requirements-analyst` writes the milestone spec in `docs/specs/`.
2. `architect` writes 1 or 2 ADRs for the decisions that matter.
3. **Checkpoint 1**: Thanh approves the spec and ADRs. Do not start building before that. Once
   Thanh approves, push a last commit marking the spec Approved and the ADRs Accepted, then Thanh
   merges.

**Build loop (every session)**
1. The main session implements one small slice with its tests. There is no developer subagent.
   Every PR that adds or changes a feature also adds or updates its walkthrough in `docs/guides/`
   (Thanh asked for this on 2026-11-09). Write it for a junior developer: what the feature does and
   why, a Mermaid class diagram, a flowchart of one run and a sequence diagram (pick the ones that
   fit), a step-by-step tour of the code, the safety checks, the tests and a short glossary. Check
   that the Mermaid blocks render. A small fix or a docs-only PR doesn't need one.
   Example: `docs/guides/form13f-bronze-walkthrough.md`.
2. `code-reviewer` reviews the diff. Fix Blockers and Majors before opening or updating the PR.
   Ops changes (CI workflows, Docker, Compose, Makefile, env examples) also get `devops-engineer`.
   When CI workflows (they can reach the self-hosted runner), Compose, env examples, secrets, ports,
   networking or the tunnel change, `security-auditor` reviews too, before the PR is opened.
3. Open a PR (template in `.github/pull_request_template.md`), CI must be green. The PR's
   **Agents run** section names each agent that ran and what it found; the `PR agents` CI check
   (`scripts/check_pr_agents.py`) fails if an agent the changed files need is missing from it.
   A skip is allowed only with its reason written there (`skip: <reason>` for a small manual PR).
4. **Checkpoint 2**: Thanh merges. Claude never merges or pushes to `main`.

**Milestone close (once per milestone)**
1. `devops-engineer` prepares the UAT promotion. If secrets, Compose, ports or the tunnel changed,
   run `security-auditor` first.
2. `test-engineer` runs acceptance on UAT against the spec and writes the acceptance report.
3. `tech-writer` updates the README and runbooks.
4. Add a 3-line retro to `docs/retrospectives.md` (went well, didn't, one change).
5. **Checkpoint 3**: Thanh signs off and closes the milestone.

Skip what doesn't apply: a small fix goes straight into the build loop with no spec or ADR.
Agents are listed by the Agent tool only if `.claude/agents/` existed when the session started. If
they are missing (a cloud session can start from an older checkout), run a `general-purpose` agent
told to read the agent's file and follow it exactly, keep to its tools (read-only agents stay
read-only), and pass the agent's `model`. Don't skip the step.
Agents don't call each other. When something is ambiguous, an agent states its assumption in its
output, and the main session raises it with Thanh at the next checkpoint.

## Coding conventions

- Python 3.11, `mypy --strict`, ruff for lint and format. Match the existing style in the module.
- Small PRs, one concern each. Conventional commit messages (`feat:`, `fix:`, `docs:`, `ci:` ...).
- Don't expand scope. Note unrelated problems as deferred work instead of fixing them inline.
- Never claim tests pass without running them. Run a new test once without the change to see it
  fail when that is practical.
- Never commit secrets, hostnames of private infrastructure, or real `.env` files.

## GitHub

Rules for issues, labels and which tool to use are in `.claude/GITHUB_CONVENTIONS.md`. In short:
use the GitHub MCP tools when the session has them (cloud sessions), otherwise the `gh` CLI
(ThanhPC). Only the main session writes to GitHub; subagents report back instead.
