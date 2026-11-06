# Milestone specs

One spec per milestone (or per large part of one), written at the start of the milestone by the
`requirements-analyst` agent and approved by Thanh before building starts. Each spec is a data
contract: sources, tables with grain, keys and partitions, data quality rules, and numbered
acceptance criteria that the `test-engineer` checks on UAT at milestone close.

Naming: `<milestone>-<slug>.md`, for example `m1-bronze-ingestion.md`. The workflow is described in
[`CLAUDE.md`](../../CLAUDE.md).
