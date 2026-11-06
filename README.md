# SEC Holdings Lakehouse

A lakehouse over public SEC Form 13F filings, built with **Dagster**, **PySpark** and **Delta Lake**,
with entity resolution for managers and securities.

> Status: **M0 Foundations**. One Dagster asset writes a Delta table; CI runs lint, types, tests
> and Dagster definition checks.

## Architecture (so far)

```
Dagster (webserver + daemon) ──► SparkSession (Delta + S3A) ──► S3 store / local disk
        │                              │                         raw / bronze / silver / gold
        └── Postgres (run + event storage)   └── local[*] in dev, Spark standalone in UAT
```

One Docker image runs every role (Dagster webserver, Dagster daemon, Spark master, Spark workers),
so Python, PySpark and Delta versions are identical across environments.

## Environments

Everything is driven by environment variables. No code path is specific to one machine.

| Variable           | dev (laptop)        | uat (homelab)               |
|--------------------|---------------------|-----------------------------|
| `LAKEHOUSE_ENV`    | `dev`               | `uat`                       |
| `SPARK_MASTER`     | `local[*]`          | `spark://spark-master:7077` |
| `LAKEHOUSE_ROOT`   | `data/lakehouse` or `s3a://lakehouse` | `s3a://lakehouse` |
| `S3_ENDPOINT`      | `http://object-store:9000` (Compose) | `http://object-store:9000` |
| `SPARK_EXTRA_CONF` | (empty)             | driver host, executor sizing |

`uat` and `prod` refuse to start unless tables go to an object store, and an `s3a://` root
refuses to start without credentials. See [`src/sec_lakehouse/config.py`](src/sec_lakehouse/config.py).

## Quickstart

### Without Docker (fastest loop)

Needs Python 3.11, Java 17+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest
uv run dagster dev          # http://localhost:3000, writes to ./data/lakehouse
```

### Full local stack (dev)

```bash
cp .env.example .env        # then change the passwords
docker compose up --build   # Dagster UI on :3000, object store console on :9001
```

Open Dagster, select `hello_delta`, and click **Materialize**. The table lands in the object store (RustFS) at
`s3a://lakehouse/bronze/hello_delta`.

### Homelab (uat)

```bash
cp .env.uat.example .env.uat   # fill in secrets
docker compose -f docker-compose.yml -f docker-compose.uat.yml --env-file .env.uat up -d
```

This adds a Spark standalone cluster (1 master, 2 workers of 10 cores / 20 GB). Spark UI on :8080.

## Development

```bash
make lint      # ruff check + format check
make typecheck # mypy --strict
make test      # pytest (starts a local Spark)
make validate  # dagster definitions validate
make check     # all of the above, same as CI
```

### How the work is organised

Each milestone starts with a spec (`docs/specs/`) and ADRs (`docs/adr/`), then moves in small,
reviewed PRs, and ends with an acceptance run on the homelab UAT stack. Claude Code subagents in
`.claude/agents/` help with specs, design, review, tests, ops and docs; [`CLAUDE.md`](CLAUDE.md)
describes the workflow.

## Repository layout

```
src/sec_lakehouse/
  config.py          environment settings (dev / uat / prod)
  definitions.py     Dagster code location
  resources/         SparkResource, LakehouseStorage
  assets/            Dagster assets (bronze, silver, gold to come)
tests/               pytest suite, runs Spark in local mode
docker/              Dockerfile and Dagster instance config
docs/adr/            architecture decision records
```

## Decisions

- [ADR-001: Lakehouse on Delta Lake instead of a warehouse](docs/adr/001-lakehouse-over-warehouse.md)
