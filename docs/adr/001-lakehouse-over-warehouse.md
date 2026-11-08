# ADR-001: Lakehouse on Delta Lake instead of a warehouse

- Status: Accepted (Thanh confirmed 2026-11-08)
- Date: 2026-11-05

## Context

The project ingests about a decade of SEC Form 13F data: quarterly TSV data sets plus raw
EDGAR XML for recent filings. The data is append-heavy, gets restated by amendments
(13F-HR/A), and has to be backfilled and re-run safely. It must run on hardware I own
(a laptop for dev and a 14-core / 64 GB homelab for UAT), cost nothing, and be easy for a
reviewer to clone and run.

## Options

1. **Cloud warehouse** (Snowflake, BigQuery): great SQL, but it costs money past the free tier,
   can't run on the homelab, and makes it hard for someone else to reproduce.
2. **Local warehouse** (Postgres or DuckDB files): simple, but it doesn't scale out and
   doesn't show distributed processing. Raw files and tables would live in separate systems.
3. **Lakehouse**: open table format on object storage (S3-compatible: RustFS locally, S3 or ADLS in the cloud), processed by Spark.

## Decision

Use a lakehouse: **Delta Lake tables on S3-compatible storage, processed with PySpark,
orchestrated by Dagster**, in a raw / bronze / silver / gold layout.

## Why

- **ACID writes on files.** `replaceWhere` and `MERGE` make quarterly partitions idempotent
  and handle amendments without hand-rolled locking.
- **Time travel.** Every table version stays queryable, which helps with debugging restatements
  and with "what did we know as of quarter X" questions.
- **Same storage for raw files and tables.** Raw filings are kept immutable next to the Delta
  tables built from them, so any layer can be rebuilt.
- **Portable.** An S3-compatible store (RustFS) in dev and UAT, S3 or ADLS in an optional cloud prod, with no code changes.
- **Open readers.** Gold tables can be served with DuckDB's delta extension, with no Spark at
  query time.

## Consequences

- Spark is heavy locally, so dev uses 1 to 2 years of data and full backfills run on the homelab.
- Delta tables need upkeep: `OPTIMIZE` and a scheduled `VACUUM`, plus disk headroom
  (about 100 GB) for version history.
- More moving parts than a single database (Spark, an object store, Postgres for Dagster). Docker Compose
  and one shared image keep this manageable.
