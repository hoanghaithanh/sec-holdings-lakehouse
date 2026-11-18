# Refreshing the Silver 13F tables

How to build Silver for one data set, run the full Silver backfill, rerun a data set, and what to
do after a Bronze re-land or a blocked run. Background: [M2 spec](../specs/m2-silver.md),
[ADR-005](../adr/005-silver-keys-layout-and-write-strategy.md),
[walkthrough](../guides/form13f-silver-filing-walkthrough.md).

As of slice S1, one job, `form13f_silver_job`, refreshes two tables for one data set (one
partition, the same keys as Bronze):

- `silver/form13f_filing`: one typed row per filing, Delta-partitioned by `period_of_report`;
- `silver/form13f_quarantine`: the Bronze rows that failed a rule, partitioned by `_dataset`.

Holdings (S2) and the effective table (S3) join the same job later. A Silver run reads Bronze only:
it makes no SEC request and works with `SEC_USER_AGENT` unset.

## Before the first run

- The Bronze data set is loaded and its run was green (`form13f_bronze_job`,
  [Bronze runbook](form13f-bronze.md)). A Silver run refuses a data set whose Bronze tables come
  from different Bronze runs ("half loaded").
- No Bronze backfill is running. Bronze and Silver share the pool `delta_writes` (limit 1), so
  they would only queue behind each other, but a Silver run during a Bronze backfill can read a
  data set that is about to change.
- In the Dagster UI, **Deployment → Concurrency** lists the pool `delta_writes` with limit 1.

On UAT, run the commands below from `~/sec-lakehouse` with the `dc` shortcut from the
[Bronze runbook](form13f-bronze.md#commands-used-below). On ThanhPC use plain `docker compose`.

## Refresh one data set

UI: **Jobs → form13f_silver_job → Partitions**, pick the data set, **Materialize**.

CLI:

```bash
dc exec dagster-daemon dagster job backfill -w /opt/dagster/home/workspace.yaml \
  -j form13f_silver_job --partitions 2022q4 --noprompt
```

Check: the run is green, and `silver/form13f_filing` shows `inserted`, `updated`, `deleted`,
`quarantined` and `superseded` in its metadata. A rerun with nothing changed in Bronze shows
`0, 0, 0` and writes no new filing version.

## Full backfill

```bash
dc exec dagster-daemon dagster job backfill -w /opt/dagster/home/workspace.yaml \
  -j form13f_silver_job --all --noprompt
```

The order of the data sets does not matter: each accession ends up with the copy from the
latest data set that holds it, whichever ran first (ADR-005).

## Rerun one data set

Same command as "Refresh one data set". Safe at any time: it only rewrites rows that differ from
Bronze, and leaves other data sets' rows (and their `_silver_run_id`) alone.

## After a Bronze re-land

If a data set was re-landed in Bronze ([Bronze runbook](form13f-bronze.md)), its Silver rows are
stale until Silver runs again for it. Rerun Silver for that data set, then read the filing
asset's metadata:

- `handed_back_to`: data sets that also hold an accession this data set dropped. Rerun Silver for
  each one listed, so the earlier copy comes back. `none (gone from Bronze)` needs nothing.
- `taken_over_from`: data sets whose rows this run replaced with newer copies. Rerun those too if
  their quarantine rows matter (their old quarantine rows for those accessions stay until then).

## A run that fails SDQ1 (no Silver rows for the data set)

SDQ1 runs after writing. It fails when every filing of the data set was quarantined or is owned
by a later data set. Read SDQ3's metadata (`quarantined`, `superseded`): if every row is
superseded, the data set was fully republished in a later zip and the failure is expected; note
it in the acceptance report. Otherwise treat it like an SDQ6 block below.

## A run blocked by SDQ6 (too many quarantined filings)

SDQ6 blocks a data set when more than 1% of its filings would be quarantined, and nothing is
written. The error lists the top reason codes.

1. Look at the reason codes and `reason_detail` in the failed run's SDQ6 metadata and logs.
2. If the rule is wrong, fix it in a PR. If the data really is bad and should load anyway, add
   the data set to `QUARANTINE_BLOCK_RATIO_OVERRIDES` in
   `src/sec_lakehouse/sources/form13f/silver_schemas.py`, with the reason, in a PR.
3. After it is merged and deployed, rerun the data set.

To look at quarantined rows of one data set, in a Spark shell or notebook:

```python
quarantine = spark.read.format("delta").load("<LAKEHOUSE_ROOT>/silver/form13f_quarantine")
rows = quarantine.where("_dataset = '2022q4'")
rows.select("source_table", "reason_codes", "reason_detail").show(20, truncate=False)
```
