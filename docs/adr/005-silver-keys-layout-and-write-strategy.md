# ADR-005: Silver keys, layout, write strategy and quarantine

- Status: Proposed (awaiting Checkpoint 1)
- Date: 2026-11-17
- Spec: `docs/specs/m2-silver.md` (draft)
- Depends on spec answers: designed on the spec's recommended answers, which Thanh has not
  decided yet.
  - **Q1 = B** (Delta partition by `period_of_report`). With **A** (`_dataset`), the period list
    in the `MERGE` becomes a `_dataset` list (X plus the data sets it takes accessions from) and
    Gold reads by quarter scan everything; the rest stands. With **C**, there is no list and every
    `MERGE` reads the whole table.
  - **Q6 = A** (latest data set wins). With **B**, `owner` below becomes `min(_dataset)`, nothing
    else changes. With **C**, conflicting copies get a new reason code in every data set that
    holds them (about +2 h).
  - **Q4 = B** (block above 1%). With **A**, SDQ6 is warn only; with **C**, the threshold is 0.
  - **Q8 = A** (acceptance after M1 closes). The order-independence argument assumes Bronze is
    complete before the Silver backfill.
  - Q5 only decides which tables exist; the pattern below applies to any added table.

## Context

One Silver run handles one Bronze data set (the spec's contract), must be safe to re-run (AC7)
and must give the same result whatever order the data sets run in (AC8). The same accession can
sit in two data sets; the latest one wins (Q6). Holdings are large (about 150 to 200 M rows,
**unverified**); filings are small. Rows that fail a rule go to quarantine instead of
disappearing. Constraints: about 6 h a week, 2 Spark workers, Delta on S3-compatible storage
with one writer at a time (ADR-002), and the Spark rules in `CLAUDE.md`.

Terms used below:

- A **Dagster partition** is a slice of an asset that runs and is tracked on its own; here one
  per SEC data set, as in Bronze. A **Delta partition** is a folder of files inside a table, one
  per value of a column (`period_of_report=2024-12-31/`). A query that filters on that column
  reads only the matching folders; this is **pruning**. The two are independent: one Dagster
  partition (a data set) writes into several Delta partitions (quarters).
- **`MERGE`** compares new rows with a table on a key and, in one atomic commit, updates matches,
  inserts the rest, and deletes rows when told to.
- **`replaceWhere`** overwrites only the rows matching a condition, in one commit (ADR-002).
- **Schema enforcement**: Delta refuses a write whose columns or types differ from the table's.
  Silver never turns on `mergeSchema` or `overwriteSchema`, so a schema change is always a PR.
- **Idempotent**: running a step twice on the same input leaves the same result as once.
- An **asset check** is a Dagster test stored with the table's history; **block** fails the step,
  **warn** records a number (ADR-002).
- A **pool** is a named Dagster slot limit shared by all runs; a step in a full pool waits.

## Options

How a data set run refreshes `filing` and `holding`:

1. **`MERGE` the new rows with `WHEN NOT MATCHED BY SOURCE DELETE`.** The textbook statement.
   Tested on delta-spark 3.3.3: that clause makes the `MERGE` read every file of the table, even
   when both conditions name one period (a file removed by hand in another period made it fail),
   and a rerun with no change rewrote every file. For holdings, a full read and rewrite per data
   set. Kept only for the small effective table (ADR-004).
2. **Change-set `MERGE` (chosen).** Spark first works out what must change (insert, update,
   delete), then `MERGE`s only that, with the affected periods as literals in the condition.
   Tested: a literal `IN` list on the partition column prunes; `t.period_of_report =
   s.period_of_report` alone does not. A rerun with no change writes nothing.
3. **`replaceWhere` on `_dataset = 'X'`.** One write, no comparison. But `_dataset` is not the
   partition column under Q1 B, so it rewrites every file holding a row of X, take-overs from
   another data set need an extra accession list in the condition, and `CLAUDE.md` allows
   `replaceWhere` on the partition only.

Asset shape: **one multi-asset** writing `filing`, `holding` and `quarantine` (chosen: one Spark
application, filings feed holdings in memory, quarantine has one writer per data set), or **three
assets** (nicer lineage, but quarantine would have two writers and holdings would re-read
filings in a second Spark application).

## Decision

- Multi-asset `silver_form13f` on `FORM13F_PARTITIONS`, producing `silver/form13f_filing`,
  `silver/form13f_holding` and `silver/form13f_quarantine`; `deps` on the four Bronze tables it
  reads; `can_subset=False`; `pool="delta_writes"`. Plus `silver/form13f_filing_effective`
  (ADR-004). Job `form13f_silver_job` selects these two assets only, so a Silver run never makes
  an HTTP request (AC2). Backfill: `dagster job backfill -j form13f_silver_job --all --noprompt`.
- `filing` and `holding`: Delta partition `period_of_report`, written by a change-set `MERGE`.
- `quarantine`: Delta partition `_dataset`, written by `replaceWhere _dataset = '<key>'`.
- An accession belongs to the latest Bronze data set whose SUBMISSION holds it. A filing and its
  holding lines always come from that one data set.

## Design

### Tables

| Table | Key | Delta partition | Write |
|---|---|---|---|
| `silver.form13f_filing` | `accession_number` | `period_of_report` | change-set `MERGE` |
| `silver.form13f_holding` | `(accession_number, infotable_sk)` | `period_of_report` | change-set `MERGE` |
| `silver.form13f_quarantine` | `(source_table, _dataset, _source_line)` | `_dataset` | `replaceWhere` |
| `silver.form13f_filing_effective` | `accession_number` | none | `MERGE` (ADR-004) |

Paths come from `storage.table_path("silver", ...)`. Each schema is a declared `StructType` in
`sources/form13f/silver_schemas.py`; the step creates a missing table with
`DeltaTable.createIfNotExists(...).addColumns(schema).partitionedBy(...)` (checked on 3.3.3), so
the first write never infers a schema.

### One run for data set X

0. **Guard.** The four Bronze tables read (submission, coverpage, summarypage, infotable) must
   have rows for X and one and the same `_run_id` (a pruned `DISTINCT`, a few values at most).
   Otherwise fail: "Bronze X is half loaded, rerun `form13f_bronze_job` for X" (ADR-002 has no
   cross-table transaction).
1. **Owner.** `owner(A) = max(_dataset)` over `bronze.form13f_submission` rows of accession A.
   Keys sort in time order as strings (ADR-002). This reads two columns of the whole table (about
   a million short rows), the only unpruned Bronze read.
2. **Cast and split.** Built-in functions only (`to_date(c, 'dd-MMM-yyyy')`, `rlike`, `cast`,
   `when`). Each Bronze row of X gets a `reason_codes` array. Rows whose accession has
   `owner > X` are **superseded**: counted, never written, never quarantined. Holdings join X's
   good filings (a broadcast join: one data set has tens of thousands of filings) for `cik`,
   dates, types and `value_unit`; no match is `NO_FILING`. The frame is persisted, then split
   into "good" (`size(reason_codes) = 0`) and "quarantine", so each row lands in exactly one.
3. **Block checks before any write** (nothing is written if one fails): SDQ4, SDQ5, SDQ6.
4. **Scope and periods.** `S_X` = accessions with `owner = X`, plus accessions whose current
   Silver filing row has `_dataset = X`. `P_X` = periods of X's good filings plus periods of
   Silver filing rows in `S_X`. `P_X` is collected to the driver: it holds quarter ends that
   passed `BAD_PERIOD`, so at most about 150 dates.
5. **Change sets**, per table: the good rows (new) full-outer-joined on the key with the Silver
   rows in `period_of_report IN P_X` and accession in `S_X` (old). Only new: insert. Both and any
   column differs (null-safe `<=>`, generated from the declared schema, including `_dataset`):
   update. Only old: delete. Identical: dropped. `_silver_processed_at` and `_silver_run_id` are
   set on inserts and updates only. The change set is persisted, then counted and merged.
6. **Writes**, in this order: holding `MERGE`, filing `MERGE`, quarantine `replaceWhere`. An empty
   change set skips its `MERGE` (no new Delta version).
7. **Checks after writing**: SDQ1, SDQ2, SDQ3, SDQ7 (block), SDQ9 to SDQ13 (warn), all filtered
   on `period_of_report IN P_X` or the quarantine's `_dataset`, so they prune. A block failure
   here fails the step after the data is written, like Bronze DQ5: the run shows red, the
   effective step does not run, and a rerun after the fix repairs the data set. Run metadata:
   rows inserted, updated, deleted, quarantined and superseded per table, the size of `P_X`, and
   the data sets accessions were taken over from or handed back to (see Risks).

The holding `MERGE` (the filing one is the same on `accession_number`):

```python
periods = ", ".join(f"DATE'{p}'" for p in p_x)  # literal list: this is what prunes
(DeltaTable.forPath(session, holding_path).alias("t")
    .merge(changes.alias("s"),
           f"t.period_of_report IN ({periods}) "
           "AND t.accession_number = s.accession_number AND t.infotable_sk = s.infotable_sk")
    .whenMatchedDelete(condition="s._op = 'D'")
    .whenMatchedUpdate(condition="s._op = 'U'", set=columns)
    .whenNotMatchedInsert(condition="s._op = 'U'", values=columns)
    .execute())
```

Each target row matches at most one change row (both sides are unique on the key: Bronze DQ4
and SDQ2), and Delta fails loudly if that is ever broken. If a re-published copy carries another
period, the old row's period is in `P_X` (from the filing table), so the update moves the row
to its new Delta partition instead of leaving a duplicate.

**Why holdings first.** The filing table is the index that says which periods may hold rows of
`S_X`: every Silver holding row has a Silver filing row with the same accession and period (SDQ7,
block). Writing filings last means a run that dies between the two `MERGE`s still finds every
affected period on its rerun.

### Idempotency and order independence

- **What a run reads.** The new rows come from Bronze X and the owner map from Bronze; neither
  depends on Silver or on run order. Silver is read only to find the rows to replace.
- **What a run guarantees.** Afterwards, for every accession in `S_X`, Silver holds exactly X's
  good rows if X owns it, and nothing otherwise. Rows outside `S_X` are not in the change set,
  so other data sets' rows keep their values, `_silver_run_id` and `_silver_processed_at` (AC7).
- **Rerun of X.** Same inputs, so the change set is empty: no write to `filing` or `holding`; the
  quarantine partition is rewritten with the same rows.
- **Any order (AC8).** With complete Bronze, a run of W never touches an accession A owned by
  X ≠ W: A is not owned by W, and its Silver row says X once X has run. So the final state is
  "for each accession, the good rows of its owner", a function of Bronze only. Take-overs are
  plain updates: when X later than W owns A, X's run rewrites A's rows from `_dataset = W` to X.
  So when the next zip repeats an accession Silver already has, the earlier data set needs no
  rerun.

### Quarantine

- **Rows.** Filing-level failures give one row with `source_table = 'submission'` and the
  submission's `_source_line`; `raw_row = to_json(struct(submission, coverpage, summarypage))`,
  each a struct of the original Bronze strings, so the filing can be replayed without the zip.
  Holding failures give `source_table = 'infotable'` and its own `raw_row`. `reason_codes` is
  `array_compact(array(when(rule, 'CODE'), ...))`, every failed rule in a fixed order;
  `reason_detail` names the failing columns and values.
- **Cascade.** A quarantined filing sends all its owned holding lines to quarantine as
  `NO_FILING`, so no holding ever lacks a value unit.
- **Rerun.** One `replaceWhere _dataset = '<key>'` writes all of X's quarantine rows (filing and
  holding together) in one commit; an empty frame empties the partition (tested), so a fixed row
  leaves quarantine.
- **Threshold (SDQ6, Q4 B).** Per table, `quarantined / (Bronze rows of X - superseded)`: warn
  above 0, block above `0.01`. Per-data-set overrides live in code
  (`QUARANTINE_BLOCK_RATIO_OVERRIDES = {"<key>": 0.03}`, reviewed in a PR), so the one-command
  backfill (AC4) never needs a manual step. The block message lists the top three reason codes.

### Types and the value unit

- Integers are checked with a regex before casting (`^[0-9]{1,16}$` for amounts, `{1,18}` for
  `infotable_sk`), because with ANSI mode off (Spark 3.5's default) an overflowing cast returns
  null silently (tested). `to_date(c, 'dd-MMM-yyyy')` parses `31-MAY-2013` and gives null for
  `31-FEB-2013` (tested); null on a required date is `BAD_DATE`.
- `value_unit` is computed once, on the filing row, from `filing_date` (before `2023-01-03` is
  `THOUSANDS`), and copied to holdings by the join in step 2. `value_usd = cast(value_reported *
  1000 as decimal(20,0))`: Spark types the product as `decimal(25,0)`, so the cast back is
  explicit; 16 digits times 1000 fits in 20.

### Concurrency

Both Silver assets use the `delta_writes` pool with Bronze, so across all runs one step at a time
writes any Delta table (`dagster.yaml`, limit 1, two runs at once). Readers see the last
committed version of each table, so reading Bronze or Silver during another step is safe; the
guard in step 0 covers a Bronze load that failed in the middle.

## Consequences

- More code than a plain overwrite: owner map, scope, change set and a three-clause `MERGE`.
  Tests need hand-made fixtures for a take-over, a re-land, a period change, a rerun with no
  change, and both run orders of a pair of data sets that share an accession (AC8).
- Silver and Bronze writes never overlap, so a Silver backfill and a Bronze load queue behind
  each other. A second pool for Silver is the lever if that hurts.
- The per-run SDQ2 on holdings covers the periods in `P_X` (all copies of a key share an
  accession, hence a period); full-table uniqueness on holdings is checked at acceptance (AC13).
- Many small `MERGE`s leave small files; `OPTIMIZE` is deferred work (spec deferred 7).

## Risks

- **Late amendments spread a data set over many periods.** Each period in `P_X` is read in full
  to build the change set and by the `MERGE` (a quarter is a few million holding rows,
  **unverified**). Shows up as per-partition runtime (AC18). S1 counts distinct periods per data
  set from Bronze SUBMISSION; if typical counts are high, revisit Q1.
- **A re-landed data set drops an accession that an earlier data set also holds.** X's run deletes
  it, and it is missing until the earlier data set reruns. Shows up as "handed back" in the run
  metadata; the runbook says to rerun the data sets listed there.
- **Stale quarantine after a take-over.** The earlier data set's quarantine rows for that accession
  stay until it reruns; SDQ3 for that data set then counts them twice. Only happens when Bronze
  grows after Silver ran; the take-over metadata names the data sets to rerun.
- **A missing pool limit** (fresh dev instance) lets two `MERGE`s race. Same as ADR-002: Compose
  carries `docker/dagster.yaml`, and the runbook checks it before a backfill.

## Open questions (for Checkpoint 1)

1. **Ownership per accession, not per key.** The spec's "Duplicates" section applies "latest wins"
   per key, so for holdings per `(accession_number, infotable_sk)`. Recommendation: per
   accession, so a filing's lines never mix two copies. AC5's "superseded duplicate rows" then
   means "rows of X whose accession is owned by a later data set".
2. **Threshold overrides "through config".** Run config in the launchpad would break AC4's "no
   manual step". Recommendation: the overrides map in code, as above.
3. **Orphan COVERPAGE and SUMMARYPAGE rows** (no SUBMISSION row in the same data set, Bronze DQ6).
   The spec defines `NO_FILING` for holdings only, so these would vanish silently.
   Recommendation: quarantine them as `NO_FILING` with their own `source_table`.
4. **SDQ2 "across the whole table" on every run.** On holdings that is a 200 M row shuffle per
   data set. Recommendation: per run within `P_X`, whole table in AC13.
