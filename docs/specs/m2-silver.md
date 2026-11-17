# M2 Silver: spec

- Status: Draft, awaiting Checkpoint 1
- Date: 2026-11-17
- Build window: Tue 2026-11-17 to Sun 2027-01-10, about 8 calendar weeks, ~40 h (6 h/week, with a
  light holiday dip from Dec 21 to Jan 3). Roadmap deadline for M2 is still 2027-01-31, which is
  about 3 weeks (~18 h) of float behind the build window.
- Builds on: `docs/specs/m1-bronze-ingestion.md`, ADR-002, ADR-003.

## Goal

Turn the seven all-string Bronze tables into typed, cleaned, deduplicated Silver tables that a
Gold query can trust: explicit schemas, dates and numbers cast, `value` converted to dollars by
filing date, 13F amendments resolved into an "effective report" per filer and period, and every
row that fails a cast or a rule parked in a quarantine table with its reason. M2 also closes with
the first formal dev to UAT promotion and an acceptance run on UAT against this spec.

## In scope

1. Silver tables built only from Bronze (no new external source): `silver.form13f_filing`,
   `silver.form13f_holding`, `silver.form13f_quarantine`, and (Q2 recommendation)
   `silver.form13f_filing_effective`. Which other Bronze tables get a Silver version is Q5.
2. Declared Silver schemas and explicit type casting (dates, integers, booleans, decimals).
3. Quarantine of rows that fail casting or rules, with reason codes and the original row.
4. Amendment handling: 13F-HR/A and 13F-NT/A, `RESTATEMENT` vs `NEW HOLDINGS`.
5. Value-unit normalization by filing date (thousands before 2023-01-03, dollars from then on).
6. Deduplication of keys that appear in more than one data set (M1 DQ7).
7. Delta `MERGE` upserts on declared keys, re-runnable per Bronze data set.
8. Dagster asset checks on every new table (row count, key uniqueness, not-null keys, plus the
   rules below).
9. Unit tests on small real fixtures plus small hand-made edge-case fixtures.
10. Silver runbook, one walkthrough in `docs/guides/` per feature PR, UAT promotion, acceptance
    report `docs/qa/m2-acceptance.md`, retro lines.

## Out of scope

- Source 2 (EDGAR full-index and XML, backlog #5) and the new-data-set sensor (#6): see Q3.
  Silver keys use `accession_number`, which Source 2 filings also have, so they can join later.
- Detecting republished zips (#7). Silver only needs a documented "re-run Silver after a Bronze
  re-land" step.
- Manager and security entity resolution (M3): no CUSIP-to-security mapping, no manager name
  matching. `cusip` is only trimmed, upper-cased and format-checked.
- Gold: per-quarter aggregates, holding-level effective view (unless Q2 chooses otherwise),
  serving.
- Parsing the `othermanager` reference list in INFOTABLE into rows.
- Price data or any second source to validate `value`.

## Sources

No new external source and no SEC request. Input is the seven M1 Bronze Delta tables
(`bronze.form13f_*`, partitioned by `_dataset`, all columns `string`, empty fields are `""`, not
null; ADR-002). A Silver run must succeed with `SEC_USER_AGENT` unset (AC2).

What was checked on 2026-11-17: the SEC readme (<https://www.sec.gov/files/form_13f_readme.pdf>,
fetched), the `FORM13F_metadata.json` shipped in the zips, and the three fixtures in
`tests/fixtures/form13f` (`2013q2`, `2022q4`, `2026-06_2026-08`). Everything else is marked
**unverified** and is measured in slice S1 or S4 once the real Bronze data exists.

| Fact | Status |
|---|---|
| Line key of INFOTABLE is `(accession_number, infotable_sk)`; `infotable_sk` is `NUMBER(38)`, not null | Verified (readme and metadata JSON). Fixture values are unique and grow over time (23.8 M in 2013, 130 M in 2026), which suggests a global sequence. Global uniqueness across data sets is **unverified** |
| `amendmenttype` text values are `RESTATEMENT` and `NEW HOLDINGS`; empty for non-amendments | Seen in fixtures (3 x `RESTATEMENT`, 1 x `NEW HOLDINGS` in `2013q2`; 1 x `RESTATEMENT` in `2022q4`). The readme only says "a restatement or adds new holdings entries" and does not list values, so the complete domain over 54 data sets is **unverified** |
| `isamendment` is `Y`, `N` or empty; `amendmentno` is empty for non-amendments, `1` or `2` seen | Verified in fixtures. Originals show both `N` and empty, so empty means "not an amendment" |
| Report types: `13F HOLDINGS REPORT`, `13F NOTICE`, `13F COMBINATION REPORT` | Seen in fixtures; full domain **unverified** |
| A `13F-NT/A` can carry `amendmenttype = NEW HOLDINGS` with report type `13F NOTICE` and no INFOTABLE rows (`0000950123-13-003781`, period 30-SEP-2012, filed 29-MAY-2013) | Seen in `2013q2` fixture. Meaning **unverified** (see Q7) |
| `value` unit: "Starting on January 3, 2023, market value is reported rounded to the nearest dollar. Previously, market value was reported in thousands." | Verified in the readme. The readme does not say filing date or period; "by filing date" is the M1 spec's reading and stays an assumption until AC9 checks a real late amendment |
| Fixture sanity of the unit rule: `2022q4` AMD 100,000 shares, value 6,336 (thousands, about $63/share); `2026-06_2026-08` Microsoft 14,219 shares, value 5,304,131 (dollars, about $373/share) | Verified in fixtures |
| `value` and share fields are at most 16 digits (`NUMBER(16)`) | Verified (metadata JSON). Thousands x 1000 can exceed `bigint`, so dollars need `decimal(20,0)` |
| `cik` is a 10-character zero-padded string (`0000823621`) | Seen in fixtures. Whether shorter CIKs exist is **unverified** |
| `sshprnamttype` is `SH` or `PRN`; `putcall` is `Put`, `Call` or empty; `investmentdiscretion` is `SOLE`, `DFND` or `OTR` | Seen in fixtures; full domains **unverified** |
| INFOTABLE `othermanager` is free text: seen `""`, `None` (literal), `0`, `1`, `2`, `01`, `4`, `1,2` | Seen in fixtures. Kept as a raw string in M2 |
| Text fields can carry trailing spaces (`AERCAP HOLDINGS NV `) and non-US state or country codes (`P7`, `K3`) and zip codes (`1000 BV`, `NA`) | Seen in fixtures |
| Dates are `DD-MON-YYYY` with upper-case month (`31-MAY-2013`) | Verified in fixtures |
| `tablevaluetotal` follows the same unit rule as `value` | Looks right in fixtures (2026 totals are in the hundreds of millions to billions) but **unverified**. The fixtures keep only 20 INFOTABLE lines per filing, so sum(value) cannot be reconciled on them |
| Rows per data set | Bronze INFOTABLE is expected in the low millions per data set (about 150 to 200 M rows over 54 sets), **unverified** until the M1 backfill reports row counts |

## Partitioning and refresh (contract; mechanism is for ADR-005)

- One Silver Dagster partition = one Bronze `_dataset` (same keys, same order as
  `FORM13F_PARTITIONS`), so lineage Bronze to Silver is 1:1 and a Silver re-run is as small as a
  Bronze re-run.
- Every Silver row records the Bronze data set it came from in `_dataset`.
- Physical Delta partitioning of Silver tables is Q1.
- Refresh is Delta `MERGE` on the declared keys, scoped to the data set being processed. After a
  Silver run for data set X, Silver rows whose `_dataset` is X equal exactly the cleaned Bronze rows
  of X, and no row of any other data set changes (AC7). The result must not depend on the order in
  which data sets were processed (AC8).
- Silver tables have a single writer at a time, same constraint as ADR-002 (Delta on S3).
- If a Bronze data set is re-landed (runbook, Q6 of M1), Silver for that data set is re-run by hand;
  the runbook says so.

## Tables and assets

Common to all Silver tables:

| Column | Type | Meaning |
|---|---|---|
| `_dataset` | string | Bronze data set that supplied this row (the latest one that holds the key, see "Duplicates") |
| `_source_line` | long | `_source_line` of the Bronze row (the submission row for `filing`) |
| `_silver_processed_at` | timestamp (UTC) | When the row was last written or changed |
| `_silver_run_id` | string | Dagster run id of that write |

Cleaning rules for text columns (all tables):

- `""` becomes `null` for optional columns; leading and trailing whitespace is trimmed.
- Names, addresses and free text keep their case and content otherwise (name matching is M3).
- Key and code columns are trimmed; `cusip`, `submission_type`, `amendment_type`, `report_type`,
  `sshprnamttype`, `putcall`, `investmentdiscretion` are upper-cased.
- Dates parse from `DD-MON-YYYY`. Integers must match `^[0-9]+$`. `Y`/`N` become boolean, anything
  else becomes null and is counted by SDQ10.

### `silver.form13f_filing`

- Grain: one row per filing (`accession_number`).
- Key: `accession_number`. Never null, unique across the whole table.
- Built from: `bronze.form13f_submission` (drives the row), `bronze.form13f_coverpage` and
  `bronze.form13f_summarypage` (left joined on `accession_number` within the same `_dataset`;
  notices have no summary page, so its columns are null for them).
- Refresh: `MERGE` on `accession_number`.

| Column | Type | Source and rule |
|---|---|---|
| `accession_number` | string | submission |
| `cik` | string | submission; 10 digits, zero-padded, never cast to a number |
| `submission_type` | string | submission; `13F-HR`, `13F-HR/A`, `13F-NT`, `13F-NT/A` (set **unverified**, SDQ3 checks it) |
| `filing_date` | date | submission; required |
| `period_of_report` | date | submission; required |
| `report_calendar_or_quarter` | date | coverpage `reportcalendarorquarter` |
| `report_type` | string | coverpage `reporttype` |
| `is_amendment` | boolean | true when `submission_type` ends with `/A` (not taken from `isamendment`, which is blank on many originals) |
| `amendment_no` | int | coverpage; null when not an amendment |
| `amendment_type` | string | coverpage; `RESTATEMENT`, `NEW HOLDINGS`, or null. Any other value is stored as null, flagged, and counted by SDQ10 (the filing is not quarantined) |
| `conf_denied_expired` | boolean | coverpage `confdeniedexpired` |
| `date_denied_expired` | date | coverpage; optional |
| `date_reported` | date | coverpage; optional |
| `reason_for_non_confidentiality` | string | coverpage |
| `is_confidential_omitted` | boolean | summarypage `isconfidentialomitted` (empty in many real rows, so null is normal) |
| `filing_manager_name` | string | coverpage `filingmanager_name` |
| `filing_manager_street1`, `_street2`, `_city`, `_state_or_country`, `_zip_code` | string | coverpage; codes and zips kept as text |
| `form13f_file_number`, `crd_number`, `sec_file_number` | string | coverpage |
| `provide_info_for_instruction5` | boolean | coverpage |
| `additional_information` | string | coverpage, up to 4000 chars |
| `other_included_managers_count`, `table_entry_total` | int | summarypage |
| `table_value_total_reported` | decimal(20,0) | summarypage `tablevaluetotal`, in the filing's own unit |
| `table_value_total_usd` | decimal(20,0) | same, in dollars (unit rule below) |
| `value_unit` | string | `THOUSANDS` if `filing_date` < 2023-01-03, else `DOLLARS` |

### `silver.form13f_holding`

- Grain: one row per holding line of a filing.
- Key: `(accession_number, infotable_sk)`. Never null, unique across the whole table.
- Built from: `bronze.form13f_infotable` joined to `silver.form13f_filing` for the filing fields.
  A holding line whose filing is not in Silver (orphan, or the filing was quarantined) is
  quarantined with `NO_FILING`.
- Refresh: `MERGE` on the key.

| Column | Type | Source and rule |
|---|---|---|
| `accession_number` | string | infotable |
| `infotable_sk` | long | infotable; overflow or non-digit goes to quarantine |
| `cik`, `filing_date`, `period_of_report`, `submission_type`, `amendment_type` | as in `filing` | copied from `silver.form13f_filing`, so Gold and the effective logic need no join for the common filters |
| `issuer_name` | string | `nameofissuer`, trimmed (real data has trailing spaces) |
| `title_of_class` | string | `titleofclass` |
| `cusip` | string | upper-cased; 9 alphanumeric characters expected (`N00985106` is valid). A different shape is kept and flagged, see `cusip_format_ok` |
| `cusip_format_ok` | boolean | true when `cusip` is 9 alphanumeric characters |
| `figi` | string | optional |
| `value_reported` | decimal(20,0) | `value` as filed, in the filing's own unit |
| `value_unit` | string | `THOUSANDS` or `DOLLARS`, from `filing_date` |
| `value_usd` | decimal(20,0) | `value_reported` x 1000 if `THOUSANDS`, else x 1 |
| `shares_or_principal` | decimal(20,0) | `sshprnamt` (up to 16 digits) |
| `shares_or_principal_type` | string | `SH` (shares) or `PRN` (principal amount in dollars); the type is kept because the two are not comparable |
| `put_call` | string | `PUT`, `CALL` or null |
| `investment_discretion` | string | `SOLE`, `DFND`, `OTR`, other values kept |
| `other_manager_refs_raw` | string | `othermanager` untouched apart from trimming and `""` to null (`None`, `1,2`, `01` stay as they are) |
| `voting_auth_sole`, `voting_auth_shared`, `voting_auth_none` | decimal(20,0) | voting authority share counts |

### `silver.form13f_filing_effective` (Q2 recommendation, option B)

- Grain: one row per filing, saying how that filing takes part in the effective report of its
  `(cik, period_of_report)`. Small table (one row per filing, hundreds of thousands in total).
- Key: `accession_number`.
- Refresh: fully rebuilt from `silver.form13f_filing` after each Silver run (small, so a full
  overwrite is cheap and idempotent). It is not partitioned by data set, because the filings of
  one `(cik, period_of_report)` come from several data sets.

| Column | Type | Meaning |
|---|---|---|
| `accession_number` | string | key |
| `cik`, `period_of_report`, `filing_date`, `submission_type`, `amendment_no`, `amendment_type` | as in `filing` | copied |
| `effective_role` | string | `BASE`, `ADDITION`, `SUPERSEDED`, `NOT_HOLDINGS`, `UNCLASSIFIED` |
| `base_accession_number` | string | for `ADDITION` and `BASE`, the accession of the BASE of the set; null otherwise |
| `superseded_by_accession_number` | string | for `SUPERSEDED`, the filing that replaced it |
| `base_missing` | boolean | true for an `ADDITION` whose set has no BASE in Silver (e.g. the original predates 2013 Q2) |

Resolution rule (the contract; ADR-004 owns edge details). Within one `(cik, period_of_report)`,
consider holdings-bearing filings, meaning `submission_type` in `13F-HR`, `13F-HR/A`, ordered by
`(filing_date, amendment_no, accession_number)`:

1. The **BASE** is the latest filing that is an original `13F-HR` or a `RESTATEMENT` amendment. A
   restatement replaces the whole earlier report.
2. Every `NEW HOLDINGS` amendment ordered after the BASE is an **ADDITION**: its holdings are added
   on top of the BASE.
3. Every other holdings-bearing filing (earlier originals, earlier restatements, `NEW HOLDINGS`
   amendments ordered before the BASE) is **SUPERSEDED**, by the BASE.
4. Notices (`13F-NT`, `13F-NT/A`) have no holdings: role `NOT_HOLDINGS`. Whether a notice
   restatement can empty an earlier report is Q7.
5. A `13F-HR/A` with no usable `amendment_type` is `UNCLASSIFIED` and takes no part in the set.

The effective holdings of a filer and period are then the holdings of the BASE plus the holdings
of its ADDITIONs. Building that holding-level view is Gold unless Q2 says otherwise.

### `silver.form13f_quarantine`

- Grain: one row per rejected Bronze row.
- Key: `(source_table, _dataset, _source_line)`. Not null, unique.
- Partitioned by `_dataset`. Refresh: `replaceWhere _dataset = '<key>'`, so a re-run replaces that
  data set's quarantine and a fixed row leaves it.
- Columns: `source_table` (`submission`, `infotable`, ...), `_dataset`, `_source_line`,
  `accession_number`, `reason_codes` (array of string, never empty), `reason_detail` (string),
  `raw_row` (string, JSON of every original Bronze column as a string, so the row can be replayed
  without going back to the zip), `_silver_processed_at`, `_silver_run_id`.

| Reason code | Applies to | Rule |
|---|---|---|
| `BAD_DATE` | filing | `filing_date`, `period_of_report` or `report_calendar_or_quarter` empty, not `DD-MON-YYYY`, or not a real date. Optional dates that are bad become null and are counted by SDQ10 instead |
| `BAD_PERIOD` | filing | `period_of_report` is not a calendar quarter end, or is after `filing_date`. Provisional: confirm on real data in S1; if it hits more than 0.1% of filings it becomes a warning |
| `BAD_SUBMISSION_TYPE` | filing | not in the declared set. Provisional, same S1 check |
| `BAD_INTEGER` | filing, holding | `infotable_sk`, `value`, `sshprnamt`, voting fields, `table*` totals or `amendmentno` is not a non-negative integer, or overflows its Silver type |
| `NO_FILING` | holding | no filing row in Silver for the `accession_number` (child row without a submission, or the filing was quarantined) |

A filing that is quarantined takes all its holding lines with it as `NO_FILING`, so a bad filing
date can never leave holdings without a unit. A row in more than one data set is evaluated once
(see "Duplicates"); an older copy that a later data set replaces is never quarantined.

Other Bronze tables that get a Silver version (Q5) reuse the same reason codes.

## Value-unit normalization

- `value_unit = THOUSANDS` when `filing_date < 2023-01-03`, else `DOLLARS`. Always by **filing
  date from SUBMISSION**, never by period of report or by data set: an amendment filed in 2024 for
  2022 Q3 is in dollars, and a `2023q1` data set can in principle hold both units.
- `value_usd = value_reported x 1000` for `THOUSANDS`, `x 1` for `DOLLARS`. Same rule for
  `table_value_total_usd`.
- The as-filed number is always kept in `value_reported`, so the conversion can be audited and
  redone.
- Whether any filer used dollars before 2023-01-03 (early adopters) is **unverified**. SDQ9 looks
  for it; a hit changes the rule and is reported to Thanh, not patched silently.

## Duplicates (M1 DQ7)

Bronze keys are unique inside one `_dataset` (M1 DQ4), but the same accession number can sit in
two data sets (late-day filings around a window edge, or a later re-publication). The number of
such accessions is not known yet: M1 DQ7 counts them on UAT.

- Silver identity is the key, not the key plus data set. One key gives one Silver row.
- Default policy (Q6): the copy from the **latest** `_dataset` wins, and `_dataset` records it.
  Identical copies collapse silently; copies with different content are counted by SDQ11.
- The result must be the same whichever data set was processed first (AC8).

## Data quality rules (Dagster asset checks)

Checks run for the data set just processed and are **block** (fail the asset, nothing newer is
published) or **warn** (record a number, run continues), as in M1. Every new table gets the
standard three: row count > 0, key uniqueness, not-null keys. The quarantine table may be empty,
so its row count is warn-level.

| ID | Rule | Tables | Level |
|---|---|---|---|
| SDQ1 | Row count > 0 for the data set | filing, holding, filing_effective (whole table) | block |
| SDQ2 | Key not null and not empty; key unique across the whole table, not only the data set | filing, holding, filing_effective, quarantine | block |
| SDQ3 | Conservation: Bronze rows of the data set = Silver rows + quarantine rows + superseded duplicate rows (AC5) | filing, holding | block |
| SDQ4 | Declared types hold; `filing_date` is between 2013-05-01 and the run date; `period_of_report <= filing_date` | filing | block |
| SDQ5 | `value_unit` matches the filing-date rule; `value_usd = value_reported x (1000 or 1)` | holding, filing | block |
| SDQ6 | Quarantine rate for the data set: warn above 0 rows, block above the threshold chosen in Q4 | filing, holding | warn, then block |
| SDQ7 | Referential: every holding has a filing; every filing has a Bronze submission | holding, filing | block |
| SDQ8 | Effective set: each `(cik, period_of_report)` with a holdings-bearing filing has exactly one `BASE`; every `ADDITION` is ordered after its BASE; `base_missing` count recorded | filing_effective | block, count is warn |
| SDQ9 | Unit sanity: for `SH` lines with `shares_or_principal > 0`, the median of `value_usd / shares_or_principal` per data set is between 1 and 1000 dollars; early adopters (`DOLLARS`-sized values with a pre-2023 filing date) counted | holding | warn |
| SDQ10 | Counts of unrecognised flags (not Y/N/empty), unknown `amendment_type` on a `/A` filing, an `amendment_type` set on a non-amendment, bad optional dates | filing | warn |
| SDQ11 | Duplicate keys across data sets whose content differs | filing, holding | warn |
| SDQ12 | Per filing, `sum(value_reported)` of its holdings vs `table_value_total_reported`, and holding count vs `table_entry_total`, for filings with no quarantined lines and `is_confidential_omitted` not `Y` | filing | warn |
| SDQ13 | `cusip_format_ok = false` count | holding | warn |

Thresholds in SDQ9 and SDQ12 are first guesses and are tuned on real data in S4.

## Acceptance criteria (checked on UAT by `test-engineer`)

Table names below are logical; on UAT they are path-based Delta tables under
`LAKEHOUSE_ROOT/silver/` and `.../bronze/`, queried with Spark SQL. Every UAT command needs
Thanh's explicit OK for that step (CLAUDE.md homelab rules).

- **AC1** Prerequisite: M1 is signed off and `SELECT count(DISTINCT _dataset) FROM
  bronze.form13f_submission` equals the number of keys in `FORM13F_PARTITIONS` (54 on 2026-11-06,
  more if a new data set was added).
- **AC2** Config and network: a Silver run for one data set succeeds with `SEC_USER_AGENT` unset
  and makes no HTTP request.
- **AC3** Partitions: the Silver assets list the same partition keys, in the same order, as the
  Bronze assets. Check: one `python -I -c` that compares the two partition definitions.
- **AC4** One-command backfill: from empty Silver tables, the single command in
  `docs/runbooks/form13f-silver.md` materializes every partition of every Silver table with no
  manual step, and every block-level check passes for every partition. Warn-level counts are listed
  in `docs/qa/m2-acceptance.md`.
- **AC5** Conservation: for every `_dataset` and for both `filing` and `holding`, Bronze rows =
  Silver rows + quarantine rows + superseded duplicate rows (rows of that data set whose key also
  exists in a later `_dataset`; keys sort in time order, ADR-002). Check, for holdings, must return
  0 rows:
  `SELECT b._dataset FROM (bronze rows per _dataset) b LEFT JOIN (silver rows per _dataset) s
  LEFT JOIN (quarantine rows per _dataset, source_table = 'infotable') q LEFT JOIN (superseded
  rows per _dataset) d WHERE b.n <> coalesce(s.n,0) + coalesce(q.n,0) + coalesce(d.n,0)`.
  The full query lives in the acceptance report.
- **AC6** Schema: `DESCRIBE` of each Silver table matches the declared schema. No date, count or
  amount column is `string`; `value_usd` is `decimal(20,0)`.
- **AC7** Idempotent rerun: re-materializing `2024-03_2024-05` leaves, for every Silver table, the
  total row count, the row count per `_dataset` and a content hash unchanged
  (`SELECT _dataset, count(*), sum(xxhash64(<all business columns>)) FROM <table> GROUP BY 1`
  before and after). Rows of every other `_dataset` keep their `_silver_run_id` and
  `_silver_processed_at`. Zero HTTP requests.
- **AC8** Order independence: on a scratch `LAKEHOUSE_ROOT`, processing `2022q4` then `2023q1`
  gives the same content hash for all Silver tables as `2023q1` then `2022q4` (also covered by a
  unit test on the fixtures plus one hand-made data set pair that shares an accession).
- **AC9** Value unit: `SELECT value_unit, min(filing_date), max(filing_date) FROM
  silver.form13f_filing GROUP BY 1` shows `THOUSANDS` max below 2023-01-03 and `DOLLARS` min on or
  after it. At least one `13F-HR/A` with `period_of_report <= 2022-09-30` and `filing_date >=
  2023-01-03` exists, is `DOLLARS`, and its holdings pass SDQ9; its accession number is written in
  the acceptance report. If no such filing exists in the data, the report says so and AC9 relies
  on the unit test instead.
- **AC10** Unit sanity at the boundary: `percentile_approx(value_usd / shares_or_principal, 0.5)`
  over `SH` lines with `shares_or_principal > 0` is between 1 and 1000 for each of `2022q4` and
  `2023q1`. A median near 0.001 or 1,000,000 means the unit rule is wrong.
- **AC11** Amendments: `SELECT cik, period_of_report FROM silver.form13f_filing_effective WHERE
  effective_role = 'BASE' GROUP BY 1,2 HAVING count(*) > 1` returns 0 rows, and every
  `(cik, period_of_report)` with a `13F-HR` or `13F-HR/A` has one `BASE`. Examples checked by
  accession and recorded in the report: the RESTATEMENT `0001085146-22-003667` (filed 2022-10-31,
  period 2022-06-30, `2022q4` fixture) is `BASE`, and the original `13F-HR` of the same CIK and
  period, if it is in Bronze, is `SUPERSEDED` by it; at least one real `NEW HOLDINGS` `13F-HR/A`
  is `ADDITION` (accession chosen in S3). Row counts per `effective_role` are in the report.
- **AC12** Quarantine: no row has an empty `reason_codes` or null `raw_row`; a unit test has one
  fixture row per reason code that lands in quarantine with that code; the report lists quarantine
  counts per reason code and per data set, and no data set was blocked by SDQ6 (or each blocked
  one is explained).
- **AC13** Keys: for `filing`, `holding`, `filing_effective` and `quarantine`, the not-null and
  uniqueness queries on the declared keys return 0 offending rows across the whole table.
- **AC14** Duplicates: `SELECT accession_number FROM silver.form13f_filing GROUP BY 1 HAVING
  count(*) > 1` returns 0 rows; the report states how many accession numbers were in more than one
  Bronze data set (M1 DQ7) and that, for each, `_dataset` equals the latest data set holding it.
- **AC15** Lossless: for 3 sampled data sets (`2013q2`, `2022q4`, latest), 20 random holding rows
  matched by `(accession_number, infotable_sk)` equal the Bronze row after the documented cleaning:
  `value_reported`, `shares_or_principal` and voting fields equal the Bronze strings as numbers,
  and `issuer_name` equals the Bronze value trimmed.
- **AC16** Asset checks: for each Silver table, `dagster definitions validate` passes and a test
  asserts that the asset has checks for row count, key uniqueness and not-null keys (plus the SDQ
  rules above), with the blocking flags in the table above.
- **AC17** Dev: on ThanhPC, the same command for a 4-partition range spanning the unit change
  (`2022q3` to `2023q2`) completes using local storage.
- **AC18** Record keeping: `docs/qa/m2-acceptance.md` lists per partition the rows in, rows out,
  quarantined rows and runtime, the total backfill wall-clock time, the warn-level counts, and the
  quarantine reason histogram.
- **AC19** Docs: each feature PR added or updated a walkthrough in `docs/guides/`
  (`ls docs/guides` shows one per slice that added a feature; each has the Mermaid diagrams it
  needs); `docs/runbooks/form13f-silver.md` has run one partition, full backfill and re-run
  after a Bronze re-land.
- **AC20** Promotion: the acceptance run happened on an image deployed to UAT from `main`
  through `docs/runbooks/uat-deploy.md`. The report records the commit SHA and image tag
  (`docker compose ps` on UAT shows the same tag), the previous tag as the rollback target, and
  that `devops-engineer` ran the pre-promotion checklist (and `security-auditor` if secrets,
  Compose, ports or the tunnel changed). Silver is fully rebuildable from Bronze, so rollback
  needs no data restore: dropping a Silver partition's rows and re-running it reproduces the AC7
  hash.
- **AC21** `make check` and CI are green; tests use small real fixtures in `tests/fixtures/`
  (the three existing samples) and hand-made edge-case fixtures marked as such in the fixtures
  README.

## Edge cases

- **Amendments.** `RESTATEMENT` replaces the full earlier report; `NEW HOLDINGS` adds entries.
  Both are filed as their own accession numbers, possibly years after the period, in whichever data
  set covers their filing date. A `NEW HOLDINGS` amendment ordered before a later restatement is
  superseded by it. Several amendments on the same filing date are ordered by `amendment_no`, then
  `accession_number`; whether `amendment_no` is reliable on real data is **unverified** (S3
  profiles it). An amendment whose original is not in Bronze (original predates 2013 Q2, or lies
  outside the 54 data sets) gives a BASE (restatement) or `base_missing = true` (new holdings), never
  an error.
- **Notice amendments.** `13F-NT/A` with `NEW HOLDINGS` exists in the `2013q2` fixture. It has no
  INFOTABLE rows, so it cannot add holdings; role `NOT_HOLDINGS`.
- **Value unit change.** By filing date, see above. `table_value_total` follows the same rule.
  Values are up to 16 digits, so x 1000 needs `decimal(20,0)`. A filing on the exact day
  2023-01-03 is `DOLLARS`; 2022-12-30 is `THOUSANDS` (boundary tests on both).
- **`isamendment` blank vs `N`.** Both mean not an amendment; `is_amendment` is derived from
  `submission_type`, and a mismatch with `isamendment` is counted by SDQ10.
- **Confidential treatment.** Rows are missing from INFOTABLE when confidential treatment applies;
  `is_confidential_omitted`, `conf_denied_expired`, `date_denied_expired`, `date_reported` and
  `reason_for_non_confidentiality` are kept so Gold can mark such filings incomplete. Silver does
  not guess the missing rows. A `Y` value is not seen in the fixtures (**unverified** in real data).
- **Notices and empty tables.** 13F-NT and "notice" reports have a filing row and no holdings;
  that is normal, not an orphan. A data set can have an empty Bronze table (`othermanager` in
  `2026-06_2026-08` has 0 rows); Silver handles empty input and SDQ1 applies only to filing and
  holding.
- **`PRN` vs `SH`.** `sshprnamt` is a share count for `SH` and a dollar principal for `PRN`
  (21 of 160 lines in the `2022q4` fixture). Never sum the two; SDQ9 uses `SH` only.
- **Combination reports.** `13F COMBINATION REPORT` filings include holdings of other managers.
  Two filers (a parent and a subsidiary) can report the same positions. Silver keeps both; fixing
  double counting across filers is for M3 and Gold.
- **Free text.** Trailing spaces in names, non-US state and zip codes, `None` as a literal in
  `othermanager` (2013). Cleaning trims and keeps; nothing is parsed.
- **Key reuse across data sets.** If `infotable_sk` turns out unique across data sets, the key
  stays `(accession_number, infotable_sk)` anyway (it is the documented primary key).
- **Bronze re-run after Silver.** A re-landed Bronze data set makes its Silver data set stale until
  re-run; rows that disappeared from Bronze disappear from Silver (the refresh makes Silver for that
  data set equal to the cleaned Bronze).
- **Bronze load half-done.** Bronze has no cross-table transaction (ADR-002). Silver for a data
  set starts only after the Bronze run for that data set succeeded.
- **Quarantine feedback loop.** A quarantined filing cascades to its holdings (`NO_FILING`). A
  systematic problem (for example a date format change) would quarantine a whole data set, which
  is what the SDQ6 threshold is for.

## Sizing (about 40 h gross, about 38 h net of M1 leftovers)

Thanh reports the full 54-partition UAT backfill, the acceptance run and the retro as done
(2026-11-17); only committing `docs/qa/m1-acceptance.md` and the retro and closing the milestone
remain, about 2 h. That leaves about 38 h for M2 inside the build window. Each slice includes its tests, walkthrough, agent reviews and PR overhead.

| Slice (one PR each, each with its walkthrough in `docs/guides/`) | Estimate | Target week |
|---|---|---|
| **S1** Silver foundation and `silver.form13f_filing`: declared schemas, casting and cleaning rules, quarantine table and reason codes, `MERGE` refresh, asset checks SDQ1 to SDQ4, SDQ6, SDQ7, SDQ10; profile the real Bronze domains (amendment types, submission types, quarter-end rule) and settle the provisional rules | 8 h | Nov 17 to Nov 30 |
| **S2** `silver.form13f_holding`: typing, `cusip` checks, value-unit normalization, cross-data-set duplicate rule, SDQ5, SDQ11, SDQ13, boundary and duplicate fixtures | 8 h | Dec 1 to Dec 13 |
| **S3** Amendments: `silver.form13f_filing_effective`, resolution rule, SDQ8, fixtures for restatement, new holdings, missing base, same-day tie | 5 h | Dec 14 to Dec 20 |
| **S4** Hardening: SDQ9 and SDQ12 reconciliation checks, order-independence test (AC8), dev run over `2022q3` to `2023q2` (AC17), threshold tuning, performance fixes found at scale | 4 h | holiday dip, Dec 21 to Jan 3 |
| **S5** Milestone close: UAT promotion, full Silver backfill on UAT, acceptance run (AC1 to AC21), `docs/qa/m2-acceptance.md`, runbook, README, retro | 6 h | Jan 4 to Jan 10 |
| **Core total** | **31 h** | |
| **S6 (stretch)** Typed Silver for the other-manager tables (Q5) | +3 h | only after S5, before Jan 31 |
| Source 2 (Q3, #5) | +10 to 12 h (M1 estimate) | does not fit |

Core fits with about 7 h of slack inside the build window. The roadmap
deadline of Jan 31 gives 3 weeks of float, so the milestone still closes on time. If it slips, cut
in this order: S6, then SDQ12 and the threshold tuning in S4, then the dev range run shrinks to 2
partitions. Do not cut quarantine, the unit rule or the effective table: they are the milestone.

## Open questions for Thanh (decide before design)

Each has options and a recommendation, so all can be accepted at once.

**Q1. How are Silver tables physically partitioned?**
- A. By `_dataset`, like Bronze. A Silver re-run maps to one partition. But the holdings of one
  quarter are spread over many data sets (late amendments), so a Gold query "holdings for 2024 Q4"
  reads all partitions, against the project rule to filter on the partition column. The same
  accession in two data sets lands in two partitions, so the duplicate rule needs a lookup across
  partitions.
- B. By `period_of_report` (the quarter). Gold queries by quarter prune. The same accession always
  has the same period, so duplicates fall into the same partition and `MERGE` handles them with
  partition pruning. A data set run touches several period partitions (mostly one or two, plus
  late amendments), so first loads write more small pieces. Bad periods must not create junk
  partitions, which `BAD_PERIOD` quarantine prevents.
- C. Unpartitioned. Simplest, but every read and every `MERGE` scans everything (~200 M holding
  rows).
- **Recommendation: B.** The Dagster partition stays one per Bronze data set (lineage and re-runs
  unchanged); only the Delta layout uses the period. `silver.form13f_quarantine` stays partitioned
  by `_dataset` because it is replaced per data set. ADR-005 confirms the mechanism.

**Q2. Where does "effective holdings" (latest restatement wins, NEW HOLDINGS added on top) live?**
- A. Gold only. Silver stores as-filed rows plus the amendment flags. Simplest for M2, but the
  amendment logic is not tested until M4 and every Gold consumer repeats it.
- B. Silver gets a small filing-level table, `silver.form13f_filing_effective` (as specified
  above), rebuilt in full each run. Gold builds the holding-level view with one join. Amendment
  handling, a named M2 goal, is built and tested here, and no holdings are duplicated.
- C. B plus a holding-level `silver.form13f_holding_effective` table. Easy for Gold, but copies a
  large share of the ~200 M holding rows and must be kept in step with every Silver re-run.
- **Recommendation: B.** Holding-level view is a Gold task (deferred issue).

**Q3. Does backlog #5 (EDGAR full-index and XML, Source 2) join M2?**
- A. No: keep it out. It is 10 to 12 h (M1 estimate), core M2 already uses all the net build
  window, and Source 2 needs its own Bronze tables and a different parser before Silver can use it.
- B. Yes, as an early slice. Closes the 1 to 3 month gap to today, but pushes core M2 past Jan 31.
- C. Only the raw landing and index table, no XML parsing (about 5 h). Still does not fit.
- **Recommendation: A.** Revisit after M2 sign-off, before M4 (Gold needs recent data only for
  the demo). Source 2 later adds rows with a new lineage value, not new Silver columns.

**Q4. How do quarantine thresholds behave: warn or block?**
- A. Warn only. Never stops a load, but a parser or format change could quarantine a whole data
  set without anyone noticing.
- B. Warn on any quarantined row; block the data set (nothing written) when quarantined rows exceed
  1% of that data set's rows in `filing` or `holding`. The block message lists the top reason
  codes. After review, Thanh can raise the threshold for one data set through config, recorded in
  the runbook.
- C. Block on any quarantined row. Safest, but one bad real row would stop the full backfill.
- **Recommendation: B**, with 1% as a first guess that S4 tunes on real data (the acceptance
  report records the real rates). A quarantine rate of 1% on holdings would be about 30,000 rows per
  data set, so the real numbers probably sit well below it.

**Q5. Which Bronze tables get a Silver version?**
- A. All seven, 1:1 typed (adds `othermanager`, `othermanager2`, `signature`, and keeps
  `submission`, `coverpage`, `summarypage` as three tables). About +7 h, mostly mechanical.
- B. Core subset: `filing` (submission + coverpage + summarypage joined, one row per filing),
  `holding` (infotable), quarantine, and the effective table. `othermanager`, `othermanager2` and
  `signature` stay Bronze only.
- C. B, then `othermanager` and `othermanager2` as a stretch slice S6 (+3 h). `signature` stays
  Bronze: nothing in M3 or M4 needs signatory names.
- **Recommendation: C.** M3 may want other-manager names as aliases and the `othermanager`
  reference in holdings points at `othermanager2` sequence numbers, so those two are worth typing
  if time remains. If S6 is not reached, it becomes an issue.

**Q6. What happens when the same key is in more than one data set?**
- A. The copy from the latest `_dataset` wins; identical copies collapse; differing copies are
  counted by SDQ11. Deterministic and order independent.
- B. The first copy wins. Stable once loaded, but a correction that arrives in a later data set is
  ignored.
- C. Quarantine every conflicting pair for review. Most conservative, but could quarantine a whole
  window edge if the duplicate count is large.
- **Recommendation: A.** Re-confirm after M1's DQ7 numbers exist; if DQ7 shows 0 shared accessions,
  the rule stays but is a pure safeguard.

**Q7. Can a notice amendment empty an earlier holdings report?**
A `13F-NT/A` with `RESTATEMENT` could mean "the holdings are no longer reported by this filer".
- A. Notices never change the effective set (role `NOT_HOLDINGS`); a warning counts `(cik, period)`
  pairs that have a restatement notice after a holdings report, for review.
- B. A restatement notice supersedes the earlier holdings, so that filer and period have no
  effective holdings.
- **Recommendation: A**, because the rule is unverified and silently dropping a manager's holdings
  is worse than keeping them. The warning count after the backfill decides if B is worth a change.

**Q8. When does M2 acceptance run, given M1 is not signed off yet?**
M2 acceptance needs the full Bronze backfill on UAT (AC1). Thanh reports the backfill done, but
the M1 acceptance report (`docs/qa/m1-acceptance.md`) is not in the repo yet and milestone M1 is
still open (2026-11-17).
- A. Build M2 on fixtures and dev now; run the M2 acceptance on UAT only after M1 is signed off
  (target: before Jan 4).
- B. Run M2 acceptance against whatever Bronze partitions exist at close.
- **Recommendation: A.** M1 must close first. B would make AC5 and AC14 meaningless, because
  duplicates and late amendments live in neighbouring data sets. If M1 closes after Jan 4, M2
  closes later by the same number of days (still before Jan 31 unless M1 slips past mid-January).

### Raised by the architect while writing ADR-004 and ADR-005

**Q9. SDQ8 and AC11 when a group has no base candidate.** Groups with only `base_missing`
additions or only unclassified amendments have no BASE, so "exactly one BASE" would fail them.
- **Recommendation:** "exactly one BASE where a base candidate exists"; other groups are a warn count.

**Q10. Where the Q7 warning lives.** It has no check number.
- **Recommendation:** record it as metadata on SDQ8.

**Q11. How quarantine thresholds are raised (Q4).** Run config would break AC4's "no manual step".
- **Recommendation:** a reviewed per-data-set overrides map in code.

**Q12. COVERPAGE or SUMMARYPAGE rows with no SUBMISSION row.** They would drop out silently.
- **Recommendation:** quarantine them with reason `NO_FILING`.

**Q13. A quarantined restatement makes the original the BASE without notice.**
- **Recommendation:** add a warn count for it in S3.

## Decisions

To be filled at Checkpoint 1.

## Candidate ADRs for the architect

1. **ADR-004: Amendment handling.** The resolution algorithm for the effective report (BASE,
   ADDITION, SUPERSEDED, `NOT_HOLDINGS`, `UNCLASSIFIED`), ordering and tie-breaks, missing base,
   notice amendments (Q7), what is stored where (Q2), how and when the effective table is rebuilt
   and how Gold will consume it, plus the unit tests it needs (the fixtures have a restatement in
   `2022q4` and a notice with new holdings in `2013q2`).
2. **ADR-005: Silver keys and write strategy.** `MERGE` on the declared keys vs `replaceWhere`
   (which tables use which), physical partitioning (Q1), how a data set run removes rows that left
   Bronze (for example `WHEN NOT MATCHED BY SOURCE`, to be tested on delta-spark 3.3.3),
   cross-data-set ownership of a key so results are order independent (Q6, AC8), avoiding rewrites
   of unchanged rows, quarantine design (where the split happens, reason codes, `raw_row`, the
   SDQ6 threshold in Q4), value-unit normalization placement, how the holding table gets its
   filing fields, Dagster asset shape (one multi-asset or several, pool `delta_writes` for the
   single writer, serialization with Bronze).

## Deferred (for the main session to file as issues)

1. Holding-level effective view or table in Gold (Q2 option B), layer `gold`, for M4.
2. Parse `othermanager` reference lists (`1,2`, `None`, `01`) and link to `othermanager2`
   sequence numbers, layer `silver` or `gold`, for M3 or M4.
3. `signature` (and `othermanager`/`othermanager2` if S6 is not reached) typed Silver tables.
4. Investigate `13F-NT/A` `RESTATEMENT` semantics once the Q7 warning count exists.
5. Double counting across filers in `13F COMBINATION REPORT` (parent and subsidiary), for M3 and
   M4.
6. CUSIP check-digit validation and CINS handling (letter-leading identifiers such as
   `N00985106`), for M3 entity resolution.
7. Delta maintenance of Silver tables (`OPTIMIZE`, small files after many `MERGE`s, `VACUUM`
   retention), tech-debt, `infra`.
8. Comment on #7 (republished zips): a detected re-land should mark the Silver data set stale or
   trigger its re-run; comment on #6 (sensor): a new data set should also chain the Silver run.
9. Backlog #5 (Source 2): update with the Q3 outcome; it still needs Bronze tables plus a Silver
   path.
10. Update `tests/fixtures/form13f/README.md` for the hand-made edge-case fixtures (part of S1
    to S3, not a separate issue unless it slips).
