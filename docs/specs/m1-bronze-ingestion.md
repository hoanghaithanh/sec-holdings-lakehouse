# M1 Bronze ingestion: spec

- Status: Approved at Checkpoint 1 (open questions decided by Thanh, PR #4 merged 2026-11-06)
- Date: 2026-11-06
- Build window: Tue 2026-11-10 to Sun 2026-12-20, about 6 weeks at 6 h/week (~36 h)

## Goal

Land every SEC Form 13F data set from 2013 Q2 to today, unchanged, in the object store, and load
each file into Bronze Delta tables with ingestion metadata. A full backfill runs from scratch with
one documented command, and re-running any partition gives the same result. M2 (Silver) builds
amendments, value-unit normalization and dedup on top of this, so Bronze must keep every source
field and change nothing.

## In scope

1. Source 1: the quarterly 13F data set zips, all of them, from `2013q2` to the latest published.
2. Raw landing: each zip stored once, unchanged, at a path keyed by its partition.
3. Seven Bronze Delta tables, one per TSV in the zip, plus ingestion metadata columns.
4. One Dagster partition per SEC data set (see Open question Q1 for the exact key).
5. An HTTP client that respects EDGAR fair access: at most 10 requests/s, a `User-Agent` taken
   from config, retries with backoff on 429/5xx and network errors.
6. Dagster asset checks on every Bronze table.
7. Full backfill on UAT, with row counts and runtime per partition recorded in the acceptance report.
8. A runbook: run one partition, run the full backfill, re-run a partition.

## Out of scope

- Typing, cleaning, dedup, amendment resolution, value-unit normalization (M2 Silver).
- Manager and security entity resolution (M3).
- Parsing filings outside 13F (13D/G, N-PORT and so on).
- Source 2 and the sensor, unless Thanh keeps them (Q2, Q5). Both are specified below so the
  decision is cheap either way.

## Sources

### Source 1: Form 13F data sets (verified on sec.gov, 2026-11-06)

- Page: <https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets>.
  Readme with table definitions: <https://www.sec.gov/files/form_13f_readme.pdf>.
- Coverage: from `2013q2_form13f.zip` (XML-era filings, May 2013 on) to
  `01jun2026-31aug2026_form13f.zip` today. 2013 Q1 and earlier do not exist as data sets.
- **Period cadence changed in 2024** (confirmed):
  - 2013 Q2 to 2023 Q4: calendar quarters, named `YYYYqN_form13f.zip` (43 files).
  - 2024 on: three-month ranges named `DDmonYYYY-DDmonYYYY_form13f.zip`: Mar-May, Jun-Aug,
    Sep-Nov, Dec-Feb. The transition file `01jan2024-29feb2024` covers only two months.
    Dec-Feb files cross a year boundary. 11 such files so far, so **54 data sets today**,
    plus one new one roughly every 3 months.
- The period is the filing-date window (filings after 5:30 pm ET on the last business day roll
  into the next set). The readme implies this but does not say it outright: treat as
  **likely, not confirmed**. A data set is therefore not "the holdings for quarter X": one set
  holds reports for many periods of report, including late amendments.
- Contents: up to 7 tab-delimited UTF-8 text files plus a W3C tabular-data metadata (JSON) file.
  Tables: SUBMISSION, COVERPAGE, OTHERMANAGER, OTHERMANAGER2, SIGNATURE, SUMMARYPAGE, INFOTABLE.
  Exact file names inside the zip are **unverified** (the readme does not list them; expect
  `<TABLE>.tsv`). Dates are text in `DD-MON-YYYY` format.
- Access: a handful of downloads per run, still through the rate-limited client.
- Sizes of the zips and row counts per set: **unverified**. INFOTABLE is expected to be millions
  of rows per set; the backfill measures the real numbers.

### Source 2: EDGAR full-index and information-table XML (deferred, Q2, #5)

- Index: `https://www.sec.gov/Archives/edgar/full-index/<YYYY>/QTR<n>/` with `form.idx`,
  `company.idx`, `master.idx` (pipe-delimited: CIK, company name, form type, date filed,
  file path). The directory fetch failed from this sandbox, so the file list and format are
  **unverified**.
- Raw filing: each 13F-HR filing has a primary document and an information-table XML. The XML
  file name varies per filing and must be found from the filing index; element names follow the
  13F XML technical specification (**not fetched, unverified**).
- Purpose: holdings for filings newer than the latest published data set (a 1 to 3 month gap).
- Access (verified on <https://www.sec.gov/os/accessing-edgar-data>): at most 10 requests/s, a
  `User-Agent` naming the organisation and an admin contact email. SEC throttles clients that
  ignore this. One 13F filing costs about 2 to 3 requests, and a filing window has several
  thousand 13F-HR filings, so one window is roughly 10 to 20 minutes at the limit.

## Partitioning (contract, key format decided in Q1)

- One partition = one SEC data set. Partition keys sort in time order and map 1:1 to a zip.
  Example: `2013q2` ... `2023q4`, `2024-01_2024-02`, `2024-03_2024-05`, ..., `2026-06_2026-08`.
- Each Bronze row carries `_dataset` (the partition key), so `replaceWhere _dataset = '<key>'`
  replaces exactly one partition and nothing else.
- Bronze tables are physically partitioned by `_dataset` so reads of one set prune.

## Tables and assets

### raw: `raw/form13f_datasets/<_dataset>/<original zip name>`

- Grain: one object per data set zip, stored byte for byte.
- Written once. If the object exists, the asset skips the download (no HTTP call) and reuses it.
  Never overwritten or deleted by the pipeline.
- Materialization metadata: source URL, size in bytes, SHA-256, download time.

### Bronze tables (7)

All seven share these rules:

- Columns: every source column, same name in lower case, **all stored as `string`**, exactly as in
  the file (no trimming, no date or number parsing). Typing is M2's job, so a malformed value
  can never fail or silently null out a Bronze load.
- The schema is declared in code. If a file's header does not match the declared columns
  (missing, extra or renamed), the partition **fails** with a clear message. No inference.
- Metadata columns on every row:

| Column | Type | Meaning |
|---|---|---|
| `_dataset` | string | Partition key, e.g. `2024-03_2024-05` |
| `_source_file` | string | Full raw path of the zip plus the member file, e.g. `.../INFOTABLE.tsv` |
| `_source_line` | long | Line number in the TSV, for tracing a row back to the file |
| `_ingested_at` | timestamp (UTC) | When this partition was loaded |
| `_run_id` | string | Dagster run id |

- Refresh: `replaceWhere _dataset = '<key>'`, append-only across partitions.

| Table | Grain (one row per ...) | Source key (from readme) | Columns kept for M2 |
|---|---|---|---|
| `bronze.form13f_submission` | filing | `accession_number` | `filing_date`, `submissiontype` (13F-HR, 13F-HR/A, 13F-NT, ...), `cik`, `periodofreport` |
| `bronze.form13f_coverpage` | filing | `accession_number` | `reportcalendarorquarter`, `isamendment`, `amendmentno`, `amendmenttype`, `confdeniedexpired`, `datedeniedexpired`, `datereported`, `reasonfornonconfidentiality`, `filingmanager_name` + address, `reporttype`, `form13ffilenumber`, `crdnumber`, `secfilenumber`, `provideinfoforinstruction5`, `additionalinformation` |
| `bronze.form13f_summarypage` | filing | `accession_number` | `otherincludedmanagerscount`, `tableentrytotal`, `tablevaluetotal`, `isconfidentialomitted` |
| `bronze.form13f_infotable` | holding line | `accession_number, infotable_sk` | `nameofissuer`, `titleofclass`, `cusip`, `figi`, `value`, `sshprnamt`, `sshprnamttype`, `putcall`, `investmentdiscretion`, `othermanager`, `voting_auth_sole`, `voting_auth_shared`, `voting_auth_none` |
| `bronze.form13f_othermanager` | other manager on the cover page | `accession_number, othermanager_sk` | `cik`, `form13ffilenumber`, `crdnumber`, `secfilenumber`, `name` |
| `bronze.form13f_othermanager2` | other included manager (summary page) | `accession_number, sequencenumber` (readme marks only `accession_number`; **unverified**) | `cik`, `form13ffilenumber`, `crdnumber`, `secfilenumber`, `name` |
| `bronze.form13f_signature` | filing | `accession_number` | `name`, `title`, `phone`, `signature`, `city`, `stateorcountry`, `signaturedate` |

Keys are unique **within one `_dataset`**. Whether an accession number can appear in two data sets
is unknown; DQ7 measures it.

### Source 2 tables (deferred, Q2, #5)

- `raw/edgar_full_index/<YYYY>/QTR<n>/form.idx`, written once per closed quarter. The current,
  still-growing quarter is re-fetched under a dated name, never overwriting an older copy.
- `bronze.edgar_filing_index`: one row per 13F-family filing (13F-HR, 13F-HR/A, 13F-NT, 13F-NT/A)
  in the index: `cik`, `company_name`, `form_type`, `date_filed`, `file_path`, `accession_number`
  (derived from the path), plus metadata. Partitioned by index quarter, `replaceWhere`.
- `raw/edgar_13f_xml/<cik>/<accession_number>/<original file name>`, fetched once per filing.
- `bronze.edgar_infotable`: one row per `infoTable` element, all values as strings, same column
  names as `bronze.form13f_infotable` where an equivalent exists, plus `accession_number`, a
  line sequence number and metadata. Only for filings after the end of the latest data set.

## Data quality rules (Dagster asset checks)

Checks marked **block** fail the asset; **warn** record a number and let the run continue.

| ID | Rule | Tables | Level |
|---|---|---|---|
| DQ1 | Row count > 0 for the partition | submission, coverpage, summarypage, signature, infotable | block |
| DQ2 | Row count >= 0 is recorded (can be empty) | othermanager, othermanager2 | warn |
| DQ3 | Source key not null and not empty | all | block |
| DQ4 | Source key unique within `_dataset` | all | block |
| DQ5 | Rows loaded = data lines in the TSV (lines minus header) per file | all | block |
| DQ6 | Every `accession_number` in infotable, coverpage, summarypage, signature, othermanager(2) exists in submission for the same `_dataset` | all except submission | warn (count of orphans) |
| DQ7 | Number of `accession_number`s in this partition that also exist in another partition | submission | warn |
| DQ8 | `cusip` not 9 characters; `value` or `sshprnamt` not a non-negative integer | infotable | warn (count) |
| DQ9 | Metadata columns not null; `_dataset` equals the partition key | all | block |

## Acceptance criteria (checked on UAT by `test-engineer`)

- **AC1** Config: with the SEC `User-Agent` setting unset, a Source 1 run fails before any HTTP
  request. The value is not in the repo; `grep` of the repo finds no contact email.
- **AC2** Partitions: Dagster lists exactly one Bronze partition per data set on the SEC page
  (54 as of 2026-11-06, from `2013q2` to the latest), in time order.
- **AC3** Raw: for every partition, `raw/form13f_datasets/<key>/` holds exactly one zip whose
  SHA-256 equals the one in the materialization metadata.
- **AC4** One-command backfill: from an empty bucket, the single command in
  `docs/runbooks/` materializes every partition of all 7 Bronze tables, with no manual step.
- **AC5** Every block-level DQ check passes for every partition; warn-level counts are listed
  in `docs/qa/m1-acceptance.md`.
- **AC6** Idempotent rerun: re-materializing `2024-03_2024-05` (or any one partition) leaves
  the row count of every Bronze table, in total and per `_dataset`, unchanged; adds exactly one
  Delta version per table; and makes zero HTTP downloads (the raw zip is reused, visible in logs).
- **AC7** Isolation: after AC6, rows of every other `_dataset` keep their original
  `_ingested_at` and `_run_id`.
- **AC8** Lossless: for 3 sampled partitions (`2013q2`, `2022q4`, latest), 20 random INFOTABLE
  rows matched by `_source_line` equal the raw TSV line field by field, byte for byte.
- **AC9** Rate limit: the HTTP client's unit test shows no more than 10 requests in any
  1-second window under load, retries a 429/503 with backoff, and gives up after a set number of
  attempts with a clear error.
- **AC10** Record keeping: `docs/qa/m1-acceptance.md` lists, per partition, the zip size, row
  count per table and runtime, plus the total backfill wall-clock time.
- **AC11** Dev: on ThanhPC, the same command with a 4-partition range (e.g. `2022q4` to
  `2023q3`) completes using local storage.
- **AC12** `make check` and CI are green; tests use small real fixtures in `tests/fixtures/`.

Deferred with Source 2 (#5): **AC13** for one closed index quarter, every 13F-HR in `form.idx` that is
not in a published data set has exactly one raw XML object and its holdings in
`bronze.edgar_infotable`; a rerun makes zero HTTP requests for filings already landed.
Deferred with the sensor (#6): **AC14** with the published list mocked to add a new zip, the sensor
requests exactly one run for the new partition and none on its next tick.

## Edge cases

- **2024 cadence change**: the two-month `01jan2024-29feb2024` set and the year-crossing
  Dec-Feb sets must parse and get correct keys. Tests cover both file-name formats.
- **Amendments**: 13F-HR/A filings appear as their own accession numbers, possibly years after
  the period they amend, in whatever data set covers their filing date. `amendmenttype`
  ("RESTATEMENT" or "NEW HOLDINGS", **values unverified**) and `amendmentno` must survive as-is.
- **Value unit change**: `value` is in thousands of dollars before 2023-01-03 and in dollars from
  then on. The switch goes by **filing date, not period of report** (an amendment filed in 2024
  for 2021 Q4 is in dollars). Bronze keeps `value` raw; `filing_date` lives in submission, so
  M2 must join to it. `tablevaluetotal` probably follows the same rule (**unverified**).
- **Confidential treatment**: rows are omitted from INFOTABLE when confidential treatment is
  granted; `isconfidentialomitted`, `confdeniedexpired`, `datedeniedexpired` and
  `reasonfornonconfidentiality` must be kept. Later filings can add the holdings back.
- **Notices with no holdings**: 13F-NT and 13F-HR "NOTICE" report types have a submission and
  cover page but no INFOTABLE rows. DQ6 counts orphans in one direction only for this reason.
- **Free text**: `additionalinformation` (up to 4000 chars) and names may hold tabs, quotes or
  line breaks. DQ5 catches any row that the parser splits or merges.
- **Republished zips**: if SEC replaces a zip under the same name, the pipeline keeps the first
  copy. Picking up a new version is a manual, documented step (runbook), never automatic.
- **Missing or new file in a zip**: a missing table fails the partition; an unknown extra file
  is landed in raw (it is in the zip) and logged, not loaded.
- **SEC errors**: 403 (bad User-Agent), 429 and 5xx are retried or fail cleanly; a partial
  download is never written to raw.
- **Filer CIK vs manager name**: kept as separate raw fields (`submission.cik`,
  `coverpage.filingmanager_name`); no matching in M1.

## Sizing (about 36 h available)

| Slice | Estimate |
|---|---|
| Partition list from file names, config for User-Agent, rate-limited HTTP client with tests | 6 h |
| Raw landing asset (download once, checksum, skip if present) | 4 h |
| 7 Bronze tables: declared schemas, metadata, `replaceWhere`, fixtures | 8 h |
| Asset checks DQ1 to DQ9 | 4 h |
| Dev run, UAT backfill, fixes found at scale | 5 h |
| Runbook, acceptance report, reviews and PR overhead | 5 h |
| **Core total** | **32 h** |
| Source 2 (index + XML landing + XML parsing + tests) | +10 to 12 h |
| Sensor for new data sets | +3 h |

Core fits with about 4 h of buffer. Core + Source 2 + sensor (~46 h) does **not** fit before
2026-12-20. Recommended cut: ship the core in M1, move Source 2 to its own slice early in M2
(or a M1.5), keep the sensor as stretch only if the core lands by 2026-12-06.

## Decisions (Thanh, 2026-11-06)

All six open questions were settled on the recommended answers. Deferred work is tracked as issues.

- **Q1 Partition key: one partition per SEC data set.** One zip = one partition = one
  `replaceWhere`. Filing quarter and period of report stay as columns, so M2 can partition Silver
  by period of report.
- **Q2 Source 2 (EDGAR full-index + XML): not in M1.** Backlog: #5.
- **Q3 Backfill start: `2013q2`**, the first data set.
- **Q4 Bronze column types: all source columns as strings**, typed in M2.
- **Q5 Sensor: not in M1.** New data sets are added by a quarterly runbook step. Backlog: #6.
- **Q6 Republished zips: keep the first copy, re-land by hand via the runbook** (Option A).
  Automatic detection and versioned raw copies (Option B) are in the backlog: #7.

## Candidate ADRs for the architect

1. Partitioning and idempotency for Bronze (Q1 answer, `replaceWhere` scope, raw path layout).
2. EDGAR HTTP client: rate limiting, retries, User-Agent config, and where raw bytes are kept.
