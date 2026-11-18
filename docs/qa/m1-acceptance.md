# M1 Bronze ingestion: acceptance report

- Spec: `docs/specs/m1-bronze-ingestion.md` (AC1 to AC12)
- Environment: UAT (homelab, Docker Compose, Spark standalone with 1 master and 2 workers), `main` at
  `26d7c78` (PR #19) for the load, `8fc11ac` (PR #20) when the evidence was read
- Load of record: Dagster backfill `yoddtebi` of `form13f_bronze_job --all`, 2026-11-17 06:37 to
  08:39 UTC
- Evidence: read-only scripts run in the `dagster-daemon` container (Dagster run history, check
  evaluations and materialization metadata; Spark aggregates over the Bronze Delta tables). Only
  counts and hashes were collected, never table rows.
- Written by Claude on 2026-11-18 from that evidence. Thanh signs off at Checkpoint 3.

## Result

**10 of 12 criteria pass, AC3 passes in part, and AC11 was not run.** Every block-level check passed
on all 54 data sets. Bronze holds 124,012,468 INFOTABLE rows and 409,685 filings from `2013q2` to
`2026-06_2026-08`.

| AC | What | Result | Evidence |
|---|---|---|---|
| AC1 | No `User-Agent`, no request | Pass | Backfill `lqcbshow` (2026-11-07, before `SEC_USER_AGENT` was set on UAT): all 54 runs failed with `ValueError: SEC_USER_AGENT is not set` before any download. Unit tests `test_client_refuses_to_start_without_user_agent` and `test_sec_resource_fails_at_start_without_user_agent`. `git grep` finds no real contact email in the repo. |
| AC2 | One partition per data set, in time order | Pass | 54 partitions, `2013q2` to `2026-06_2026-08`; `test_keys_are_unique_and_sort_in_time_order`. |
| AC3 | One raw zip per partition with the recorded SHA-256 | Pass in part | All 54 zips are present and were hashed (3,051 MB in total). All 54 raw steps of `yoddtebi` logged "Raw zip present, reusing" and none downloaded. The hashes were not compared one by one with the materialization metadata. |
| AC4 | One command loads everything | Pass | `dagster job backfill -j form13f_bronze_job --all` from the runbook: 54 of 54 runs succeeded with no manual step. The raw zips were already landed, so this was not a run from an empty bucket. |
| AC5 | Block checks pass, warn counts listed | Pass | 43 checks per run, 2,322 in total; no ERROR-severity failure. Warn counts are below. |
| AC6 | A rerun changes nothing and adds one Delta version | Pass | Every data set was loaded twice (Nov 11, then `yoddtebi`). Row counts are identical per table and per data set, each table went from Delta version 53 to 107 (one commit per data set), and no zip was downloaded again. Shown with the full reload, not with a separate one-partition rerun. |
| AC7 | A rerun leaves other data sets alone | Pass | After `yoddtebi`, all 378 (table, data set) pairs have exactly one `_run_id`, it is the run that loaded that data set, and its `_ingested_at` falls inside that run. Each later `replaceWhere` left the 53 others untouched. |
| AC8 | Lossless | Pass | 20 random INFOTABLE rows matched by `_source_line` against the raw TSV line, field by field: 20/20 in `2013q2`, `2022q4` and `2026-06_2026-08`. |
| AC9 | Rate limit and retries | Pass | `tests/test_sec_http.py`: `test_never_more_than_max_rps_in_any_second`, `test_retries_5xx_with_backoff_then_succeeds`, `test_gives_up_after_max_attempts` and others. |
| AC10 | Record keeping | Pass | This report: per data set zip size, rows per table and run time, plus wall-clock time. |
| AC11 | 4-partition run on ThanhPC | Not run | No record of a local run with `--from 2022q4 --to 2023q3`. Left for Thanh. |
| AC12 | `make check` and CI green, real fixtures | Pass | CI green on `main` at `8fc11ac`; 156 tests pass locally; samples of 3 real zips in `tests/fixtures/form13f/`. |

## Backfill timing

| Load | Code | Runs | Wall clock | Run time per data set (min / median / max) | Sum of run time |
|---|---|---|---|---|---|
| `fdvnbwse`, 2026-11-11 | PR #14, no DQ checks | 54 succeeded | 1 h 40 min | 82 s / 104 s / 169 s | 5,825 s |
| `xdukmzhx`, 2026-11-17 05:34 | PR #15 | 8 failed DQ4, 46 cancelled | 12 min | n/a | n/a |
| `yoddtebi`, 2026-11-17 06:37 (**of record**) | PR #19 | 54 succeeded | 2 h 02 min | 129 s / 267 s / 288 s | 14,222 s |

The runs are serialized by the `delta_writes` pool (one Bronze writer at a time). With the checks,
a data set takes about 2.6 times as long as before. The likely cause is the extra Spark passes the
checks make over each cached table, but this was not profiled (deferred).

## Data quality checks (load of record)

| Check | Level | Result over 54 data sets |
|---|---|---|
| DQ1 has rows | block | passed in all |
| DQ2 row count (other-manager tables) | warn | 370,152 rows recorded; largest 5,147 (othermanager, `2024-06_2024-08`) |
| DQ3 key not null | block | passed in all |
| DQ4 key unique | block (warn on othermanager2) | passed on 6 tables in all; othermanager2: 230 duplicate keys across all 54 data sets, at most 9 (`2025-12_2026-02`) |
| DQ5 rows match the file | block | passed in all |
| DQ6 accession in SUBMISSION | warn | 0 orphans |
| DQ7 accession in another data set | warn | 0 shared |
| DQ8 INFOTABLE values | warn | 0 bad `cusip`, 0 bad `value`, 0 bad `sshprnamt` |
| DQ9 metadata valid | block | passed in all |

DQ4 on othermanager2 was a block check until PR #19. The first backfill with the checks
(`xdukmzhx`) stopped on it, because real filings give one `sequencenumber` to several different
managers (up to 13 copies of one key in a 2013q2 filing). Nothing was written, Bronze kept the Nov 11
load, and Thanh decided to keep the rows as published and count them (PR #19). Silver resolves them.

## Per data set

| Data set | Zip (MB) | Run (s) | submission | coverpage | summarypage | signature | othermanager | othermanager2 | infotable |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `2013q2` | 2.0 | 129 | 166 | 166 | 141 | 166 | 47 | 179 | 87,715 |
| `2013q3` | 36.5 | 264 | 5,227 | 5,227 | 3,813 | 5,227 | 3,428 | 2,811 | 1,533,636 |
| `2013q4` | 36.2 | 275 | 5,043 | 5,043 | 3,690 | 5,043 | 3,320 | 2,686 | 1,519,317 |
| `2014q1` | 38.1 | 266 | 5,425 | 5,425 | 4,047 | 5,425 | 3,450 | 2,510 | 1,584,857 |
| `2014q2` | 38.4 | 263 | 5,415 | 5,415 | 4,023 | 5,415 | 3,562 | 2,651 | 1,588,232 |
| `2014q3` | 39.6 | 264 | 5,482 | 5,482 | 4,097 | 5,482 | 3,467 | 2,594 | 1,638,747 |
| `2014q4` | 39.1 | 261 | 5,465 | 5,465 | 4,061 | 5,465 | 3,442 | 2,622 | 1,636,491 |
| `2015q1` | 42.4 | 259 | 6,045 | 6,045 | 4,512 | 6,045 | 3,842 | 2,835 | 1,768,828 |
| `2015q2` | 41.3 | 252 | 5,914 | 5,914 | 4,418 | 5,914 | 3,891 | 2,747 | 1,711,601 |
| `2015q3` | 42.6 | 258 | 6,000 | 6,000 | 4,514 | 6,000 | 3,861 | 2,886 | 1,772,870 |
| `2015q4` | 41.2 | 264 | 5,975 | 5,975 | 4,469 | 5,975 | 3,784 | 2,831 | 1,695,347 |
| `2016q1` | 41.7 | 249 | 6,072 | 6,072 | 4,557 | 6,072 | 3,722 | 2,897 | 1,710,477 |
| `2016q2` | 42.1 | 245 | 6,034 | 6,034 | 4,526 | 6,034 | 3,837 | 2,864 | 1,747,959 |
| `2016q3` | 42.1 | 251 | 6,007 | 6,007 | 4,519 | 6,007 | 3,718 | 2,732 | 1,742,410 |
| `2016q4` | 41.8 | 249 | 5,994 | 5,994 | 4,526 | 5,994 | 3,665 | 2,820 | 1,716,138 |
| `2017q1` | 43.7 | 248 | 6,210 | 6,210 | 4,677 | 6,210 | 3,940 | 2,859 | 1,805,106 |
| `2017q2` | 43.3 | 267 | 6,218 | 6,218 | 4,706 | 6,218 | 3,761 | 2,796 | 1,790,999 |
| `2017q3` | 42.6 | 268 | 6,207 | 6,207 | 4,697 | 6,207 | 3,531 | 2,846 | 1,746,541 |
| `2017q4` | 43.5 | 253 | 6,174 | 6,174 | 4,655 | 6,174 | 3,744 | 2,673 | 1,801,866 |
| `2018q1` | 45.3 | 257 | 6,570 | 6,570 | 5,017 | 6,570 | 3,771 | 2,764 | 1,879,731 |
| `2018q2` | 46.5 | 264 | 6,622 | 6,622 | 5,081 | 6,622 | 3,774 | 2,771 | 1,925,787 |
| `2018q3` | 46.6 | 267 | 6,647 | 6,647 | 5,087 | 6,647 | 3,782 | 2,738 | 1,934,475 |
| `2018q4` | 48.5 | 267 | 6,659 | 6,659 | 5,112 | 6,659 | 3,972 | 2,884 | 2,040,197 |
| `2019q1` | 46.5 | 263 | 6,887 | 6,887 | 5,328 | 6,887 | 3,850 | 2,733 | 1,945,558 |
| `2019q2` | 54.1 | 254 | 7,144 | 7,144 | 5,534 | 7,144 | 3,964 | 3,033 | 2,301,992 |
| `2019q3` | 49.9 | 248 | 6,932 | 6,932 | 5,381 | 6,932 | 3,850 | 2,851 | 2,116,186 |
| `2019q4` | 49.5 | 271 | 6,981 | 6,981 | 5,359 | 6,981 | 3,754 | 2,891 | 2,116,307 |
| `2020q1` | 50.4 | 282 | 7,285 | 7,285 | 5,727 | 7,285 | 3,799 | 2,889 | 2,159,065 |
| `2020q2` | 49.7 | 266 | 7,296 | 7,296 | 5,757 | 7,296 | 3,789 | 2,919 | 2,163,834 |
| `2020q3` | 49.1 | 266 | 7,157 | 7,157 | 5,681 | 7,157 | 3,598 | 2,872 | 2,089,408 |
| `2020q4` | 50.2 | 264 | 7,237 | 7,237 | 5,771 | 7,237 | 3,593 | 2,997 | 2,156,794 |
| `2021q1` | 56.6 | 256 | 7,883 | 7,883 | 6,375 | 7,883 | 3,751 | 2,967 | 2,454,113 |
| `2021q2` | 56.2 | 257 | 7,927 | 7,927 | 6,390 | 7,927 | 3,530 | 2,845 | 2,376,681 |
| `2021q3` | 62.9 | 269 | 8,096 | 8,096 | 6,501 | 8,096 | 3,693 | 2,984 | 2,660,728 |
| `2021q4` | 60.8 | 268 | 7,985 | 7,985 | 6,430 | 7,985 | 3,657 | 3,056 | 2,569,324 |
| `2022q1` | 64.1 | 255 | 8,921 | 8,921 | 7,251 | 8,921 | 3,829 | 3,228 | 2,684,317 |
| `2022q2` | 62.7 | 276 | 8,906 | 8,906 | 7,292 | 8,906 | 3,785 | 3,143 | 2,603,731 |
| `2022q3` | 63.1 | 286 | 8,948 | 8,948 | 7,321 | 8,948 | 3,840 | 3,100 | 2,632,327 |
| `2022q4` | 62.8 | 271 | 9,010 | 9,010 | 7,308 | 9,010 | 3,915 | 3,434 | 2,622,058 |
| `2023q1` | 71.8 | 271 | 9,201 | 9,201 | 7,627 | 9,201 | 3,815 | 3,252 | 2,786,017 |
| `2023q2` | 68.9 | 268 | 9,113 | 9,113 | 7,513 | 9,113 | 3,817 | 3,472 | 2,672,308 |
| `2023q3` | 70.2 | 270 | 9,108 | 9,108 | 7,522 | 9,108 | 3,917 | 3,281 | 2,724,039 |
| `2023q4` | 73.0 | 269 | 9,196 | 9,196 | 7,624 | 9,196 | 3,831 | 3,238 | 2,886,468 |
| `2024-01_2024-02` | 73.0 | 271 | 9,433 | 9,433 | 7,864 | 9,433 | 3,943 | 3,221 | 2,848,736 |
| `2024-03_2024-05` | 78.2 | 269 | 9,884 | 9,884 | 8,241 | 9,884 | 4,425 | 3,367 | 3,026,705 |
| `2024-06_2024-08` | 83.9 | 273 | 10,117 | 10,117 | 8,244 | 10,117 | 5,147 | 3,515 | 3,278,515 |
| `2024-09_2024-11` | 81.7 | 285 | 9,891 | 9,891 | 8,249 | 9,891 | 4,178 | 3,545 | 3,201,864 |
| `2024-12_2025-02` | 86.5 | 288 | 10,605 | 10,605 | 8,848 | 10,605 | 4,425 | 3,735 | 3,362,299 |
| `2025-03_2025-05` | 88.3 | 271 | 10,765 | 10,765 | 9,032 | 10,765 | 4,454 | 3,811 | 3,440,002 |
| `2025-06_2025-08` | 86.3 | 276 | 10,799 | 10,799 | 8,938 | 10,799 | 4,883 | 3,910 | 3,360,306 |
| `2025-09_2025-11` | 85.6 | 287 | 10,422 | 10,422 | 8,691 | 10,422 | 4,358 | 3,759 | 3,267,091 |
| `2025-12_2026-02` | 90.3 | 275 | 11,372 | 11,372 | 9,500 | 11,372 | 4,751 | 3,925 | 3,473,209 |
| `2026-03_2026-05` | 99.4 | 276 | 11,761 | 11,761 | 9,846 | 11,761 | 4,751 | 4,242 | 3,822,885 |
| `2026-06_2026-08` | 100.7 | 276 | 11,852 | 11,852 | 10,036 | 11,852 | 4,848 | 3,890 | 3,830,274 |
| **Total (54)** | 3,051.2 | 14,222 | 409,685 | 409,685 | 326,126 | 409,685 | 207,051 | 163,101 | 124,012,468 |

## Findings and follow-ups

- **Disk:** `/mnt/md0` grew from 15 GB to 92 GB during the two loads on Nov 17, while the root SSD
  stayed at 21%. Most likely this is the Delta files of the replaced versions (nothing runs
  `VACUUM` yet) plus Spark scratch on the array. Not yet checked. Deferred: Delta maintenance
  (`VACUUM`, `OPTIMIZE`) is already listed in the M2 spec's deferred work.
- **Run time** went from a median of 104 s to 267 s per data set with the checks (above).
- **Backfills launched from the UI** run as `__ASSET_JOB`, not `form13f_bronze_job`. Use the runbook
  command so runs are easy to find.
- **AC11** (dev run on ThanhPC) is open.
