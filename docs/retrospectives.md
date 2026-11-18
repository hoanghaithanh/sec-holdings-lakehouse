# Retrospectives

Three lines per milestone: what went well, what didn't, and one change for the next milestone.

## M1 Bronze ingestion (2026-11-06 to 2026-11-18)

- **Went well:** the spec and its six decisions were settled in one day (PR #4). The build came in
  as four small PRs (#8, #9, #14, #15) with samples of real SEC zips as fixtures. All 54 data sets
  (124 M holdings) now load with one command, about five weeks ahead of the roadmap's Dec 20 date.
- **Didn't:** the data quality checks first met real data after they were merged. The first full
  backfill with them stopped on a key that the SEC data doesn't actually hold unique
  (othermanager2, fixed in #19). The earlier load had also filled the root SSD (#16, #17). The
  acceptance report came a week after the load.
- **One change for M2:** before a data slice's PR is merged, run it on a few real UAT data sets
  (oldest, a mid one around the January 2023 unit change, and the newest), and write the acceptance
  numbers in the same session as the backfill.

_Drafted by Claude from the PRs and the thread history. Thanh to confirm or reword._
