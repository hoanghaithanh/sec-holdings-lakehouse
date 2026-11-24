# Real 13F samples

Small cuts of three real SEC Form 13F data-set zips, downloaded from sec.gov on 2026-11-11:
`2013q2` (the first data set), `2022q4` and `2026-06_2026-08` (the latest).

Each sample keeps 10 accession numbers and, for those filings, every line of the six small tables
and at most 20 INFOTABLE lines per filing. Lines keep their original bytes and order, and every
table keeps its original member name and header. `FORM13F_metadata.json` is kept whole;
`FORM13F_readme.htm` (447 KB) is dropped.

Data lines per table:

| Sample | submission | coverpage | othermanager | signature | summarypage | othermanager2 | infotable |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2013q2 | 10 | 10 | 8 | 10 | 8 | 3 | 160 |
| 2022q4 | 10 | 10 | 3 | 10 | 8 | 4 | 160 |
| 2026-06_2026-08 | 10 | 10 | 0 | 10 | 10 | 1 | 200 |

The data is public SEC data. Used by `tests/test_form13f_real_samples.py`.

## Hand-made data sets (not real SEC data)

The Silver tests also write small made-up data sets straight to Bronze (`bronze_rows` in
`tests/test_form13f_silver_asset.py`). Their accession numbers (`0000950123-13-0000NN`), CIKs and
manager names are invented, and each row is chosen to hit one Silver rule: a bad date, a period
that is not a quarter end, an unknown submission type, an oversized integer, an orphan COVERPAGE
row, the value-unit boundary (2022-12-30 vs 2023-01-03), a re-land that changes a period, and one
accession shared by two data sets. Each made-up filing has one made-up INFOTABLE line
(`MADE UP CORP`, CUSIP `123456789`) unless a test gives its own lines: an empty or oversized
amount, a bad `infotable_sk`, a short CUSIP, a CINS-shaped CUSIP, an orphan line, and lines that
change, leave or join on a re-land.
