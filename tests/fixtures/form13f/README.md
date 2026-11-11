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
