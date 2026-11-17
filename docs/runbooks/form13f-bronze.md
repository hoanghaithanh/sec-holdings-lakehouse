# Loading 13F data sets into Bronze

How to load one SEC 13F data set, run the full backfill, rerun a data set, and pick up a zip that
SEC republished. Background: [M1 spec](../specs/m1-bronze-ingestion.md),
[walkthrough](../guides/form13f-bronze-walkthrough.md).

Everything goes through one job, `form13f_bronze_job`. Each run handles one data set (one
partition, e.g. `2013q2` or `2026-06_2026-08`):

1. `raw/form13f_dataset_zip` downloads the zip from SEC, unless raw already holds it (then it
   sends no request).
2. `bronze_form13f` loads the 7 Bronze tables for that data set and runs checks DQ1 to DQ9.

## Before the first run

- The stack runs the latest `main` (UAT deploys it automatically after CI; see
  [uat-deploy.md](uat-deploy.md)).
- `SEC_USER_AGENT` is set in `.env.uat` (or `.env` on ThanhPC): `grep -c '^SEC_USER_AGENT=.' .env.uat`
  prints `1`. Without it the run stops before it sends a request.
- In the Dagster UI, **Deployment → Concurrency** lists the pool `delta_writes` with limit 1.
- Disk: `df -h /mnt/md0` and `df -h /`. Raw zips and Bronze go to the object store on `/mnt/md0`.
  Each run also unpacks one zip under `/tmp` in the `dagster-daemon` container (on the system
  disk) and deletes it at the end of the run; allow a few GB free there.

## Commands used below

On UAT, run from `~/sec-lakehouse` on the server. Define this shell shortcut first:

```bash
dc() { docker compose -f docker-compose.yml -f docker-compose.uat.yml --env-file .env.uat "$@"; }
```

The function lasts only for that shell session. On ThanhPC with the local Docker stack, use plain
`docker compose` instead of `dc`.

## Load one data set

UI: **Jobs → form13f_bronze_job → Partitions**, pick the data set, **Materialize**.

CLI:

```bash
dc exec dagster-daemon dagster job backfill -w /opt/dagster/home/workspace.yaml \
  -j form13f_bronze_job --partitions 2013q2 --noprompt
```

Check: the run is green in the UI, and each of the 7 Bronze assets shows a `row_count` and a
passed check list.

## Full backfill (every data set from 2013q2)

```bash
dc exec dagster-daemon dagster job backfill -w /opt/dagster/home/workspace.yaml \
  -j form13f_bronze_job --all --noprompt
```

This queues one run per data set (54 as of November 2026; the list lives in
`sources/form13f/datasets.py`). `docker/dagster.yaml` runs at most 2 at once, and only one Bronze
load at a time (pool `delta_writes`), so a second zip downloads while the first loads. Each zip is
one SEC request, and each run limits itself to `SEC_MAX_REQUESTS_PER_SECOND` (5), so two runs
together stay at or under SEC's 10 per second.

While it runs, watch the Dagster **Runs** page and the Spark master UI. One Bronze load uses all
the Spark worker cores, so avoid other Spark work on UAT during the backfill.

For a range, swap `--all` for `--from 2022q4 --to 2023q3`. That is the dev check on ThanhPC
(AC11).

Check afterwards:

- **Runs** page, filter by job `form13f_bronze_job`: every run green. A red run names the failed
  check or the bad line in its error. Rerun only that data set (next section) after fixing the
  cause.
- To finish after failures, rerun only the failed data sets: `--partitions <key>,<key>` or the
  backfill's **Re-execute failed** button in the UI. Running `--all` again reloads everything; it
  is safe (no SEC requests, same result) but costs Spark time.
- Warn checks (DQ2, DQ6, DQ7, DQ8) never fail a run. Their counts are on each asset's
  **Checks** tab; copy them into the acceptance report.

## Rerun one data set

Same as "Load one data set". The raw zip is reused (the log says so and no SEC request is made),
and each Bronze table replaces only that data set's rows (`replaceWhere`). Other data sets keep
their rows, `_ingested_at` and `_run_id`.

If a blocking check (DQ1, DQ3, DQ4, DQ9) fails, nothing is written and Bronze keeps the previous
load of that data set. If DQ5 fails (rows written differ from the file), the run fails after the
write; rerun the data set once the cause is fixed.

## Pick up a zip that SEC republished

The pipeline keeps the first copy of each zip on purpose (ADR-003). To take a new version, delete
the stored zip by hand and rerun the data set. Do this only for a data set you have a reason to
reload, such as an SEC notice.

This changes raw data on UAT, so it needs Thanh's OK for that data set first.

1. Move the old zip aside, into `raw/form13f_datasets_superseded/`, so raw keeps exactly one zip
   per data set (AC3) and the old copy is not lost. Set the two values first; the zip name is in
   the raw asset's materialization metadata. The image has no `curl`, so this uses `boto3`, which
   the pipeline already depends on:

   ```bash
   dc exec -T -e KEY=2024-03_2024-05 -e ZIP=01mar2024-31may2024_form13f.zip dagster-daemon python - <<'EOF'
   import os
   from datetime import UTC, datetime
   from urllib.parse import urlsplit

   import boto3
   from botocore.config import Config

   root = urlsplit(os.environ["LAKEHOUSE_ROOT"])  # s3a://<bucket>[/<prefix>]
   bucket, prefix = root.netloc, root.path.strip("/")
   def key(path): return f"{prefix}/{path}" if prefix else path

   s3 = boto3.client(
       "s3",
       endpoint_url=os.environ["S3_ENDPOINT"],
       aws_access_key_id=os.environ["S3_ACCESS_KEY"],
       aws_secret_access_key=os.environ["S3_SECRET_KEY"],
       region_name="us-east-1",
       config=Config(s3={"addressing_style": "path"}),
   )
   old = key(f"raw/form13f_datasets/{os.environ['KEY']}/{os.environ['ZIP']}")
   stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
   kept = key(f"raw/form13f_datasets_superseded/{os.environ['KEY']}/{stamp}-{os.environ['ZIP']}")
   s3.copy_object(Bucket=bucket, Key=kept, CopySource={"Bucket": bucket, "Key": old})
   s3.head_object(Bucket=bucket, Key=kept)  # fails loudly if the copy is missing
   s3.delete_object(Bucket=bucket, Key=old)
   print("kept", kept, "- deleted", old)
   EOF
   ```

2. Check the output names both paths. If it raised an error, nothing was deleted.
3. Rerun the data set as above. The raw asset downloads the new zip and records its new SHA-256.
   Bronze replaces that data set's rows.
4. Note the date, the data set and both SHA-256 values in the acceptance report or the PR that
   asked for it.

## Undoing a bad load

Bronze can always be rebuilt from raw: fix the cause, then rerun the data set. A run that was
stopped half way (for example the stack restarted) can stay "Started" in the UI and keep a
concurrency slot; terminate it in the UI (**Runs → Terminate**, mark it failed if asked), then
rerun that data set.

## When something fails

| Error | Meaning | What to do |
| --- | --- | --- |
| `SEC_USER_AGENT ... not set` | No contact in the User-Agent | Set it in the env file and restart the stack. |
| `Form13fFormatError: zip is missing or has duplicate tables` | SEC changed the zip layout | Compare the zip with `sources/form13f/schemas.py`; change it in a PR. |
| `Form13fFormatError: ... header does not match` | A column was renamed, added or dropped | Same: update `schemas.py` on purpose. |
| `Form13fFormatError: ... lines [...] do not have N fields` | A tab or line break inside a value | Look at those lines in the zip; the parser needs a fix. |
| `BronzeQualityError: blocking checks failed` | DQ1, DQ3, DQ4 or DQ9 failed | Nothing was written. The message names the check and the table. |
| `BronzeQualityError: ... wrote N rows but the file has M` | DQ5 failed | Rerun once; if it repeats, open an issue with the run id. |
