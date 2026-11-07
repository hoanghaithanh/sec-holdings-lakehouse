# ADR-003: SEC HTTP client and raw landing

- Status: Proposed
- Date: 2026-11-06
- Spec: `docs/specs/m1-bronze-ingestion.md`
- Spec decisions it builds on: **Q2** (Source 2 not in M1, #5, but the client must be reusable
  for it), **Q6** (keep the first copy of a republished zip; automatic detection is #7). Decided by Thanh on 2026-11-06.

## Context

SEC's fair-access rules allow at most 10 requests per second and require every request to send a
`User-Agent` with the app name and a contact email. SEC throttles or blocks clients that ignore
this (`403`). The contact email is personal, so it must come from config and never be in git
(AC1). Every raw download is stored once and never fetched twice.

Source 1 is only 54 large downloads (sizes unverified; INFOTABLE has millions of rows per set).
Source 2 (EDGAR XML, a later slice) is the opposite: thousands of small requests, where the rate
limit really matters. The client must serve both without a rewrite.

The zips must land in the object store (RustFS on dev Compose and UAT) or on local disk
(`uv run dagster dev`, where `LAKEHOUSE_ROOT` is a folder). On UAT the Spark executors run in
other containers, so anything Spark reads must be in the object store. Tests must run in CI with
no network.

## Options

**HTTP library**

1. **httpx (chosen).** Small, typed (works with `mypy --strict` without stub packages), has
   timeouts on by default and a clean streaming API. Its `MockTransport` lets tests fake SEC
   responses with no extra library. Version 0.28.1 supports Python 3.11.
2. **requests.** Already installed (Dagster depends on it), but it needs `types-requests` for
   mypy and a mocking library (`responses`) for tests. Its built-in retry (`urllib3.Retry`) runs
   inside the adapter, so retries would bypass our rate limiter and could break the 10/s rule.

**Retries**: tenacity 9.2.1 was considered. A 30-line loop in our client was chosen instead,
because every attempt must go through the same rate limiter and the same fake clock in tests,
and honouring `Retry-After` would need a custom tenacity wait anyway.

**Writing to the object store**

1. **boto3 (chosen)**, behind the existing `LakehouseStorage` resource, with a plain-file backend
   when `LAKEHOUSE_ROOT` is a local folder. Well known, handles multipart upload of big files.
   Adds `boto3` (1.43.x) and the dev-only `types-boto3[s3]` stubs for mypy.
2. **Hadoop FileSystem through Spark's JVM.** No new dependency, but the raw asset would need a
   SparkSession just to copy a file, and pushing bytes through py4j is slow and awkward.
   (fsspec + s3fs was also ruled out: s3fs pins botocore tightly, a known source of conflicts.)

**Where the zip is unpacked and parsed**

1. **Driver unpacks to staging, Spark reads the TSVs (chosen).** The Dagster step (the Spark
   driver process) streams each zip member to `staging/form13f/<key>/<MEMBER>.tsv` in the store,
   adding the line number to the front of each line. Spark then reads those files in parallel on
   the workers.
2. **Parse in Python and build DataFrames on the driver.** Simple, but holds millions of rows in
   driver memory, against the project's Spark rules.
3. **Spark reads the zip itself** (`binaryFile` plus a Python UDF to unzip). One zip becomes one
   task, so there is no parallelism, and it needs a Python UDF.

## Decision

- An `SecClient` class in `src/sec_lakehouse/sources/sec_http.py`, built on httpx, wrapped in a
  Dagster resource `sec`. It owns the User-Agent, the rate limiter and the retry loop.
- `SEC_USER_AGENT` is a required env var with no default, read at run time.
- Downloads stream to a temp file, are verified, then are uploaded once to raw with boto3 (or
  copied on local disk). An existing raw object means no HTTP call at all.
- Unpacking happens on the driver, streaming to staging. Parsing happens in Spark with built-in
  functions.

## Design

### Config

| Env var | Required | Default | Rule |
|---|---|---|---|
| `SEC_USER_AGENT` | yes | none | Must contain an app name and an email (`name@domain.tld`), no line breaks |
| `SEC_MAX_REQUESTS_PER_SECOND` | no | `5` | Integer from 1 to 10; anything above 10 is refused |

- The resource takes the value as `EnvVar("SEC_USER_AGENT")`, like the S3 keys today. Dagster
  resolves it when a run starts, so `dagster definitions validate` and CI work without it, and a
  run that uses `sec` fails at resource start-up, before any request (AC1). The value never shows
  in the Dagster UI.
- Default 5 per second, not 10: `dagster.yaml` allows 2 concurrent runs and each run has its own
  limiter, so 2 x 5 stays within SEC's limit with no shared state between processes.
- `.env.example` and `.env.uat.example` get the variable with a placeholder and no email. Tests
  use an address at the reserved `example.com` domain; the AC1 grep is for Thanh's real address.

### Client

```text
SecClient(user_agent, max_rps, transport=None, clock=time.monotonic, sleep=time.sleep)
  get(url) -> bytes                       # small documents, for Source 2 later
  download(url, dest: BinaryIO) -> DownloadResult(url, size, sha256, content_length)
```

- **Hosts**: only `https://www.sec.gov/` URLs are accepted, so the User-Agent (with the email) is
  never sent anywhere else. The exact zip URL path is **unverified** (sec.gov was blocked from
  the sandbox); it is checked in the first build slice and kept as one constant.
  *Amended 2026-11-06 (slice 1):* the SEC page links the newest zip from a different folder
  (`/files/datastandardsinnovation/...` instead of `/files/structureddata/...`), so the code
  keeps each zip's full URL as SEC links it, in `FORM13F_ZIP_URLS`, instead of one base constant.
- **Rate limiter**: every attempt, including retries, waits until at least `1 / max_rps` seconds
  have passed since the previous request started. With a lock, so a thread pool in Source 2 can
  share one client. At 5/s that is 200 ms between requests, well under 10 in any second.
- **Retries**: on 429, 500, 502, 503, 504, connect and read timeouts, and a stream cut off in the
  middle. At most 5 attempts, waiting 1, 2, 4, 8 s plus random jitter, or the `Retry-After`
  header when it asks for longer (capped at 60 s). After the last attempt it raises
  `SecRequestError` with the URL, status and attempt count.
- **No retry**: 403 fails at once with "SEC refused the request: check SEC_USER_AGENT and the
  request rate", because retrying a block makes it worse. 404 fails with "data set not found"
  and the URL.
- **Timeouts**: 10 s to connect, 60 s between reads. Logs never include the User-Agent.

### Raw landing (download once)

1. Build the raw key `raw/form13f_datasets/<key>/<zip name>`. If `storage.exists(key)`: no HTTP
   call, log "raw zip present, reusing", hash the stored object for the metadata, done (AC6).
2. Otherwise stream the response into a temp file on the driver's disk, hashing SHA-256 on the
   way. Memory use stays flat whatever the zip size.
3. Verify before landing: the size equals `Content-Length` when SEC sends one, and
   `zipfile.ZipFile(tmp).testzip()` finds no bad member. A failure here retries the download;
   nothing has been written to raw yet.
4. `storage.put_new(key, tmp)`: a boto3 upload (multipart for big files) or, on local disk,
   copy to a temp name then `os.replace`. In both cases the object appears whole or not at all,
   so a partial download can never sit in raw.
5. Metadata: URL, size, SHA-256, download time, `downloaded: true|false`.

`LakehouseStorage` grows a small object API (`exists`, `put_new`, `open_read`, and
`delete_prefix`, which refuses anything outside `staging/`), using `S3_ENDPOINT` and the S3 keys
already in config. No code path is specific to one machine.

*Amended 2026-11-06 (slice 2):* the API is `exists`, `put_new`, `iter_bytes` and `sha256`
(`delete_prefix` comes with the staging slice). On local disk `put_new` copies to a temp name and
then hard-links it into place (`os.link`), not `os.replace`, because a link fails if the target
already exists while `os.replace` would silently overwrite; this needs a filesystem with hard
links (NTFS, ext4, APFS). On S3 it is a check before the upload. The boto3 client is set to
path-style addressing and sends checksum headers only when required, for RustFS.

### Unpack and parse (used by ADR-002)

- **Unpack (driver, streaming)**: for each member, read the header line, compare it with the
  declared columns, then copy the rest line by line to `staging/form13f/<key>/<MEMBER>.tsv` as
  `<line number>\t<original line>`, counting lines for DQ5. The line terminator (`\n` or `\r\n`)
  is not part of the row; nothing else is changed. Staging is scratch space: overwritten on each
  run and deleted after a successful load.
- **Parse (Spark)**: `spark.read.option("lineSep", "\n").text(path)`, then
  `split(value, "\t", -1)` and pick fields by position. Built-in functions only, no Python UDF.
  Checked locally on PySpark 3.5: quotes, leading spaces and empty fields come through unchanged,
  and an empty field stays `""`, not null. Spark's CSV reader was avoided because by default it
  turns empty strings into nulls and interprets quotes, which would break the lossless rule
  (AC8).
- Any line with the wrong number of fields fails the partition, with the first few line numbers
  in the error.

### Tests without network

- **Client**: `httpx.MockTransport` plays scripted responses (200, 429 with `Retry-After`, 503
  twice then 200, 403, a stream cut off halfway). A fake clock and fake sleep make the timing
  tests instant and exact: "no more than N requests in any 1-second window", backoff waits, give
  up after 5 attempts (AC9). A test also asserts the client refuses to start with an empty or
  email-less User-Agent.
- **Raw landing**: the local-disk storage backend in `tmp_path`, with the mock transport. A second
  materialization makes zero requests.
- **Bronze**: two small real zips in `tests/fixtures/` (one `YYYYqN` set and one 2024+ range
  set), cut down to a few dozen lines per TSV while keeping the original headers. They are built
  once on ThanhPC by a script in `scripts/`, which runs with the real User-Agent and is never run
  in CI. Spark tests read them through local storage.
- No test touches sec.gov or needs RustFS. The Compose smoke test in CI stays as it is.

### Reuse for Source 2

Source 2 uses `get()` for index pages, `download()` for XML, the same limiter, and the same
landing helper, which takes a raw key and a URL rather than anything 13F-specific. At Source 2
volumes the 5/s per run may be too slow; the fix then is one SEC concurrency pool.

## Consequences

- Two new runtime dependencies (`httpx`, `boto3`) and one dev dependency (`types-boto3[s3]`).
  boto3 is large, but it is the standard S3 client and every reviewer knows it.
- `LakehouseStorage` now has two backends (local disk and S3) to keep in step, and both need
  tests.
- Staging writes the unpacked TSVs once more per load, and the driver's temp disk must hold one
  zip. Unpacking is one sequential Python pass per partition before Spark starts.

## Risks

- **Free text with real line breaks or tabs inside a field** (the spec flags
  `additionalinformation`). Shows up as a wrong field count and a failed partition, never as
  silently shifted columns. Fix: a quote-aware split for the affected table, decided when the
  real data shows it.
- **SEC blocks the client** (403) during the backfill. Shows up as a clear error naming the
  User-Agent. Fix: check the value, lower `SEC_MAX_REQUESTS_PER_SECOND`, resume the backfill;
  zips already landed are not fetched again.
- **Very large zips** fill the driver's temp disk on UAT. Shows up as an `OSError` in the raw
  step; the acceptance report records zip sizes so this is visible before it bites.
