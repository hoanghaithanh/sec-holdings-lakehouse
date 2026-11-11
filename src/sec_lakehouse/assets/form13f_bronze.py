"""Bronze layer: the 7 tables of each 13F data-set zip as Delta tables (ADR-002, ADR-003).

One multi-asset writes all 7 tables from one pass over the zip. Every source column is kept as a
string, plus 5 metadata columns. Each table is overwritten only for the partition being loaded
(`replaceWhere _dataset = '<key>'`), so a rerun replaces exactly one data set and nothing else.
"""

# No `from __future__ import annotations` here: Dagster inspects the context annotation at runtime.
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)

from sec_lakehouse.assets.form13f_checks import (
    MUST_HAVE_ROWS,
    check_result,
    check_specs,
    infotable_value_problems,
    orphan_accessions,
    shared_accessions,
    table_metrics,
)
from sec_lakehouse.assets.form13f_raw import RAW_ZIP_ASSET, raw_zip_path
from sec_lakehouse.resources import LakehouseStorage, SparkResource
from sec_lakehouse.sources.form13f.datasets import DATASETS_BY_KEY, FORM13F_PARTITIONS
from sec_lakehouse.sources.form13f.schemas import TABLES, TableSpec, bronze_table_name
from sec_lakehouse.sources.form13f.unpack import Form13fFormatError, UnpackedTable, unpack

BRONZE_KEYS = {spec.name: AssetKey(["bronze", bronze_table_name(spec)]) for spec in TABLES}
STAGING_PREFIX = "staging/form13f"


class BronzeQualityError(RuntimeError):
    """A blocking data quality check failed (spec DQ1, DQ3, DQ4, DQ5, DQ9)."""


def read_bronze(session: Any, path: str) -> Any | None:
    """The Bronze table at `path`, or None before its first load. Other errors are raised."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(session, path):
        return None
    return session.read.format("delta").load(path)


def pre_write_checks(
    frames: list[tuple[UnpackedTable, Any]], existing_submission: Any | None, key: str
) -> dict[str, list[AssetCheckResult]]:
    """DQ1 to DQ4 and DQ6 to DQ9 on the parsed tables, per table. DQ5 runs after the write."""
    submission = next(df for table, df in frames if table.spec.name == "submission")
    results: dict[str, list[AssetCheckResult]] = {}
    for table, df in frames:
        name = table.spec.name
        asset_key = BRONZE_KEYS[name]

        def result(check_id: str, failures: int, **counts: int) -> AssetCheckResult:
            return check_result(check_id, asset_key, key, failures, **counts)  # noqa: B023

        m = table_metrics(df, table.spec, key)
        if name in MUST_HAVE_ROWS:
            out = [result("DQ1", int(m["rows"] == 0), rows=m["rows"])]
        else:
            out = [result("DQ2", 0, rows=m["rows"])]
        out += [
            result("DQ3", m["key_missing"], rows_missing_key=m["key_missing"]),
            result("DQ4", m["duplicate_keys"], duplicate_keys=m["duplicate_keys"]),
            result("DQ9", m["meta_bad"], rows_with_bad_metadata=m["meta_bad"]),
        ]
        if name == "submission":
            shared = shared_accessions(submission, existing_submission, key)
            out.append(result("DQ7", shared, shared_accessions=shared))
        else:
            orphans = orphan_accessions(df, submission)
            out.append(result("DQ6", orphans, orphan_accessions=orphans))
        if name == "infotable":
            bad = infotable_value_problems(df)
            out.append(
                result(
                    "DQ8",
                    sum(bad.values()),
                    bad_cusip=bad["cusip"],
                    bad_value=bad["value"],
                    bad_sshprnamt=bad["sshprnamt"],
                )
            )
        results[name] = out
    return results


def parse_staged(session: Any, uri: str, table: UnpackedTable) -> Any:
    """Split the staged lines on tabs with Spark built-ins and pick the declared columns."""
    from pyspark.sql import functions as F

    parts = F.split(F.col("value"), "\t", -1)
    lines = session.read.option("lineSep", "\n").text(uri).select(parts.alias("p"))

    expected = len(table.header) + 1  # the line number, then the fields
    bad = lines.filter(F.size("p") != expected).select(F.col("p")[0].alias("line")).limit(5)
    bad_lines = [row["line"] for row in bad.collect()]  # at most 5 rows
    if bad_lines:
        raise Form13fFormatError(
            f"{table.member}: lines {bad_lines} do not have {len(table.header)} fields "
            "(a tab or line break inside a value?)"
        )

    position = {name: i + 1 for i, name in enumerate(table.header)}
    columns = [
        (F.col("p")[position[name]] if name in position else F.lit(None).cast("string")).alias(name)
        for name in table.spec.columns
    ]
    return lines.select(*columns, F.col("p")[0].cast("long").alias("_source_line"))


@multi_asset(
    specs=[
        AssetSpec(
            BRONZE_KEYS[spec.name],
            deps=[RAW_ZIP_ASSET],
            partitions_def=FORM13F_PARTITIONS,
            group_name="form13f",
            description=f"SEC 13F {spec.name.upper()} rows, all columns as strings.",
        )
        for spec in TABLES
    ],
    check_specs=check_specs(BRONZE_KEYS),
    partitions_def=FORM13F_PARTITIONS,
    can_subset=False,
    compute_kind="spark",
    # Delta on S3 is safe with one writer at a time (ADR-002); dagster.yaml limits the pool.
    # One pool for every Bronze table, so at most one Bronze writer runs at a time.
    pool="delta_writes",
)
def bronze_form13f(
    context: AssetExecutionContext, spark: SparkResource, storage: LakehouseStorage
) -> Iterator[MaterializeResult[None] | AssetCheckResult]:
    """Load one data set's 7 tables into Bronze, replacing only that partition."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    key = context.partition_key
    dataset = DATASETS_BY_KEY[key]
    zip_path = raw_zip_path(dataset)
    zip_uri = storage.uri(zip_path)
    staging = f"{STAGING_PREFIX}/{key}"
    # Timezone-aware, so Spark stores the true UTC instant whatever the driver's OS timezone.
    ingested_at = datetime.now(UTC)
    run_id = context.run.run_id
    session = spark.get_session()

    storage.delete_prefix(staging)  # leftovers from a failed run
    with tempfile.TemporaryDirectory(prefix="form13f-bronze-") as tmp_dir:
        local_zip = Path(tmp_dir) / dataset.zip_name
        storage.download_to(zip_path, local_zip)
        tables = unpack(local_zip, Path(tmp_dir) / "unpacked")
        for table in tables:
            storage.put_staging(f"{staging}/{table.spec.name}.tsv", table.path)
    # From here on only the staged copies exist; `table.path` points at a deleted temp file.

    # Parse and check all 7 tables before writing any, so a bad row in one table never leaves
    # the others half-replaced for this partition. The frames are cached: the checks and the
    # write read them several times.
    frames: list[tuple[UnpackedTable, Any]] = []
    for table in tables:
        spec: TableSpec = table.spec
        df = parse_staged(session, storage.uri(f"{staging}/{spec.name}.tsv"), table).select(
            *spec.columns,
            F.lit(key).alias("_dataset"),
            F.lit(f"{zip_uri}/{table.member}").alias("_source_file"),
            "_source_line",
            F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
            F.lit(run_id).alias("_run_id"),
        )
        frames.append((table, df.persist(StorageLevel.MEMORY_AND_DISK)))

    try:
        submission_spec = next(spec for spec in TABLES if spec.name == "submission")
        existing = read_bronze(
            session, storage.table_path("bronze", bronze_table_name(submission_spec))
        )
        checks = pre_write_checks(frames, existing, key)
        blocked = sorted(
            f"{r.check_name} on {table}"
            for table, results in checks.items()
            for r in results
            if not r.passed and r.severity == AssetCheckSeverity.ERROR
        )
        if blocked:
            for results in checks.values():
                yield from results
            raise BronzeQualityError(
                f"{key}: blocking checks failed, nothing written: {', '.join(blocked)}"
            )

        for table, df in frames:
            spec = table.spec
            asset_key = BRONZE_KEYS[spec.name]
            path = storage.table_path("bronze", bronze_table_name(spec))
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", f"_dataset = '{key}'")
                .partitionBy("_dataset")
                .save(path)
            )

            written = (
                session.read.format("delta").load(path).where(F.col("_dataset") == key).count()
            )
            context.log.info("%s: %d rows for %s", spec.name, written, key)
            yield MaterializeResult(
                asset_key=asset_key,
                metadata={
                    "path": MetadataValue.path(path),
                    "row_count": MetadataValue.int(written),
                    "source_file": MetadataValue.text(f"{zip_uri}/{table.member}"),
                    "missing_optional_columns": MetadataValue.text(
                        ", ".join(sorted(set(spec.columns) - set(table.header))) or "none"
                    ),
                },
            )
            yield check_result(
                "DQ5",
                asset_key,
                key,
                abs(written - table.rows),
                written=written,
                file_lines=table.rows,
            )
            yield from checks[spec.name]
            if written != table.rows:
                raise BronzeQualityError(
                    f"{spec.name}: wrote {written} rows but the file has {table.rows} data lines"
                )
    finally:
        for _, df in frames:
            df.unpersist()

    storage.delete_prefix(staging)
