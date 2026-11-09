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
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)

from sec_lakehouse.assets.form13f_raw import RAW_ZIP_ASSET, raw_zip_path
from sec_lakehouse.resources import LakehouseStorage, SparkResource
from sec_lakehouse.sources.form13f.datasets import DATASETS_BY_KEY, FORM13F_PARTITIONS
from sec_lakehouse.sources.form13f.schemas import TABLES, TableSpec, bronze_table_name
from sec_lakehouse.sources.form13f.unpack import Form13fFormatError, UnpackedTable, unpack

BRONZE_KEYS = {spec.name: AssetKey(["bronze", bronze_table_name(spec)]) for spec in TABLES}
STAGING_PREFIX = "staging/form13f"


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
    partitions_def=FORM13F_PARTITIONS,
    can_subset=False,
    compute_kind="spark",
    # Delta on S3 is safe with one writer at a time (ADR-002); dagster.yaml limits the pool.
    pool="delta_writes",
)
def bronze_form13f(
    context: AssetExecutionContext, spark: SparkResource, storage: LakehouseStorage
) -> Iterator[MaterializeResult[None]]:
    """Load one data set's 7 tables into Bronze, replacing only that partition."""
    from pyspark.sql import functions as F

    key = context.partition_key
    dataset = DATASETS_BY_KEY[key]
    zip_path = raw_zip_path(dataset)
    zip_uri = storage.uri(zip_path)
    staging = f"{STAGING_PREFIX}/{key}"
    ingested_at = datetime.now(UTC).replace(tzinfo=None)
    session = spark.get_session()

    storage.delete_prefix(staging)  # leftovers from a failed run
    with tempfile.TemporaryDirectory(prefix="form13f-bronze-") as tmp_dir:
        local_zip = Path(tmp_dir) / dataset.zip_name
        storage.download_to(zip_path, local_zip)
        tables = unpack(local_zip, Path(tmp_dir) / "unpacked")
        for table in tables:
            storage.put_staging(f"{staging}/{table.spec.name}.tsv", table.path)

    for table in tables:
        spec: TableSpec = table.spec
        path = storage.table_path("bronze", bronze_table_name(spec))
        df = parse_staged(session, storage.uri(f"{staging}/{spec.name}.tsv"), table).select(
            *spec.columns,
            F.lit(key).alias("_dataset"),
            F.lit(f"{zip_uri}/{table.member}").alias("_source_file"),
            "_source_line",
            F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
            F.lit(context.run_id).alias("_run_id"),
        )
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_dataset = '{key}'")
            .partitionBy("_dataset")
            .save(path)
        )

        written = session.read.format("delta").load(path).where(F.col("_dataset") == key).count()
        if written != table.rows:
            raise RuntimeError(
                f"{spec.name}: wrote {written} rows but the file has {table.rows} data lines"
            )
        context.log.info("%s: %d rows for %s", spec.name, written, key)
        yield MaterializeResult(
            asset_key=BRONZE_KEYS[spec.name],
            metadata={
                "path": MetadataValue.path(path),
                "row_count": MetadataValue.int(written),
                "source_file": MetadataValue.text(f"{zip_uri}/{table.member}"),
                "missing_optional_columns": MetadataValue.text(
                    ", ".join(sorted(set(spec.columns) - set(table.header))) or "none"
                ),
            },
        )

    storage.delete_prefix(staging)
