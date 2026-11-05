"""M0 smoke-test asset: proves Dagster -> Spark -> Delta works end to end."""

# No `from __future__ import annotations` here: Dagster inspects the context annotation at runtime.
from datetime import UTC, datetime

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from sec_lakehouse.resources import LakehouseStorage, SparkResource

# A few well-known 13F filers, so the smoke test already looks like the domain.
SAMPLE_MANAGERS = [
    ("0001067983", "BERKSHIRE HATHAWAY INC"),
    ("0001364742", "BLACKROCK INC."),
    ("0000102909", "VANGUARD GROUP INC"),
]


@asset(group_name="smoke", compute_kind="spark")
def hello_delta(
    context: AssetExecutionContext, spark: SparkResource, storage: LakehouseStorage
) -> MaterializeResult[None]:
    """Write a tiny Delta table of 13F filers and read it back."""
    session = spark.get_session()
    path = storage.table_path("bronze", "hello_delta")
    ingested_at = datetime.now(UTC).replace(tzinfo=None)

    df = session.createDataFrame(
        [(cik, name, ingested_at) for cik, name in SAMPLE_MANAGERS],
        schema="cik string, manager_name string, _ingested_at timestamp",
    )
    df.write.format("delta").mode("overwrite").save(path)

    row_count = session.read.format("delta").load(path).count()
    version = session.sql(f"DESCRIBE HISTORY delta.`{path}`").agg({"version": "max"}).first()
    context.log.info("Wrote %s rows to %s", row_count, path)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "row_count": MetadataValue.int(row_count),
            "delta_version": MetadataValue.int(int(version[0]) if version else 0),
        }
    )
