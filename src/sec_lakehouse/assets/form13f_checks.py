"""Data quality checks DQ1 to DQ9 for the Bronze 13F tables (spec, "Data quality rules").

The checks run inside `bronze_form13f`, on the parsed data of one data set:
- block checks (DQ1, DQ3, DQ4, DQ9) run before any table is written, so a failing data set
  never replaces what Bronze already holds; DQ5 compares the rows written with the file;
- warn checks (DQ2, DQ6, DQ7, DQ8) only record a count and let the run continue.

Every metric is one Spark aggregation that returns a single row, so nothing unbounded reaches
the driver.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import reduce
from typing import Any

from dagster import AssetCheckResult, AssetCheckSeverity, AssetCheckSpec, AssetKey, MetadataValue

from sec_lakehouse.sources.form13f.schemas import METADATA_COLUMNS, TABLES, TableSpec

# DQ1 needs rows in these tables; the other-manager tables can be empty (DQ2).
MUST_HAVE_ROWS = frozenset({"submission", "coverpage", "summarypage", "signature", "infotable"})
UNSIGNED_INT = r"^[0-9]+$"


@dataclass(frozen=True)
class Check:
    id: str
    name: str
    description: str
    blocking: bool
    tables: frozenset[str]


ALL_TABLES = frozenset(spec.name for spec in TABLES)

CHECKS: tuple[Check, ...] = (
    Check("DQ1", "dq1_has_rows", "DQ1: the data set has at least one row.", True, MUST_HAVE_ROWS),
    Check(
        "DQ2",
        "dq2_row_count",
        "DQ2: records the row count; the table may be empty.",
        False,
        ALL_TABLES - MUST_HAVE_ROWS,
    ),
    Check(
        "DQ3",
        "dq3_key_not_null",
        "DQ3: source key columns are not null or empty.",
        True,
        ALL_TABLES,
    ),
    Check(
        "DQ4",
        "dq4_key_unique",
        "DQ4: the source key is unique within the data set.",
        True,
        ALL_TABLES,
    ),
    Check(
        "DQ5",
        "dq5_rows_match_file",
        "DQ5: rows written equal the data lines in the file.",
        True,
        ALL_TABLES,
    ),
    Check(
        "DQ6",
        "dq6_accession_in_submission",
        "DQ6: counts accession numbers that are not in SUBMISSION for the same data set.",
        False,
        ALL_TABLES - {"submission"},
    ),
    Check(
        "DQ7",
        "dq7_accession_in_other_datasets",
        "DQ7: counts accession numbers that also appear in another data set.",
        False,
        frozenset({"submission"}),
    ),
    Check(
        "DQ8",
        "dq8_infotable_values",
        "DQ8: counts cusip values that are not 9 characters, and value or sshprnamt values that "
        "are not non-negative integers.",
        False,
        frozenset({"infotable"}),
    ),
    Check(
        "DQ9",
        "dq9_metadata_valid",
        "DQ9: metadata columns are not null and _dataset equals the partition key.",
        True,
        ALL_TABLES,
    ),
)
CHECKS_BY_ID = {check.id: check for check in CHECKS}


def check_specs(asset_keys: dict[str, AssetKey]) -> list[AssetCheckSpec]:
    return [
        AssetCheckSpec(
            check.name,
            asset=asset_keys[table],
            description=check.description,
            blocking=check.blocking,
        )
        for check in CHECKS
        for table in sorted(check.tables)
    ]


def check_result(
    check_id: str, asset_key: AssetKey, dataset: str, failures: int, **counts: int
) -> AssetCheckResult:
    """A check result where `failures` is the number of offending rows or keys (0 = pass).

    The data set is in the metadata because Dagster keeps check history per asset, not per
    partition: without it, the counts of 54 data sets could not be told apart.
    """
    check = CHECKS_BY_ID[check_id]
    metadata: dict[str, MetadataValue] = {"dataset": MetadataValue.text(dataset)}
    metadata.update({name: MetadataValue.int(value) for name, value in counts.items()})
    return AssetCheckResult(
        asset_key=asset_key,
        check_name=check.name,
        passed=failures == 0,
        severity=AssetCheckSeverity.ERROR if check.blocking else AssetCheckSeverity.WARN,
        metadata=metadata,
    )


def _any(conditions: Iterable[Any]) -> Any:
    return reduce(lambda a, b: a | b, conditions)


def table_metrics(df: Any, spec: TableSpec, key: str) -> dict[str, int]:
    """Row count, missing keys, duplicate keys and bad metadata for one parsed table."""
    from pyspark.sql import functions as F

    key_missing = _any(F.col(k).isNull() | (F.trim(F.col(k)) == "") for k in spec.keys)
    meta_bad = _any(F.col(c).isNull() for c in METADATA_COLUMNS) | (F.col("_dataset") != key)
    counts = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(key_missing, 1).otherwise(0)).alias("key_missing"),
        F.sum(F.when(meta_bad, 1).otherwise(0)).alias("meta_bad"),
    ).first()
    duplicate_keys = df.groupBy(*spec.keys).count().filter(F.col("count") > 1).count()
    return {
        "rows": int(counts["rows"]),
        "key_missing": int(counts["key_missing"] or 0),
        "meta_bad": int(counts["meta_bad"] or 0),
        "duplicate_keys": int(duplicate_keys),
    }


def orphan_accessions(df: Any, submission: Any) -> int:
    """DQ6: distinct accession numbers in `df` that SUBMISSION of the same data set lacks."""
    accessions = df.select("accession_number").distinct()
    known = submission.select("accession_number").distinct()
    return int(accessions.join(known, "accession_number", "left_anti").count())


def shared_accessions(submission: Any, existing: Any | None, key: str) -> int:
    """DQ7: distinct accession numbers of this data set that another data set already holds."""
    if existing is None:
        return 0
    from pyspark.sql import functions as F

    others = existing.where(F.col("_dataset") != key).select("accession_number").distinct()
    mine = submission.select("accession_number").distinct()
    return int(mine.join(others, "accession_number", "inner").count())


def infotable_value_problems(df: Any) -> dict[str, int]:
    """DQ8: counts of cusip, value and sshprnamt values outside the expected shape."""
    from pyspark.sql import functions as F

    def bad_int(column: str) -> Any:
        return F.col(column).isNull() | ~F.col(column).rlike(UNSIGNED_INT)

    counts = df.agg(
        F.sum(F.when(F.length(F.coalesce("cusip", F.lit(""))) != 9, 1).otherwise(0)).alias("cusip"),
        F.sum(F.when(bad_int("value"), 1).otherwise(0)).alias("value"),
        F.sum(F.when(bad_int("sshprnamt"), 1).otherwise(0)).alias("sshprnamt"),
    ).first()
    return {name: int(counts[name] or 0) for name in ("cusip", "value", "sshprnamt")}
