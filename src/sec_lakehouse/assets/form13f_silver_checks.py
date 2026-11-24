"""Asset checks of the Silver 13F tables, slices S1 and S2 (M2 spec, "Data quality rules";
ADR-005).

Like Bronze, the checks run inside the asset, in two groups:
- before any write (nothing is written if one blocks): SDQ4 dates and types, SDQ5 value unit,
  SDQ6 quarantine rate;
- after the writes: SDQ1 rows, SDQ2 keys, SDQ3 conservation, SDQ7 referential, and the warn
  counts SDQ10, SDQ11, SDQ13 and the quarantine row count.

SDQ6 is one check with two levels: it warns when any row is quarantined and blocks above the
threshold. Every metric is a Spark aggregation that returns one row.
"""

from __future__ import annotations

from collections.abc import Mapping
from functools import reduce
from typing import Any

from dagster import AssetCheckResult, AssetCheckSeverity, AssetCheckSpec, AssetKey, MetadataValue

from sec_lakehouse.assets.form13f_checks import Check

FILING_TABLE = "filing"
HOLDING_TABLE = "holding"
QUARANTINE_TABLE = "quarantine"
_FILING = frozenset({FILING_TABLE})
_HOLDING = frozenset({HOLDING_TABLE})
_QUARANTINE = frozenset({QUARANTINE_TABLE})
_ROWS = _FILING | _HOLDING

SILVER_CHECKS: tuple[Check, ...] = (
    Check("SDQ1", "sdq1_has_rows", "SDQ1: the data set has at least one Silver row.", True, _ROWS),
    Check(
        "SDQ2",
        "sdq2_key_unique",
        "SDQ2: the key is not null and unique (filing: whole table; holding: the accessions "
        "this run is responsible for). On quarantine also: every row has reason codes and its "
        "raw Bronze row.",
        True,
        _ROWS | _QUARANTINE,
    ),
    Check(
        "SDQ3",
        "sdq3_rows_conserved",
        "SDQ3: Bronze rows of the data set (SUBMISSION for filing, INFOTABLE for holding) = "
        "Silver rows + quarantined rows + rows superseded by a later data set.",
        True,
        _ROWS,
    ),
    Check(
        "SDQ4",
        "sdq4_dates_and_types",
        "SDQ4: the rows match the declared schema, filing_date is between 2013-05-01 and the "
        "run date, and period_of_report <= filing_date. Checked before writing.",
        True,
        _FILING,
    ),
    Check(
        "SDQ5",
        "sdq5_value_unit",
        "SDQ5: value_unit is THOUSANDS before 2023-01-03 and DOLLARS from then on (by filing "
        "date), and the dollar amount is the reported one x 1000 or x 1. Checked before writing.",
        True,
        _ROWS,
    ),
    Check(
        "SDQ6",
        "sdq6_quarantine_rate",
        "SDQ6: warns when any row is quarantined; blocks (nothing written) above the "
        "threshold, 1% unless the data set has a reviewed override.",
        True,
        _ROWS,
    ),
    Check(
        "SDQ7",
        "sdq7_referential",
        "SDQ7: filing: every Silver filing row of the data set has its Bronze SUBMISSION row. "
        "holding: every Silver holding row of the data set has a Silver filing row with the "
        "same accession, period and data set.",
        True,
        _ROWS,
    ),
    Check(
        "SDQ10",
        "sdq10_unrecognised_values",
        "SDQ10: counts unrecognised Y/N flags, unknown or misplaced amendment types, isamendment "
        "mismatches, bad optional dates and CIKs that are not 10 digits.",
        False,
        _FILING,
    ),
    Check(
        "SDQ11",
        "sdq11_differing_duplicates",
        "SDQ11: counts keys of the data set that another data set also holds with different "
        "raw Bronze content (the latest data set wins either way).",
        False,
        _ROWS,
    ),
    Check(
        "SDQ13",
        "sdq13_cusip_format",
        "SDQ13: counts holding lines whose CUSIP is not 9 letters or digits (kept, flagged in "
        "cusip_format_ok).",
        False,
        _HOLDING,
    ),
    Check(
        "QROWS",
        "quarantine_row_count",
        "Records the quarantined rows of the data set (not an SDQ rule); the count may be 0.",
        False,
        _QUARANTINE,
    ),
)
SILVER_CHECKS_BY_ID = {check.id: check for check in SILVER_CHECKS}


def silver_check_specs(asset_keys: dict[str, AssetKey]) -> list[AssetCheckSpec]:
    return [
        AssetCheckSpec(
            check.name,
            asset=asset_keys[table],
            description=check.description,
            blocking=check.blocks(table),
        )
        for check in SILVER_CHECKS
        for table in sorted(check.tables)
    ]


def silver_check_result(
    check_id: str,
    asset_key: AssetKey,
    dataset: str,
    failures: int,
    *,
    table: str,
    metrics: Mapping[str, int | float | str],
    severity: AssetCheckSeverity | None = None,
) -> AssetCheckResult:
    """A result where `failures` counts offending rows or keys (0 = pass).

    `metrics` go to the check's metadata. `severity` overrides the declared level, for SDQ6,
    which warns below its threshold.
    """
    check = SILVER_CHECKS_BY_ID[check_id]
    if table not in check.tables:
        raise ValueError(f"{check.name} is not declared for table {table!r}")
    metadata: dict[str, MetadataValue] = {"dataset": MetadataValue.text(dataset)}
    for name, value in metrics.items():
        if isinstance(value, bool | str):
            metadata[name] = MetadataValue.text(str(value))
        elif isinstance(value, int):
            metadata[name] = MetadataValue.int(value)
        else:
            metadata[name] = MetadataValue.float(value)
    if severity is None:
        severity = AssetCheckSeverity.ERROR if check.blocks(table) else AssetCheckSeverity.WARN
    return AssetCheckResult(
        asset_key=asset_key,
        check_name=check.name,
        passed=failures == 0,
        severity=severity,
        metadata=metadata,
    )


def key_problems(df: Any, keys: tuple[str, ...]) -> dict[str, int]:
    """Rows with a null or empty key part, and keys that appear more than once."""
    from pyspark.sql import functions as F

    missing = reduce(
        lambda a, b: a | b,
        (F.col(k).isNull() | (F.trim(F.col(k).cast("string")) == "") for k in keys),
    )
    null_keys = df.where(missing).count()
    duplicates = df.groupBy(*keys).count().where(F.col("count") > 1).count()
    return {"null_keys": int(null_keys), "duplicate_keys": int(duplicates)}


def dates_and_types(good: Any, schema: Any, run_date: Any) -> dict[str, int]:
    """SDQ4 on the rows about to be written.

    The cleaning rules already quarantine missing dates and periods after the filing date, so
    on correct code only the date range can fail here. The other counts are a safety net
    against a regression in those rules; keep them.
    """
    from pyspark.sql import functions as F

    from sec_lakehouse.sources.form13f.silver_schemas import EARLIEST_FILING_DATE

    declared = [(f.name, f.dataType) for f in schema.fields]
    actual = [(f.name, f.dataType) for f in good.schema.fields]
    wanted = [pair for pair in declared if pair[0] in dict(actual)]
    row = good.agg(
        F.sum(
            F.when(
                F.col("filing_date").isNull()
                | (F.col("filing_date") < F.lit(EARLIEST_FILING_DATE))
                | (F.col("filing_date") > F.lit(run_date)),
                1,
            ).otherwise(0)
        ).alias("filing_date_out_of_range"),
        F.sum(
            F.when(
                F.col("period_of_report").isNull()
                | (F.col("period_of_report") > F.col("filing_date")),
                1,
            ).otherwise(0)
        ).alias("period_after_filing"),
    ).first()
    return {
        "schema_mismatch": int(actual != wanted),
        "filing_date_out_of_range": int(row["filing_date_out_of_range"] or 0),
        "period_after_filing": int(row["period_after_filing"] or 0),
    }


def value_unit_problems(good: Any, pairs: tuple[tuple[str, str], ...]) -> dict[str, int]:
    """SDQ5 on the rows about to be written. `pairs` are `(reported, usd)` column names.

    Written independently of the cleaning code on purpose, so a regression there shows here.
    """
    from pyspark.sql import functions as F

    from sec_lakehouse.sources.form13f.silver_schemas import DOLLARS_FROM

    thousands = F.col("filing_date") < F.lit(DOLLARS_FROM)
    expected = F.when(thousands, F.lit("THOUSANDS")).otherwise(F.lit("DOLLARS"))
    factor = F.when(thousands, F.lit(1000)).otherwise(F.lit(1))
    wrong_unit = ~F.col("value_unit").eqNullSafe(expected)
    wrong_amount = reduce(
        lambda a, b: a | b,
        (
            ~F.col(usd).eqNullSafe((F.col(reported) * factor).cast("decimal(20,0)"))
            for reported, usd in pairs
        ),
    )
    row = good.agg(
        F.sum(F.when(wrong_unit, 1).otherwise(0)).alias("wrong_unit"),
        F.sum(F.when(wrong_amount, 1).otherwise(0)).alias("wrong_usd_amount"),
    ).first()
    return {
        "wrong_unit": int(row["wrong_unit"] or 0),
        "wrong_usd_amount": int(row["wrong_usd_amount"] or 0),
    }


def differing_duplicates(copies: Any, keys: tuple[str, ...], dataset: str) -> dict[str, int]:
    """SDQ11: keys of `dataset` that another data set also holds, and how many differ.

    `copies` has `keys`, `_dataset` and `_content` (a hash of the raw Bronze columns), one row
    per key and data set, already limited to the accessions `dataset` shares with others.
    """
    from pyspark.sql import functions as F

    per_key = (
        copies.groupBy(*keys)
        .agg(
            F.max(F.when(F.col("_dataset") == dataset, 1).otherwise(0)).alias("mine"),
            F.countDistinct("_dataset").alias("copies"),
            F.countDistinct("_content").alias("versions"),
        )
        .where((F.col("mine") == 1) & (F.col("copies") > 1))
    )
    row = per_key.agg(
        F.count(F.lit(1)).alias("shared"),
        F.sum(F.when(F.col("versions") > 1, 1).otherwise(0)).alias("differing"),
    ).first()
    return {
        "shared_keys": int(row["shared"] or 0),
        "differing_keys": int(row["differing"] or 0),
    }


def content_copies(
    bronze: Any,
    shared: Any,
    datasets: list[str],
    keys: tuple[str, ...],
    columns: tuple[str, ...],
) -> Any:
    """`keys`, `_dataset`, `_content` for the Bronze rows of `shared` accessions, for SDQ11.

    `datasets` lists the data sets that hold any of them, so the read prunes to those
    partitions. `_content` hashes the raw source columns, not the metadata columns.
    """
    from pyspark.sql import functions as F

    return (
        bronze.where(F.col("_dataset").isin(*datasets))
        .join(shared, "accession_number", "left_semi")
        .select(*keys, "_dataset", F.xxhash64(*columns).cast("string").alias("_content"))
    )
