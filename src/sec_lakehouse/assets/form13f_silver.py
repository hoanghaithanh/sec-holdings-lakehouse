"""Silver layer, slice S1: typed 13F filings and the quarantine table (M2 spec, ADR-005).

One multi-asset handles one Bronze data set per run (one Dagster partition each, as in Bronze):

1. Guard: the Bronze tables it reads hold the data set, all from one Bronze run.
2. Owner: each accession belongs to the latest data set whose SUBMISSION holds it.
3. Cast, clean and split the data set's filings into Silver rows and quarantine rows.
4. Block checks before writing (SDQ4, SDQ6).
5. Work out the change set against what Silver already holds, and `MERGE` only that.
6. Replace the data set's quarantine partition.
7. Checks after writing.

Silver makes no HTTP request and never reads the SEC resource. Holdings (S2) and the effective
table (S3) join this asset in later slices.
"""

# No `from __future__ import annotations` here: Dagster inspects the context annotation at runtime.
from collections.abc import Iterator
from datetime import UTC, datetime
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

from sec_lakehouse.assets.form13f_bronze import BRONZE_KEYS, read_bronze
from sec_lakehouse.assets.form13f_silver_checks import (
    FILING_TABLE,
    QUARANTINE_TABLE,
    dates_and_types,
    key_problems,
    silver_check_result,
    silver_check_specs,
)
from sec_lakehouse.delta_tables import (
    DELETE,
    INSERT,
    OLD_DATASET,
    OP,
    UPDATE,
    change_set,
    ensure_table,
    merge_change_set,
    replace_partition,
)
from sec_lakehouse.resources import LakehouseStorage, SparkResource
from sec_lakehouse.sources.form13f.datasets import FORM13F_PARTITIONS
from sec_lakehouse.sources.form13f.silver_filing import (
    FILING_OUTPUT,
    FILING_SOURCES,
    filing_warn_counts,
    owner_map,
    split_filings,
)
from sec_lakehouse.sources.form13f.silver_schemas import (
    FILING,
    QUARANTINE,
    QUARANTINE_BLOCK_RATIO,
    QUARANTINE_BLOCK_RATIO_OVERRIDES,
    filing_schema,
    quarantine_schema,
)

SILVER_KEYS = {
    FILING_TABLE: AssetKey(["silver", FILING.name]),
    QUARANTINE_TABLE: AssetKey(["silver", QUARANTINE.name]),
}


class SilverQualityError(RuntimeError):
    """A blocking Silver check failed."""


class BronzeNotReadyError(RuntimeError):
    """The Bronze tables this run reads are missing the data set or are half loaded."""


def bronze_inputs(session: Any, storage: LakehouseStorage, key: str) -> dict[str, Any]:
    """The whole Bronze tables Silver reads, after the step-0 guard of ADR-005.

    Bronze writes its tables one after another with no cross-table transaction (ADR-002), so a
    failed Bronze run can leave some tables of a data set from an older run. All tables read must
    hold the data set and carry one and the same `_run_id`.
    """
    from pyspark.sql import functions as F

    tables: dict[str, Any] = {}
    run_ids: set[str] = set()
    for name in FILING_SOURCES:
        path = storage.table_path("bronze", f"form13f_{name}")
        df = read_bronze(session, path)
        if df is None:
            raise BronzeNotReadyError(f"Bronze table {path} does not exist: load Bronze first")
        stats = (
            df.where(F.col("_dataset") == key)
            .agg(F.count(F.lit(1)).alias("rows"), F.collect_set("_run_id").alias("runs"))
            .first()
        )  # one row; `runs` holds one value per Bronze run of the data set (a handful at most)
        if not stats["rows"]:
            raise BronzeNotReadyError(f"Bronze {name} has no rows for {key}: load Bronze first")
        run_ids.update(stats["runs"])
        tables[name] = df
    if len(run_ids) != 1:
        raise BronzeNotReadyError(
            f"Bronze {key} is half loaded (runs {sorted(run_ids)}): "
            f"rerun form13f_bronze_job for {key}"
        )
    return tables


@multi_asset(
    specs=[
        AssetSpec(
            SILVER_KEYS[FILING_TABLE],
            deps=[BRONZE_KEYS[name] for name in FILING_SOURCES],
            partitions_def=FORM13F_PARTITIONS,
            group_name="form13f_silver",
            description="One typed row per 13F filing (SUBMISSION + COVERPAGE + SUMMARYPAGE), "
            "Delta-partitioned by period_of_report.",
        ),
        AssetSpec(
            SILVER_KEYS[QUARANTINE_TABLE],
            deps=[BRONZE_KEYS[name] for name in FILING_SOURCES],
            partitions_def=FORM13F_PARTITIONS,
            group_name="form13f_silver",
            description="Bronze rows that failed a Silver rule, with reason codes and the raw "
            "row. Delta-partitioned by _dataset.",
        ),
    ],
    check_specs=silver_check_specs(SILVER_KEYS),
    partitions_def=FORM13F_PARTITIONS,
    can_subset=False,
    compute_kind="spark",
    # One Delta writer at a time across Bronze and Silver (ADR-002, ADR-005).
    pool="delta_writes",
)
def silver_form13f(
    context: AssetExecutionContext, spark: SparkResource, storage: LakehouseStorage
) -> Iterator[MaterializeResult[None] | AssetCheckResult]:
    """Refresh Silver filings and quarantine for one data set; re-runnable (ADR-005)."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    key = context.partition_key
    run_id = context.run.run_id
    processed_at = datetime.now(UTC)
    session: Any = spark.get_session()
    filing_key, quarantine_key = SILVER_KEYS[FILING_TABLE], SILVER_KEYS[QUARANTINE_TABLE]

    bronze = bronze_inputs(session, storage, key)
    mine = {name: df.where(F.col("_dataset") == key) for name, df in bronze.items()}
    owners = owner_map(bronze["submission"]).persist(StorageLevel.MEMORY_AND_DISK)
    split = split_filings(mine["submission"], mine["coverpage"], mine["summarypage"], owners, key)
    evaluated = split.evaluated.persist(StorageLevel.MEMORY_AND_DISK)
    cached = [owners, evaluated]
    try:
        kept = ~F.col("superseded")
        bad = F.size("reason_codes") > 0
        totals = evaluated.agg(
            F.sum(F.when(F.col("superseded"), 1).otherwise(0)).alias("superseded"),
            F.sum(F.when(kept & bad, 1).otherwise(0)).alias("quarantined"),
        ).first()
        # SDQ3 compares with the Bronze table itself, not the joined frame, so a join that
        # fanned out could not hide behind its own inflated count.
        bronze_rows = int(mine["submission"].count())
        superseded = int(totals["superseded"] or 0)
        quarantined = int(totals["quarantined"] or 0)

        # --- block checks before any write ------------------------------------------------
        run_date = processed_at.date()
        sdq4 = dates_and_types(split.good, filing_schema(), run_date)
        threshold = QUARANTINE_BLOCK_RATIO_OVERRIDES.get(key, QUARANTINE_BLOCK_RATIO)
        candidates = bronze_rows - superseded
        ratio = quarantined / candidates if candidates else 0.0
        pre_checks = [
            silver_check_result(
                "SDQ4", filing_key, key, sum(sdq4.values()), table=FILING_TABLE, metrics=sdq4
            ),
            silver_check_result(
                "SDQ6",
                filing_key,
                key,
                quarantined,
                table=FILING_TABLE,
                severity=AssetCheckSeverity.ERROR if ratio > threshold else AssetCheckSeverity.WARN,
                metrics={
                    "quarantined_filings": quarantined,
                    "candidate_filings": candidates,
                    "quarantine_ratio": round(ratio, 6),
                    "block_above": threshold,
                },
            ),
        ]
        blocked = [
            str(r.check_name)
            for r in pre_checks
            if not r.passed and r.severity == AssetCheckSeverity.ERROR
        ]
        if blocked:
            yield from pre_checks
            top = (
                evaluated.where(kept & bad)
                .select(F.explode("reason_codes").alias("code"))
                .groupBy("code")
                .count()
                .orderBy(F.desc("count"))
                .limit(3)
                .collect()
            )  # at most 3 rows
            top_reasons = ", ".join(f"{r['code']} x{r['count']}" for r in top)
            raise SilverQualityError(
                f"{key}: blocking checks failed, nothing written: {', '.join(blocked)}"
                + (f" (top reasons: {top_reasons})" if top_reasons else "")
            )

        # --- change set -------------------------------------------------------------------
        filing_path = storage.table_path("silver", FILING.name)
        quarantine_path = storage.table_path("silver", QUARANTINE.name)
        ensure_table(session, filing_path, filing_schema(), FILING.partition_by)
        ensure_table(session, quarantine_path, quarantine_schema(), QUARANTINE.partition_by)
        silver = session.read.format("delta").load(filing_path)

        # S_X: accessions this data set owns, plus those Silver says came from it.
        scope = (
            evaluated.where(F.col("owner") == key)
            .select("accession_number")
            .unionByName(silver.where(F.col("_dataset") == key).select("accession_number"))
            .distinct()
        )
        current = silver.join(scope, "accession_number", "left_semi").select(*FILING_OUTPUT)
        # P_X: quarter ends that passed BAD_PERIOD, so at most about 150 dates.
        periods = sorted(
            row["period_of_report"]
            for row in split.good.select("period_of_report")
            .unionByName(current.select("period_of_report"))
            .distinct()
            .collect()
        )
        changes = (
            change_set(split.good, current, FILING.keys, FILING_OUTPUT)
            .withColumn("_silver_processed_at", F.lit(processed_at).cast("timestamp"))
            .withColumn("_silver_run_id", F.lit(run_id))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        cached.append(changes)
        ops = {r[OP]: int(r["count"]) for r in changes.groupBy(OP).count().collect()}  # <= 3 rows
        taken_over_from = sorted(
            r[OLD_DATASET]
            for r in changes.where((F.col(OP) == UPDATE) & (F.col(OLD_DATASET) != key))
            .select(OLD_DATASET)
            .distinct()
            .collect()
        )  # data set keys, at most one per partition
        handed_back_to = sorted(
            str(r["owner"] or "none (gone from Bronze)")
            for r in changes.where(F.col(OP) == DELETE)
            .join(owners, "accession_number", "left")
            .where(F.coalesce(F.col("owner"), F.lit("")) != key)
            .select("owner")
            .distinct()
            .collect()
        )

        # --- writes -----------------------------------------------------------------------
        if ops:
            merge_change_set(
                session, filing_path, changes, FILING.keys, FILING.partition_by, periods
            )
        replace_partition(
            split.quarantine.withColumn(
                "_silver_processed_at", F.lit(processed_at).cast("timestamp")
            ).withColumn("_silver_run_id", F.lit(run_id)),
            quarantine_path,
            QUARANTINE.partition_by,
            key,
        )
        context.log.info(
            "%s: filings inserted %d, updated %d, deleted %d; quarantined %d; superseded %d",
            key,
            ops.get(INSERT, 0),
            ops.get(UPDATE, 0),
            ops.get(DELETE, 0),
            quarantined,
            superseded,
        )

        # --- checks after writing ---------------------------------------------------------
        filings = session.read.format("delta").load(filing_path)
        quarantine = (
            session.read.format("delta").load(quarantine_path).where(F.col("_dataset") == key)
        )
        silver_rows = filings.where(F.col("_dataset") == key).count()
        quarantine_counts = quarantine.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(F.col("source_table") == "submission", 1).otherwise(0)).alias("filings"),
            F.sum(
                F.when(
                    F.col("reason_codes").isNull()
                    | (F.size("reason_codes") == 0)
                    | F.col("raw_row").isNull(),
                    1,
                ).otherwise(0)
            ).alias("incomplete"),
        ).first()
        quarantine_rows = int(quarantine_counts["rows"])
        quarantined_written = int(quarantine_counts["filings"] or 0)
        incomplete = int(quarantine_counts["incomplete"] or 0)
        filing_keys = key_problems(filings, FILING.keys)
        quarantine_keys = key_problems(quarantine, QUARANTINE.keys)
        missing_in_bronze = (
            filings.where(F.col("_dataset") == key)
            .join(mine["submission"], "accession_number", "left_anti")
            .count()
        )
        unbalanced = bronze_rows - (silver_rows + quarantined_written + superseded)
        warn_counts = filing_warn_counts(evaluated)
        reasons = {
            r["code"]: int(r["count"])
            for r in quarantine.select(F.explode("reason_codes").alias("code"))
            .groupBy("code")
            .count()
            .collect()
        }  # one row per reason code, at most 5

        post_filing = [
            silver_check_result(
                "SDQ1",
                filing_key,
                key,
                int(silver_rows == 0),
                table=FILING_TABLE,
                metrics={"rows": silver_rows},
            ),
            silver_check_result(
                "SDQ2",
                filing_key,
                key,
                sum(filing_keys.values()),
                table=FILING_TABLE,
                metrics=filing_keys,
            ),
            silver_check_result(
                "SDQ3",
                filing_key,
                key,
                abs(unbalanced),
                table=FILING_TABLE,
                metrics={
                    "bronze_rows": bronze_rows,
                    "silver_rows": silver_rows,
                    "quarantined": quarantined_written,
                    "superseded": superseded,
                },
            ),
            silver_check_result(
                "SDQ7",
                filing_key,
                key,
                missing_in_bronze,
                table=FILING_TABLE,
                metrics={"missing_in_bronze": missing_in_bronze},
            ),
            silver_check_result(
                "SDQ10",
                filing_key,
                key,
                sum(warn_counts.values()),
                table=FILING_TABLE,
                metrics=warn_counts,
            ),
        ]
        post_quarantine = [
            silver_check_result(
                "SDQ2",
                quarantine_key,
                key,
                sum(quarantine_keys.values()) + incomplete,
                table=QUARANTINE_TABLE,
                metrics={**quarantine_keys, "rows_without_reason_or_raw_row": incomplete},
            ),
            silver_check_result(
                "QROWS",
                quarantine_key,
                key,
                0,
                table=QUARANTINE_TABLE,
                metrics={"rows": quarantine_rows},
            ),
        ]

        yield MaterializeResult(
            asset_key=filing_key,
            metadata={
                "path": MetadataValue.path(filing_path),
                "rows_for_dataset": MetadataValue.int(silver_rows),
                "inserted": MetadataValue.int(ops.get(INSERT, 0)),
                "updated": MetadataValue.int(ops.get(UPDATE, 0)),
                "deleted": MetadataValue.int(ops.get(DELETE, 0)),
                "quarantined": MetadataValue.int(quarantined),
                "superseded": MetadataValue.int(superseded),
                "periods_touched": MetadataValue.int(len(periods)),
                "periods": MetadataValue.text(", ".join(p.isoformat() for p in periods) or "none"),
                "taken_over_from": MetadataValue.text(", ".join(taken_over_from) or "none"),
                "handed_back_to": MetadataValue.text(", ".join(handed_back_to) or "none"),
            },
        )
        yield from pre_checks
        yield from post_filing
        yield MaterializeResult(
            asset_key=quarantine_key,
            metadata={
                "path": MetadataValue.path(quarantine_path),
                "row_count": MetadataValue.int(quarantine_rows),
                "reasons": MetadataValue.json(reasons),
            },
        )
        yield from post_quarantine

        failed = sorted(
            f"{r.check_name} on {table}"
            for table, results in ((FILING.name, post_filing), (QUARANTINE.name, post_quarantine))
            for r in results
            if not r.passed and r.severity == AssetCheckSeverity.ERROR
        )
        if failed:
            raise SilverQualityError(
                f"{key}: blocking checks failed after writing: {', '.join(failed)}. "
                "Fix the cause and rerun this data set."
            )
    finally:
        for df in cached:
            df.unpersist()
