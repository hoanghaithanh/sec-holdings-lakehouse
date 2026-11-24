"""Silver layer, slices S1 and S2: typed 13F filings, holdings and quarantine (M2 spec, ADR-005).

One multi-asset handles one Bronze data set per run (one Dagster partition each, as in Bronze):

1. Guard: the Bronze tables it reads hold the data set, all from one Bronze run.
2. Owner: each accession belongs to the latest data set whose SUBMISSION holds it.
3. Cast, clean and split the data set's filings, then its holding lines (which take their
   filing fields, and the value unit, from the clean filings), into Silver rows and quarantine
   rows.
4. Block checks before writing (SDQ4, SDQ5, SDQ6).
5. Work out the change sets against what Silver already holds, and `MERGE` only those:
   holdings first, then filings (ADR-005, "Why holdings first").
6. Replace the data set's quarantine partition.
7. Checks after writing.

Silver makes no HTTP request and never reads the SEC resource. The effective table (S3) joins
this asset in a later slice.
"""

# No `from __future__ import annotations` here: Dagster inspects the context annotation at runtime.
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
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
    HOLDING_TABLE,
    QUARANTINE_TABLE,
    content_copies,
    dates_and_types,
    differing_duplicates,
    key_problems,
    silver_check_result,
    silver_check_specs,
    value_unit_problems,
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
from sec_lakehouse.sources.form13f.schemas import TABLES
from sec_lakehouse.sources.form13f.silver_filing import (
    FILING_OUTPUT,
    FILING_SOURCES,
    filing_warn_counts,
    owner_map,
    split_filings,
)
from sec_lakehouse.sources.form13f.silver_holding import (
    HOLDING_OUTPUT,
    HOLDING_SOURCES,
    INFOTABLE_COLUMNS,
    split_holdings,
)
from sec_lakehouse.sources.form13f.silver_schemas import (
    FILING,
    HOLDING,
    QUARANTINE,
    QUARANTINE_BLOCK_RATIO,
    QUARANTINE_BLOCK_RATIO_OVERRIDES,
    filing_schema,
    holding_schema,
    quarantine_schema,
)

SILVER_KEYS = {
    FILING_TABLE: AssetKey(["silver", FILING.name]),
    HOLDING_TABLE: AssetKey(["silver", HOLDING.name]),
    QUARANTINE_TABLE: AssetKey(["silver", QUARANTINE.name]),
}
SILVER_SOURCES = FILING_SOURCES + HOLDING_SOURCES
_SOURCE_COLUMNS = {spec.name: spec.columns for spec in TABLES}


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
    for name in SILVER_SOURCES:
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


@dataclass(frozen=True)
class Counts:
    """Bronze rows of the data set for one Silver table, and how they were split."""

    bronze: int
    superseded: int
    quarantined: int

    @property
    def candidates(self) -> int:
        """Rows this data set is responsible for: the base of the SDQ6 rate."""
        return self.bronze - self.superseded


def split_counts(bronze: Any, evaluated: Any) -> Counts:
    from pyspark.sql import functions as F

    kept = ~F.col("superseded")
    totals = evaluated.agg(
        F.sum(F.when(F.col("superseded"), 1).otherwise(0)).alias("superseded"),
        F.sum(F.when(kept & (F.size("reason_codes") > 0), 1).otherwise(0)).alias("quarantined"),
    ).first()
    # SDQ3 compares with the Bronze table itself, not the joined frame, so a join that fanned
    # out could not hide behind its own inflated count.
    return Counts(
        bronze=int(bronze.count()),
        superseded=int(totals["superseded"] or 0),
        quarantined=int(totals["quarantined"] or 0),
    )


def quarantine_rate(
    asset_key: AssetKey, key: str, table: str, counts: Counts, noun: str
) -> AssetCheckResult:
    """SDQ6: warn above 0 quarantined rows, block above the data set's threshold."""
    threshold = QUARANTINE_BLOCK_RATIO_OVERRIDES.get(key, QUARANTINE_BLOCK_RATIO)
    ratio = counts.quarantined / counts.candidates if counts.candidates else 0.0
    return silver_check_result(
        "SDQ6",
        asset_key,
        key,
        counts.quarantined,
        table=table,
        severity=AssetCheckSeverity.ERROR if ratio > threshold else AssetCheckSeverity.WARN,
        metrics={
            f"quarantined_{noun}": counts.quarantined,
            f"candidate_{noun}": counts.candidates,
            "quarantine_ratio": round(ratio, 6),
            "block_above": threshold,
        },
    )


def blocking_failures(results: list[AssetCheckResult]) -> list[str]:
    return [
        f"{r.check_name} on {r.asset_key.path[-1] if r.asset_key else '?'}"
        for r in results
        if not r.passed and r.severity == AssetCheckSeverity.ERROR
    ]


def duplicate_copies(bronze: dict[str, Any], mine: dict[str, Any], key: str) -> dict[str, Any]:
    """Raw-content hashes of the accessions this data set shares with others, for SDQ11.

    Returns `{}` when no other data set holds any of its accessions (the usual case), so the
    check costs one small read of Bronze SUBMISSION. Otherwise the Bronze reads prune to the
    data sets that share an accession.
    """
    from pyspark.sql import functions as F

    others = (
        bronze["submission"]
        .where(F.col("_dataset") != key)
        .select("accession_number", "_dataset")
        .join(mine["submission"].select("accession_number"), "accession_number", "left_semi")
    )
    datasets = sorted(
        r["_dataset"] for r in others.select("_dataset").distinct().collect()
    )  # data set keys, at most one per partition
    if not datasets:
        return {}
    shared = others.select("accession_number").distinct()
    datasets.append(key)
    per_table = [
        content_copies(
            bronze[name], shared, datasets, ("accession_number",), _SOURCE_COLUMNS[name]
        ).withColumn("_content", F.concat(F.lit(f"{name}:"), "_content"))
        for name in FILING_SOURCES
    ]
    filing = (
        per_table[0]
        .unionByName(per_table[1])
        .unionByName(per_table[2])
        .groupBy("accession_number", "_dataset")
        .agg(F.concat_ws("|", F.array_sort(F.collect_list("_content"))).alias("_content"))
    )
    holding = content_copies(
        bronze["infotable"],
        shared,
        datasets,
        ("accession_number", "infotable_sk"),
        INFOTABLE_COLUMNS,
    )
    return {FILING_TABLE: filing, HOLDING_TABLE: holding}


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
            SILVER_KEYS[HOLDING_TABLE],
            deps=[BRONZE_KEYS[name] for name in SILVER_SOURCES],
            partitions_def=FORM13F_PARTITIONS,
            group_name="form13f_silver",
            description="One typed row per 13F holding line (INFOTABLE), with the filing's "
            "fields and value_usd in dollars. Delta-partitioned by period_of_report.",
        ),
        AssetSpec(
            SILVER_KEYS[QUARANTINE_TABLE],
            deps=[BRONZE_KEYS[name] for name in SILVER_SOURCES],
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
    """Refresh Silver filings, holdings and quarantine for one data set; re-runnable (ADR-005)."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    key = context.partition_key
    run_id = context.run.run_id
    processed_at = datetime.now(UTC)
    session: Any = spark.get_session()
    filing_key = SILVER_KEYS[FILING_TABLE]
    holding_key = SILVER_KEYS[HOLDING_TABLE]
    quarantine_key = SILVER_KEYS[QUARANTINE_TABLE]

    bronze = bronze_inputs(session, storage, key)
    mine = {name: df.where(F.col("_dataset") == key) for name, df in bronze.items()}
    owners = owner_map(bronze["submission"]).persist(StorageLevel.MEMORY_AND_DISK)
    fsplit = split_filings(mine["submission"], mine["coverpage"], mine["summarypage"], owners, key)
    f_evaluated = fsplit.evaluated.persist(StorageLevel.MEMORY_AND_DISK)
    hsplit = split_holdings(mine["infotable"], fsplit.good, owners, key)
    h_evaluated = hsplit.evaluated.persist(StorageLevel.MEMORY_AND_DISK)
    cached = [owners, f_evaluated, h_evaluated]
    try:
        f_counts = split_counts(mine["submission"], f_evaluated)
        h_counts = split_counts(mine["infotable"], h_evaluated)

        # --- block checks before any write ------------------------------------------------
        sdq4 = dates_and_types(fsplit.good, filing_schema(), processed_at.date())
        f_sdq5 = value_unit_problems(
            fsplit.good, (("table_value_total_reported", "table_value_total_usd"),)
        )
        h_sdq5 = value_unit_problems(hsplit.good, (("value_reported", "value_usd"),))
        pre_filing = [
            silver_check_result(
                "SDQ4", filing_key, key, sum(sdq4.values()), table=FILING_TABLE, metrics=sdq4
            ),
            silver_check_result(
                "SDQ5", filing_key, key, sum(f_sdq5.values()), table=FILING_TABLE, metrics=f_sdq5
            ),
            quarantine_rate(filing_key, key, FILING_TABLE, f_counts, "filings"),
        ]
        pre_holding = [
            silver_check_result(
                "SDQ5",
                holding_key,
                key,
                sum(h_sdq5.values()),
                table=HOLDING_TABLE,
                metrics=h_sdq5,
            ),
            quarantine_rate(holding_key, key, HOLDING_TABLE, h_counts, "holdings"),
        ]
        blocked = blocking_failures(pre_filing + pre_holding)
        if blocked:
            yield from pre_filing
            yield from pre_holding
            top = (
                fsplit.quarantine.select("reason_codes")
                .unionByName(hsplit.quarantine.select("reason_codes"))
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

        # --- change sets ------------------------------------------------------------------
        filing_path = storage.table_path("silver", FILING.name)
        holding_path = storage.table_path("silver", HOLDING.name)
        quarantine_path = storage.table_path("silver", QUARANTINE.name)
        ensure_table(session, filing_path, filing_schema(), FILING.partition_by)
        ensure_table(session, holding_path, holding_schema(), HOLDING.partition_by)
        ensure_table(session, quarantine_path, quarantine_schema(), QUARANTINE.partition_by)
        silver_filings = session.read.format("delta").load(filing_path)
        silver_holdings = session.read.format("delta").load(holding_path)

        # S_X: accessions this data set owns, plus those Silver says came from it.
        scope = (
            f_evaluated.where(F.col("owner") == key)
            .select("accession_number")
            .unionByName(silver_filings.where(F.col("_dataset") == key).select("accession_number"))
            .distinct()
        )
        current_filings = silver_filings.join(scope, "accession_number", "left_semi").select(
            *FILING_OUTPUT
        )
        # P_X: quarter ends that passed BAD_PERIOD, so at most about 150 dates. Every Silver
        # holding row has a Silver filing row with the same accession and period (SDQ7), so the
        # filing table alone says which quarters may hold rows of S_X.
        periods: list[date] = sorted(
            row["period_of_report"]
            for row in fsplit.good.select("period_of_report")
            .unionByName(current_filings.select("period_of_report"))
            .distinct()
            .collect()
        )
        # A filing and its lines always come from one data set (SDQ7), so the current holding
        # rows of S_X carry this data set or one the current filing rows name: a handful of
        # keys, which let Delta skip files by their `_dataset` min/max statistics.
        datasets_in_scope = sorted(
            {key} | {r["_dataset"] for r in current_filings.select("_dataset").distinct().collect()}
        )
        current_holdings = (
            silver_holdings.where(
                F.col("period_of_report").isin(*periods)
                & F.col("_dataset").isin(*datasets_in_scope)
            )
            .join(scope, "accession_number", "left_semi")
            .select(*HOLDING_OUTPUT)
        )

        def changes_of(new: Any, old: Any, keys: tuple[str, ...], columns: list[str]) -> Any:
            df = (
                change_set(new, old, keys, columns)
                .withColumn("_silver_processed_at", F.lit(processed_at).cast("timestamp"))
                .withColumn("_silver_run_id", F.lit(run_id))
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            cached.append(df)
            return df

        f_changes = changes_of(fsplit.good, current_filings, FILING.keys, FILING_OUTPUT)
        h_changes = changes_of(hsplit.good, current_holdings, HOLDING.keys, HOLDING_OUTPUT)
        f_ops = {r[OP]: int(r["count"]) for r in f_changes.groupBy(OP).count().collect()}
        h_ops = {r[OP]: int(r["count"]) for r in h_changes.groupBy(OP).count().collect()}
        taken_over_from = sorted(
            r[OLD_DATASET]
            for r in f_changes.where((F.col(OP) == UPDATE) & (F.col(OLD_DATASET) != key))
            .select(OLD_DATASET)
            .distinct()
            .collect()
        )  # data set keys, at most one per partition
        handed_back_to = sorted(
            str(r["owner"] or "none (gone from Bronze)")
            for r in f_changes.where(F.col(OP) == DELETE)
            .join(owners, "accession_number", "left")
            .where(F.coalesce(F.col("owner"), F.lit("")) != key)
            .select("owner")
            .distinct()
            .collect()
        )

        # --- writes: holdings, then filings, then quarantine ------------------------------
        if h_ops:
            merge_change_set(
                session, holding_path, h_changes, HOLDING.keys, HOLDING.partition_by, periods
            )
        if f_ops:
            merge_change_set(
                session, filing_path, f_changes, FILING.keys, FILING.partition_by, periods
            )
        replace_partition(
            fsplit.quarantine.unionByName(hsplit.quarantine)
            .withColumn("_silver_processed_at", F.lit(processed_at).cast("timestamp"))
            .withColumn("_silver_run_id", F.lit(run_id)),
            quarantine_path,
            QUARANTINE.partition_by,
            key,
        )
        context.log.info(
            "%s: filings inserted %d, updated %d, deleted %d, quarantined %d, superseded %d; "
            "holdings inserted %d, updated %d, deleted %d, quarantined %d, superseded %d",
            key,
            f_ops.get(INSERT, 0),
            f_ops.get(UPDATE, 0),
            f_ops.get(DELETE, 0),
            f_counts.quarantined,
            f_counts.superseded,
            h_ops.get(INSERT, 0),
            h_ops.get(UPDATE, 0),
            h_ops.get(DELETE, 0),
            h_counts.quarantined,
            h_counts.superseded,
        )

        # --- checks after writing ---------------------------------------------------------
        filings = session.read.format("delta").load(filing_path)
        holdings = (
            session.read.format("delta")
            .load(holding_path)
            .where(F.col("period_of_report").isin(*periods))
        )  # prunes to P_X: every holding row this run may have touched is there
        my_holdings = holdings.where(F.col("_dataset") == key)
        quarantine = (
            session.read.format("delta").load(quarantine_path).where(F.col("_dataset") == key)
        )
        silver_filing_rows = filings.where(F.col("_dataset") == key).count()
        silver_holding_rows = my_holdings.count()
        q = quarantine.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(F.col("source_table") == "submission", 1).otherwise(0)).alias("filings"),
            F.sum(F.when(F.col("source_table") == "infotable", 1).otherwise(0)).alias("holdings"),
            F.sum(
                F.when(
                    F.col("reason_codes").isNull()
                    | (F.size("reason_codes") == 0)
                    | F.col("raw_row").isNull(),
                    1,
                ).otherwise(0)
            ).alias("incomplete"),
        ).first()
        quarantine_rows = int(q["rows"])
        q_filings, q_holdings = int(q["filings"] or 0), int(q["holdings"] or 0)
        incomplete = int(q["incomplete"] or 0)
        filing_keys = key_problems(filings, FILING.keys)
        holding_keys = key_problems(
            holdings.join(scope, "accession_number", "left_semi"), HOLDING.keys
        )
        quarantine_keys = key_problems(quarantine, QUARANTINE.keys)
        missing_in_bronze = (
            filings.where(F.col("_dataset") == key)
            .join(mine["submission"], "accession_number", "left_anti")
            .count()
        )
        joined_on = ["accession_number", "period_of_report", "_dataset"]
        holdings_without_filing = my_holdings.join(
            filings.select(*joined_on), joined_on, "left_anti"
        ).count()
        f_unbalanced = f_counts.bronze - (silver_filing_rows + q_filings + f_counts.superseded)
        h_unbalanced = h_counts.bronze - (silver_holding_rows + q_holdings + h_counts.superseded)
        warn_counts = filing_warn_counts(f_evaluated)
        copies = duplicate_copies(bronze, mine, key)
        no_shared = {"shared_keys": 0, "differing_keys": 0}
        f_dupes = (
            differing_duplicates(copies[FILING_TABLE], ("accession_number",), key)
            if copies
            else no_shared
        )
        h_dupes = (
            differing_duplicates(copies[HOLDING_TABLE], ("accession_number", "infotable_sk"), key)
            if copies
            else no_shared
        )
        bad_cusips = hsplit.good.where(~F.col("cusip_format_ok")).count()
        reasons = {
            f"{r['source_table']}:{r['code']}": int(r["count"])
            for r in quarantine.select("source_table", F.explode("reason_codes").alias("code"))
            .groupBy("source_table", "code")
            .count()
            .collect()
        }  # one row per table and reason code, a dozen at most

        post_filing = [
            silver_check_result(
                "SDQ1",
                filing_key,
                key,
                int(silver_filing_rows == 0),
                table=FILING_TABLE,
                metrics={"rows": silver_filing_rows},
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
                abs(f_unbalanced),
                table=FILING_TABLE,
                metrics={
                    "bronze_rows": f_counts.bronze,
                    "silver_rows": silver_filing_rows,
                    "quarantined": q_filings,
                    "superseded": f_counts.superseded,
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
            silver_check_result(
                "SDQ11",
                filing_key,
                key,
                f_dupes["differing_keys"],
                table=FILING_TABLE,
                metrics=f_dupes,
            ),
        ]
        post_holding = [
            silver_check_result(
                "SDQ1",
                holding_key,
                key,
                int(silver_holding_rows == 0),
                table=HOLDING_TABLE,
                metrics={"rows": silver_holding_rows},
            ),
            silver_check_result(
                "SDQ2",
                holding_key,
                key,
                sum(holding_keys.values()),
                table=HOLDING_TABLE,
                metrics=holding_keys,
            ),
            silver_check_result(
                "SDQ3",
                holding_key,
                key,
                abs(h_unbalanced),
                table=HOLDING_TABLE,
                metrics={
                    "bronze_rows": h_counts.bronze,
                    "silver_rows": silver_holding_rows,
                    "quarantined": q_holdings,
                    "superseded": h_counts.superseded,
                },
            ),
            silver_check_result(
                "SDQ7",
                holding_key,
                key,
                holdings_without_filing,
                table=HOLDING_TABLE,
                metrics={"without_filing": holdings_without_filing},
            ),
            silver_check_result(
                "SDQ11",
                holding_key,
                key,
                h_dupes["differing_keys"],
                table=HOLDING_TABLE,
                metrics=h_dupes,
            ),
            silver_check_result(
                "SDQ13",
                holding_key,
                key,
                bad_cusips,
                table=HOLDING_TABLE,
                metrics={"cusip_format_not_ok": bad_cusips},
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
                "rows_for_dataset": MetadataValue.int(silver_filing_rows),
                "inserted": MetadataValue.int(f_ops.get(INSERT, 0)),
                "updated": MetadataValue.int(f_ops.get(UPDATE, 0)),
                "deleted": MetadataValue.int(f_ops.get(DELETE, 0)),
                "quarantined": MetadataValue.int(f_counts.quarantined),
                "superseded": MetadataValue.int(f_counts.superseded),
                "periods_touched": MetadataValue.int(len(periods)),
                "periods": MetadataValue.text(", ".join(p.isoformat() for p in periods) or "none"),
                "taken_over_from": MetadataValue.text(", ".join(taken_over_from) or "none"),
                "handed_back_to": MetadataValue.text(", ".join(handed_back_to) or "none"),
            },
        )
        yield from pre_filing
        yield from post_filing
        yield MaterializeResult(
            asset_key=holding_key,
            metadata={
                "path": MetadataValue.path(holding_path),
                "rows_for_dataset": MetadataValue.int(silver_holding_rows),
                "inserted": MetadataValue.int(h_ops.get(INSERT, 0)),
                "updated": MetadataValue.int(h_ops.get(UPDATE, 0)),
                "deleted": MetadataValue.int(h_ops.get(DELETE, 0)),
                "quarantined": MetadataValue.int(h_counts.quarantined),
                "superseded": MetadataValue.int(h_counts.superseded),
                "datasets_read": MetadataValue.text(", ".join(datasets_in_scope)),
            },
        )
        yield from pre_holding
        yield from post_holding
        yield MaterializeResult(
            asset_key=quarantine_key,
            metadata={
                "path": MetadataValue.path(quarantine_path),
                "row_count": MetadataValue.int(quarantine_rows),
                "reasons": MetadataValue.json(reasons),
            },
        )
        yield from post_quarantine

        failed = sorted(blocking_failures(post_filing + post_holding + post_quarantine))
        if failed:
            raise SilverQualityError(
                f"{key}: blocking checks failed after writing: {', '.join(failed)}. "
                "Fix the cause and rerun this data set."
            )
    finally:
        for df in cached:
            df.unpersist()
