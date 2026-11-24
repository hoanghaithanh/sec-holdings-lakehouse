"""Bronze INFOTABLE of one data set to typed Silver holding rows (M2 slice S2).

Pure DataFrame functions, no I/O, like `silver_filing.py`. Rules come from the M2 spec
(`silver.form13f_holding`, "Value-unit normalization", the reason codes) and ADR-005 ("One run
for data set X", step 2: holdings join the data set's good filings for their filing fields).

Every Bronze column is a string and an empty field is `""` (ADR-002). Built-in functions only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sec_lakehouse.sources.form13f.schemas import TABLES
from sec_lakehouse.sources.form13f.silver_filing import (
    AMOUNT_DIGITS,
    QUARANTINE_OUTPUT,
    bad_unsigned,
    blank_to_null,
)
from sec_lakehouse.sources.form13f.silver_schemas import (
    BAD_INTEGER,
    CUSIP_SHAPE,
    HOLDING_COLUMNS,
    NO_FILING,
    RUN_COLUMNS,
    column_names,
)

HOLDING_SOURCES = ("infotable",)
INFOTABLE_COLUMNS = next(spec for spec in TABLES if spec.name == "infotable").columns
SK_DIGITS = 18  # fits Spark `bigint` (max 9,223,372,036,854,775,807)

# Silver columns written by this module, without the run columns the writer adds.
HOLDING_OUTPUT = [c for c in column_names(HOLDING_COLUMNS) if c not in RUN_COLUMNS]
# Copied from the Silver filing row of the same accession.
FILING_FIELDS = (
    "cik",
    "filing_date",
    "period_of_report",
    "submission_type",
    "amendment_type",
    "value_unit",
)
# (Silver column, Bronze column, required). A required amount that is empty is BAD_INTEGER; an
# optional one becomes null.
AMOUNTS = (
    ("value_reported", "value", True),
    ("shares_or_principal", "sshprnamt", True),
    ("voting_auth_sole", "voting_auth_sole", False),
    ("voting_auth_shared", "voting_auth_shared", False),
    ("voting_auth_none", "voting_auth_none", False),
)


@dataclass(frozen=True)
class HoldingSplit:
    """The INFOTABLE rows of one data set, evaluated once and split three ways.

    `evaluated` has one row per Bronze INFOTABLE row of the data set, with the raw strings
    (`it` struct), the typed columns, `reason_codes`, `owner` and `superseded`. Callers persist
    it: `good`, `quarantine` and the counts all read it.
    """

    evaluated: Any
    good: Any  # Silver holding rows (HOLDING_OUTPUT), owned by this data set and clean
    quarantine: Any  # quarantine rows (QUARANTINE_OUTPUT), source_table 'infotable'


def evaluate_holdings(infotable: Any, good_filings: Any, owners: Any, key: str) -> Any:
    """One row per INFOTABLE row of data set `key`, typed, with its reason codes.

    `infotable` holds the Bronze rows of `key` only. `good_filings` are the data set's clean,
    owned Silver filing rows: a holding takes `cik`, the dates, the types and `value_unit` from
    its filing, so the unit is decided once, by filing date, on the filing. A line whose filing
    is not among them (no SUBMISSION row, or the filing was quarantined) gets `NO_FILING`.
    """
    from pyspark.sql import functions as F

    filing_fields = good_filings.select(
        "accession_number",
        F.lit(True).alias("_has_filing"),
        *FILING_FIELDS,
    )
    frame = (
        infotable.select(
            "accession_number",
            F.col("_source_line"),
            F.struct(*INFOTABLE_COLUMNS).alias("it"),
        )
        # One data set has tens of thousands of filings at most: a broadcast join (ADR-005).
        .join(F.broadcast(filing_fields), "accession_number", "left")
        .join(owners, "accession_number", "left")
    )

    def it(c: str) -> Any:
        return F.col(f"it.{c}")

    def amount(column: str) -> Any:
        return F.when(~bad_unsigned(it(column), AMOUNT_DIGITS), blank_to_null(it(column))).cast(
            "decimal(20,0)"
        )

    cusip = F.upper(blank_to_null(it("cusip")))
    value_reported = amount("value")
    typed = {
        "infotable_sk": F.when(
            ~bad_unsigned(it("infotable_sk"), SK_DIGITS), blank_to_null(it("infotable_sk"))
        ).cast("bigint"),
        "issuer_name": blank_to_null(it("nameofissuer")),
        "title_of_class": blank_to_null(it("titleofclass")),
        "cusip": cusip,
        "cusip_format_ok": F.coalesce(cusip.rlike(CUSIP_SHAPE), F.lit(False)),
        "figi": blank_to_null(it("figi")),
        "value_reported": value_reported,
        # 16 digits x 1000 fits decimal(20,0); Spark types the product decimal(25,0), so cast.
        "value_usd": F.when(
            F.col("value_unit") == "THOUSANDS", (value_reported * 1000).cast("decimal(20,0)")
        ).otherwise(value_reported),
        **{silver: amount(bronze) for silver, bronze, _ in AMOUNTS if silver != "value_reported"},
        "shares_or_principal_type": F.upper(blank_to_null(it("sshprnamttype"))),
        "put_call": F.upper(blank_to_null(it("putcall"))),
        "investment_discretion": F.upper(blank_to_null(it("investmentdiscretion"))),
        "other_manager_refs_raw": blank_to_null(it("othermanager")),
        "_dataset": F.lit(key),
    }
    frame = frame.select("*", *(expr.alias(name) for name, expr in typed.items()))

    def raw(column: str) -> Any:
        return F.concat(F.lit(f"{column}="), F.coalesce(it(column), F.lit("<missing>")))

    def bad_amount(column: str, digits: int, required: bool) -> Any:
        missing = blank_to_null(it(column)).isNull() if required else F.lit(False)
        return missing | bad_unsigned(it(column), digits)

    # (code, condition, detail), in the fixed order of REASON_CODES.
    rules = [
        *(
            (BAD_INTEGER, bad_amount(column, digits, required), raw(column))
            for column, digits, required in (
                ("infotable_sk", SK_DIGITS, True),
                *((bronze, AMOUNT_DIGITS, required) for _, bronze, required in AMOUNTS),
            )
        ),
        (
            NO_FILING,
            F.col("_has_filing").isNull(),
            F.lit("no clean Silver filing for this accession_number in this data set"),
        ),
    ]
    failed = [(code, F.coalesce(cond, F.lit(False)), detail) for code, cond, detail in rules]
    return frame.select(
        "*",
        F.array_distinct(
            F.array_compact(F.array(*(F.when(cond, F.lit(code)) for code, cond, _ in failed)))
        ).alias("reason_codes"),
        F.concat_ws("; ", *(F.when(cond, detail) for _, cond, detail in failed)).alias(
            "reason_detail"
        ),
        # No SUBMISSION row anywhere: `owner` is null, so the line is not superseded and
        # lands in quarantine as NO_FILING.
        F.coalesce(F.col("owner") > F.lit(key), F.lit(False)).alias("superseded"),
    )


def split_holdings(infotable: Any, good_filings: Any, owners: Any, key: str) -> HoldingSplit:
    """Evaluate the data set's holding lines and split them (ADR-005, step 2).

    Lines whose accession a later data set owns are superseded: neither written nor
    quarantined, whatever their content. Each remaining line lands in exactly one of `good`
    and `quarantine`; a quarantined filing sends all its lines to quarantine as `NO_FILING`.
    """
    from pyspark.sql import functions as F

    evaluated = evaluate_holdings(infotable, good_filings, owners, key)
    kept = evaluated.where(~F.col("superseded"))
    good = kept.where(F.size("reason_codes") == 0).select(*HOLDING_OUTPUT)
    quarantine = kept.where(F.size("reason_codes") > 0).select(
        F.lit("infotable").alias("source_table"),
        "_dataset",
        "_source_line",
        "accession_number",
        "reason_codes",
        "reason_detail",
        F.to_json(F.struct(F.col("it").alias("infotable"))).alias("raw_row"),
    )
    return HoldingSplit(
        evaluated=evaluated, good=good, quarantine=quarantine.select(*QUARANTINE_OUTPUT)
    )
