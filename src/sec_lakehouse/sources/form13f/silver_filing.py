"""Bronze SUBMISSION + COVERPAGE + SUMMARYPAGE of one data set to typed Silver filing rows.

Pure DataFrame functions, no I/O: the asset reads Bronze and writes Silver, and the tests call
these directly on small frames. Rules come from the M2 spec ("Tables and assets", "Value-unit
normalization", the reason codes) and ADR-005 ("One run for data set X", steps 1 and 2).

Every Bronze column is a string and an empty field is `""` (ADR-002). Everything below uses
Spark built-in functions only: no Python UDF, nothing collected.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sec_lakehouse.sources.form13f.schemas import TABLES
from sec_lakehouse.sources.form13f.silver_schemas import (
    AMENDMENT_TYPES,
    BAD_DATE,
    BAD_INTEGER,
    BAD_PERIOD,
    BAD_SUBMISSION_TYPE,
    DOLLARS_FROM,
    FILING_COLUMNS,
    NO_FILING,
    QUARANTINE_COLUMNS,
    RUN_COLUMNS,
    SUBMISSION_TYPES,
    column_names,
)

_SPECS = {spec.name: spec for spec in TABLES}
FILING_SOURCES = ("submission", "coverpage", "summarypage")

DATE_SHAPE = r"^[0-9]{2}-[A-Z]{3}-[0-9]{4}$"  # 31-MAY-2013
DATE_FORMAT = "dd-MMM-yyyy"
INT_DIGITS = 9  # fits Spark `int` (max 2,147,483,647)
AMOUNT_DIGITS = 16  # NUMBER(16) in the SEC metadata; fits decimal(20,0) even x 1000
FLAG_VALUES = ("Y", "N")

# Silver columns written by this module, without the run columns the writer adds.
FILING_OUTPUT = [c for c in column_names(FILING_COLUMNS) if c not in RUN_COLUMNS]
QUARANTINE_OUTPUT = [c for c in column_names(QUARANTINE_COLUMNS) if c not in RUN_COLUMNS]


@dataclass(frozen=True)
class FilingSplit:
    """The rows of one data set, evaluated once and split three ways.

    `evaluated` has one row per Bronze SUBMISSION row of the data set, with the raw strings
    (`sub`, `cov`, `sum` structs), the typed columns, `reason_codes`, `owner` and `superseded`.
    Callers persist it: `good`, `quarantine` and the counts all read it.
    """

    evaluated: Any
    good: Any  # Silver filing rows (FILING_OUTPUT), owned by this data set and clean
    quarantine: Any  # quarantine rows (QUARANTINE_OUTPUT), filings and orphan child rows


def blank_to_null(column: Any) -> Any:
    """Trim; an empty result becomes null. Null stays null."""
    from pyspark.sql import functions as F

    trimmed = F.trim(column)
    return F.when(trimmed != "", trimmed)


def parse_date(column: Any) -> Any:
    """`DD-MON-YYYY` to a date; null when empty, another shape, or not a real day (31-FEB).

    The shape is checked first: with Spark's default parser policy an unusual string could
    raise instead of returning null.
    """
    from pyspark.sql import functions as F

    text = F.upper(blank_to_null(column))
    return F.when(text.rlike(DATE_SHAPE), F.to_date(text, DATE_FORMAT))


def bad_unsigned(column: Any, digits: int) -> Any:
    """True when the value is present but is not 1 to `digits` decimal digits.

    Checked before casting: with ANSI mode off, an overflowing cast silently returns null.
    """
    text = blank_to_null(column)
    return text.isNotNull() & ~text.rlike(f"^[0-9]{{1,{digits}}}$")


def to_flag(column: Any) -> Any:
    """`Y` to true, `N` to false, anything else to null (SDQ10 counts unrecognised values)."""
    from pyspark.sql import functions as F

    text = F.upper(blank_to_null(column))
    return F.when(text == "Y", True).when(text == "N", False)


def unrecognised_flag(column: Any) -> Any:
    from pyspark.sql import functions as F

    text = F.upper(blank_to_null(column))
    return text.isNotNull() & ~text.isin(*FLAG_VALUES)


def owner_map(bronze_submission: Any) -> Any:
    """`(accession_number, owner)`: the latest data set whose SUBMISSION holds the accession.

    Data set keys sort in time order as strings (ADR-002), so `max` is the latest (Q6 A). Reads
    two columns of the whole Bronze table, the only unpruned Bronze read (ADR-005, step 1).
    """
    from pyspark.sql import functions as F

    return bronze_submission.groupBy("accession_number").agg(F.max("_dataset").alias("owner"))


def _as_struct(df: Any, table: str, alias: str) -> Any:
    """`accession_number`, `_source_line` and one struct `alias` of the table's raw columns."""
    from pyspark.sql import functions as F

    columns = _SPECS[table].columns
    return df.select(
        "accession_number",
        F.col("_source_line").alias(f"_{alias}_line"),
        F.struct(*columns).alias(alias),
    )


def evaluate_filings(
    submission: Any, coverpage: Any, summarypage: Any, owners: Any, key: str
) -> Any:
    """One row per SUBMISSION row of data set `key`, typed, with its reason codes.

    The three inputs are the Bronze rows of `key` only. Notices have no SUMMARYPAGE row, so
    those fields are null for them.
    """
    from pyspark.sql import functions as F

    frame = (
        _as_struct(submission, "submission", "sub")
        .join(
            _as_struct(coverpage, "coverpage", "cov").drop("_cov_line"), "accession_number", "left"
        )
        .join(
            _as_struct(summarypage, "summarypage", "sum").drop("_sum_line"),
            "accession_number",
            "left",
        )
        .join(owners, "accession_number", "left")
    )

    def sub(c: str) -> Any:
        return F.col(f"sub.{c}")

    def cov(c: str) -> Any:
        return F.col(f"cov.{c}")

    def summ(c: str) -> Any:
        return F.col(f"sum.{c}")

    submission_type = F.upper(blank_to_null(sub("submissiontype")))
    is_amendment = submission_type.endswith("/A")
    filing_date = parse_date(sub("filing_date"))
    period = parse_date(sub("periodofreport"))
    amendment_type = F.upper(blank_to_null(cov("amendmenttype")))
    table_value = blank_to_null(summ("tablevaluetotal"))
    value_unit = F.when(filing_date < F.lit(DOLLARS_FROM), "THOUSANDS").when(
        filing_date.isNotNull(), "DOLLARS"
    )
    table_value_reported = F.when(
        ~bad_unsigned(summ("tablevaluetotal"), AMOUNT_DIGITS), table_value.cast("decimal(20,0)")
    )
    table_value_usd = F.when(
        value_unit == "THOUSANDS", (table_value_reported * 1000).cast("decimal(20,0)")
    ).otherwise(table_value_reported)

    # `accession_number` is already a column of `frame` (the join key).
    typed = {
        "cik": blank_to_null(sub("cik")),
        "submission_type": submission_type,
        "filing_date": filing_date,
        "period_of_report": period,
        "report_calendar_or_quarter": parse_date(cov("reportcalendarorquarter")),
        "report_type": F.upper(blank_to_null(cov("reporttype"))),
        "is_amendment": is_amendment,
        "amendment_no": F.when(
            is_amendment & ~bad_unsigned(cov("amendmentno"), INT_DIGITS),
            blank_to_null(cov("amendmentno")).cast("int"),
        ),
        "amendment_type": F.when(amendment_type.isin(*AMENDMENT_TYPES), amendment_type),
        "conf_denied_expired": to_flag(cov("confdeniedexpired")),
        "date_denied_expired": parse_date(cov("datedeniedexpired")),
        "date_reported": parse_date(cov("datereported")),
        "reason_for_non_confidentiality": blank_to_null(cov("reasonfornonconfidentiality")),
        "is_confidential_omitted": to_flag(summ("isconfidentialomitted")),
        "filing_manager_name": blank_to_null(cov("filingmanager_name")),
        "filing_manager_street1": blank_to_null(cov("filingmanager_street1")),
        "filing_manager_street2": blank_to_null(cov("filingmanager_street2")),
        "filing_manager_city": blank_to_null(cov("filingmanager_city")),
        "filing_manager_state_or_country": blank_to_null(cov("filingmanager_stateorcountry")),
        "filing_manager_zip_code": blank_to_null(cov("filingmanager_zipcode")),
        "form13f_file_number": blank_to_null(cov("form13ffilenumber")),
        "crd_number": blank_to_null(cov("crdnumber")),
        "sec_file_number": blank_to_null(cov("secfilenumber")),
        "provide_info_for_instruction5": to_flag(cov("provideinfoforinstruction5")),
        "additional_information": blank_to_null(cov("additionalinformation")),
        "other_included_managers_count": F.when(
            ~bad_unsigned(summ("otherincludedmanagerscount"), INT_DIGITS),
            blank_to_null(summ("otherincludedmanagerscount")).cast("int"),
        ),
        "table_entry_total": F.when(
            ~bad_unsigned(summ("tableentrytotal"), INT_DIGITS),
            blank_to_null(summ("tableentrytotal")).cast("int"),
        ),
        "table_value_total_reported": table_value_reported,
        "table_value_total_usd": table_value_usd,
        "value_unit": value_unit,
        "_dataset": F.lit(key),
        "_source_line": F.col("_sub_line"),
    }
    frame = frame.select("*", *(expr.alias(name) for name, expr in typed.items()))

    # (code, condition, detail). The detail names the failing column and its raw value.
    def raw(c: Any, name: str) -> Any:
        return F.concat(F.lit(f"{name}="), F.coalesce(c, F.lit("<missing>")))

    is_quarter_end = F.month("period_of_report").isin(3, 6, 9, 12) & (
        F.col("period_of_report") == F.last_day("period_of_report")
    )
    rules = [
        (BAD_DATE, F.col("filing_date").isNull(), raw(sub("filing_date"), "filing_date")),
        (
            BAD_DATE,
            F.col("period_of_report").isNull(),
            raw(sub("periodofreport"), "periodofreport"),
        ),
        (
            BAD_DATE,
            F.col("report_calendar_or_quarter").isNull(),
            F.when(F.col("cov").isNull(), F.lit("no COVERPAGE row for this filing")).otherwise(
                raw(cov("reportcalendarorquarter"), "reportcalendarorquarter")
            ),
        ),
        (
            BAD_PERIOD,
            F.col("period_of_report").isNotNull() & ~is_quarter_end,
            raw(sub("periodofreport"), "periodofreport (not a quarter end)"),
        ),
        (
            BAD_PERIOD,
            F.col("period_of_report") > F.col("filing_date"),
            raw(sub("periodofreport"), "periodofreport (after filing_date)"),
        ),
        (
            BAD_SUBMISSION_TYPE,
            F.col("submission_type").isNull() | ~F.col("submission_type").isin(*SUBMISSION_TYPES),
            raw(sub("submissiontype"), "submissiontype"),
        ),
        *(
            (BAD_INTEGER, bad_unsigned(column, digits), raw(column, name))
            for column, name, digits in (
                (cov("amendmentno"), "amendmentno", INT_DIGITS),
                (summ("otherincludedmanagerscount"), "otherincludedmanagerscount", INT_DIGITS),
                (summ("tableentrytotal"), "tableentrytotal", INT_DIGITS),
                (summ("tablevaluetotal"), "tablevaluetotal", AMOUNT_DIGITS),
            )
        ),
    ]
    # A null comparison (missing date) counts as "rule not failed": coalesce to false.
    failed = [(code, F.coalesce(cond, F.lit(False)), detail) for code, cond, detail in rules]
    return frame.select(
        "*",
        F.array_distinct(
            F.array_compact(F.array(*(F.when(cond, F.lit(code)) for code, cond, _ in failed)))
        ).alias("reason_codes"),
        F.concat_ws("; ", *(F.when(cond, detail) for _, cond, detail in failed)).alias(
            "reason_detail"
        ),
        (F.col("owner") > F.lit(key)).alias("superseded"),
    )


def orphan_quarantine(child: Any, table: str, submission: Any) -> Any:
    """COVERPAGE or SUMMARYPAGE rows of the data set with no SUBMISSION row (Q12)."""
    from pyspark.sql import functions as F

    columns = _SPECS[table].columns
    return child.join(
        submission.select("accession_number"), "accession_number", "left_anti"
    ).select(
        F.lit(table).alias("source_table"),
        "_dataset",
        "_source_line",
        "accession_number",
        F.array(F.lit(NO_FILING)).alias("reason_codes"),
        F.lit(f"no SUBMISSION row for this accession_number in {table}'s data set").alias(
            "reason_detail"
        ),
        F.to_json(F.struct(F.struct(*columns).alias(table))).alias("raw_row"),
    )


def split_filings(
    submission: Any, coverpage: Any, summarypage: Any, owners: Any, key: str
) -> FilingSplit:
    """Evaluate the data set and split it into Silver rows and quarantine rows (ADR-005, step 2).

    Rows whose accession a later data set owns are superseded: neither written nor quarantined.
    Each remaining row lands in exactly one of `good` and `quarantine`.
    """
    from pyspark.sql import functions as F

    evaluated = evaluate_filings(submission, coverpage, summarypage, owners, key)
    kept = evaluated.where(~F.col("superseded"))
    good = kept.where(F.size("reason_codes") == 0).select(*FILING_OUTPUT)
    bad_filings = kept.where(F.size("reason_codes") > 0).select(
        F.lit("submission").alias("source_table"),
        "_dataset",
        "_source_line",
        "accession_number",
        "reason_codes",
        "reason_detail",
        F.to_json(
            F.struct(
                F.col("sub").alias("submission"),
                F.col("cov").alias("coverpage"),
                F.col("sum").alias("summarypage"),
            )
        ).alias("raw_row"),
    )
    quarantine = bad_filings.unionByName(
        orphan_quarantine(coverpage, "coverpage", submission)
    ).unionByName(orphan_quarantine(summarypage, "summarypage", submission))
    return FilingSplit(
        evaluated=evaluated, good=good, quarantine=quarantine.select(*QUARANTINE_OUTPUT)
    )


def filing_warn_counts(evaluated: Any) -> dict[str, int]:
    """SDQ10 counts over the rows this data set writes to Silver (owned and clean)."""
    from pyspark.sql import functions as F

    good = F.size("reason_codes") == 0
    raw_amendment_type = F.upper(blank_to_null(F.col("cov.amendmenttype")))
    is_amendment_flag = to_flag(F.col("cov.isamendment"))
    counts = {
        "unrecognised_flags": unrecognised_flag(F.col("cov.confdeniedexpired"))
        | unrecognised_flag(F.col("cov.provideinfoforinstruction5"))
        | unrecognised_flag(F.col("sum.isconfidentialomitted"))
        | unrecognised_flag(F.col("cov.isamendment")),
        "unknown_amendment_type": F.col("is_amendment")
        & raw_amendment_type.isNotNull()
        & ~raw_amendment_type.isin(*AMENDMENT_TYPES),
        "missing_amendment_type": F.col("is_amendment") & raw_amendment_type.isNull(),
        "amendment_type_on_original": ~F.col("is_amendment") & raw_amendment_type.isNotNull(),
        "isamendment_mismatch": is_amendment_flag.isNotNull()
        & (is_amendment_flag != F.col("is_amendment")),
        "bad_optional_dates": (
            blank_to_null(F.col("cov.datedeniedexpired")).isNotNull()
            & F.col("date_denied_expired").isNull()
        )
        | (blank_to_null(F.col("cov.datereported")).isNotNull() & F.col("date_reported").isNull()),
        "cik_not_10_digits": ~F.coalesce(F.col("cik"), F.lit("")).rlike(r"^[0-9]{10}$"),
    }
    row = (
        evaluated.where(~F.col("superseded") & good)
        .agg(
            *(
                F.sum(F.when(F.coalesce(cond, F.lit(False)), 1).otherwise(0)).alias(name)
                for name, cond in counts.items()
            )
        )
        .first()
    )
    return {name: int(row[name] or 0) for name in counts}
