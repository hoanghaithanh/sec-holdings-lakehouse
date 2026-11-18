"""Declared schemas and rule constants of the Silver 13F tables (M2 spec, ADR-005).

Silver tables are created from these schemas before their first write, and Silver never turns on
`mergeSchema` or `overwriteSchema`, so a column change is always a reviewed PR. Delta refuses any
write whose columns or types differ from the table.

The schemas are built lazily (`filing_schema()`, ...) so importing this module does not import
PySpark; the Dagster code location loads without starting Spark.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

# --- rules -----------------------------------------------------------------------------------

SUBMISSION_TYPES = ("13F-HR", "13F-HR/A", "13F-NT", "13F-NT/A")
AMENDMENT_TYPES = ("RESTATEMENT", "NEW HOLDINGS")

# "Starting on January 3, 2023, market value is reported rounded to the nearest dollar.
# Previously, market value was reported in thousands." (SEC readme). Applied by filing date.
DOLLARS_FROM = date(2023, 1, 3)
# The first data set (2013q2) starts in May 2013; an earlier filing date fails SDQ4.
EARLIEST_FILING_DATE = date(2013, 5, 1)

# Quarantine reason codes, in the fixed order they appear in `reason_codes`.
BAD_DATE = "BAD_DATE"
BAD_PERIOD = "BAD_PERIOD"
BAD_SUBMISSION_TYPE = "BAD_SUBMISSION_TYPE"
BAD_INTEGER = "BAD_INTEGER"
NO_FILING = "NO_FILING"
REASON_CODES = (BAD_DATE, BAD_PERIOD, BAD_SUBMISSION_TYPE, BAD_INTEGER, NO_FILING)

# SDQ6 (Q4 B): block a data set when more than this share of its rows is quarantined.
QUARANTINE_BLOCK_RATIO = 0.01
# Reviewed per-data-set exceptions (Q11): {"<data set key>": 0.03}. Change only in a PR that
# explains the quarantined rows, so the one-command backfill never needs a manual step.
QUARANTINE_BLOCK_RATIO_OVERRIDES: dict[str, float] = {}


# --- tables ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class SilverTable:
    name: str  # silver/<name>
    keys: tuple[str, ...]
    partition_by: str


FILING = SilverTable("form13f_filing", ("accession_number",), "period_of_report")
QUARANTINE = SilverTable(
    "form13f_quarantine", ("source_table", "_dataset", "_source_line"), "_dataset"
)

# Written on every insert or update, and ignored when comparing old and new rows (ADR-005).
RUN_COLUMNS = ("_silver_processed_at", "_silver_run_id")

# (column, Spark SQL type, nullable). Order is the table's column order.
FILING_COLUMNS: tuple[tuple[str, str, bool], ...] = (
    ("accession_number", "string", False),
    ("cik", "string", True),
    ("submission_type", "string", True),
    ("filing_date", "date", False),
    ("period_of_report", "date", False),
    ("report_calendar_or_quarter", "date", True),
    ("report_type", "string", True),
    ("is_amendment", "boolean", True),
    ("amendment_no", "int", True),
    ("amendment_type", "string", True),
    ("conf_denied_expired", "boolean", True),
    ("date_denied_expired", "date", True),
    ("date_reported", "date", True),
    ("reason_for_non_confidentiality", "string", True),
    ("is_confidential_omitted", "boolean", True),
    ("filing_manager_name", "string", True),
    ("filing_manager_street1", "string", True),
    ("filing_manager_street2", "string", True),
    ("filing_manager_city", "string", True),
    ("filing_manager_state_or_country", "string", True),
    ("filing_manager_zip_code", "string", True),
    ("form13f_file_number", "string", True),
    ("crd_number", "string", True),
    ("sec_file_number", "string", True),
    ("provide_info_for_instruction5", "boolean", True),
    ("additional_information", "string", True),
    ("other_included_managers_count", "int", True),
    ("table_entry_total", "int", True),
    ("table_value_total_reported", "decimal(20,0)", True),
    ("table_value_total_usd", "decimal(20,0)", True),
    ("value_unit", "string", True),
    ("_dataset", "string", False),
    ("_source_line", "bigint", False),
    ("_silver_processed_at", "timestamp", False),
    ("_silver_run_id", "string", False),
)

QUARANTINE_COLUMNS: tuple[tuple[str, str, bool], ...] = (
    ("source_table", "string", False),
    ("_dataset", "string", False),
    ("_source_line", "bigint", False),
    ("accession_number", "string", True),
    ("reason_codes", "array<string>", False),
    ("reason_detail", "string", True),
    ("raw_row", "string", False),
    ("_silver_processed_at", "timestamp", False),
    ("_silver_run_id", "string", False),
)


def column_names(columns: tuple[tuple[str, str, bool], ...]) -> list[str]:
    return [name for name, _, _ in columns]


def _struct(columns: tuple[tuple[str, str, bool], ...]) -> StructType:
    from pyspark.sql import types as T

    spark_types = {
        "string": T.StringType(),
        "date": T.DateType(),
        "boolean": T.BooleanType(),
        "int": T.IntegerType(),
        "bigint": T.LongType(),
        "timestamp": T.TimestampType(),
        "decimal(20,0)": T.DecimalType(20, 0),
        "array<string>": T.ArrayType(T.StringType(), containsNull=False),
    }
    return T.StructType(
        [T.StructField(name, spark_types[kind], nullable) for name, kind, nullable in columns]
    )


def filing_schema() -> StructType:
    return _struct(FILING_COLUMNS)


def quarantine_schema() -> StructType:
    return _struct(QUARANTINE_COLUMNS)
