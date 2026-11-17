"""Unit tests of the DQ1 to DQ9 declarations and metrics, on small in-memory DataFrames."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from sec_lakehouse.assets.form13f_bronze import BRONZE_KEYS
from sec_lakehouse.assets.form13f_checks import (
    CHECKS,
    check_specs,
    infotable_value_problems,
    table_metrics,
)
from sec_lakehouse.assets.form13f_raw import RAW_ZIP_ASSET
from sec_lakehouse.definitions import defs
from sec_lakehouse.resources import SparkResource
from sec_lakehouse.sources.form13f.datasets import FORM13F_PARTITIONS
from sec_lakehouse.sources.form13f.schemas import TABLES

SPECS = {spec.name: spec for spec in TABLES}
ALL = {spec.name for spec in TABLES}

# The spec's "Data quality rules" table: check -> (tables, blocks the run).
EXPECTED = {
    "dq1_has_rows": ({"submission", "coverpage", "summarypage", "signature", "infotable"}, True),
    "dq2_row_count": ({"othermanager", "othermanager2"}, False),
    "dq3_key_not_null": (ALL, True),
    "dq4_key_unique": (ALL, True),
    "dq5_rows_match_file": (ALL, True),
    "dq6_accession_in_submission": (ALL - {"submission"}, False),
    "dq7_accession_in_other_datasets": ({"submission"}, False),
    "dq8_infotable_values": ({"infotable"}, False),
    "dq9_metadata_valid": (ALL, True),
}


def test_check_specs_match_the_spec_table() -> None:
    specs = check_specs(BRONZE_KEYS)
    found: dict[str, set[str]] = {}
    for spec in specs:
        found.setdefault(spec.name, set()).add(spec.asset_key.path[-1].removeprefix("form13f_"))
        assert spec.blocking is EXPECTED[spec.name][1], spec.name
    assert found == {name: tables for name, (tables, _) in EXPECTED.items()}
    assert len(specs) == 43 and len(CHECKS) == 9


def test_backfill_job_covers_raw_and_all_bronze_tables() -> None:
    job = defs.resolve_job_def("form13f_bronze_job")
    assert job.partitions_def == FORM13F_PARTITIONS
    assert job.asset_layer.executable_asset_keys == {RAW_ZIP_ASSET, *BRONZE_KEYS.values()}


@pytest.fixture
def session(spark_resource: SparkResource) -> Any:
    return spark_resource.model_copy().get_session()


def _submission(session: Any, rows: list[tuple[Any, ...]]) -> Any:
    columns = ["accession_number", "_dataset", "_source_file", "_source_line"]
    columns += ["_ingested_at", "_run_id"]
    return session.createDataFrame(
        rows,
        "accession_number string, _dataset string, _source_file string, "
        "_source_line long, _ingested_at timestamp, _run_id string",
    ).select(*columns)


def test_table_metrics_counts_bad_metadata_and_keys(session: Any) -> None:
    now = datetime(2026, 11, 11, tzinfo=UTC)
    df = _submission(
        session,
        [
            ("A1", "2013q2", "f", 2, now, "run"),
            ("A2", "2013q3", "f", 3, now, "run"),  # wrong _dataset
            ("A3", "2013q2", "f", 4, now, None),  # null _run_id
            (" ", "2013q2", "f", 5, None, "run"),  # blank key, null _ingested_at
            ("A1", "2013q2", "f", 6, now, "run"),  # duplicate key
        ],
    )
    metrics = table_metrics(df, SPECS["submission"], "2013q2")
    assert metrics == {"rows": 5, "key_missing": 1, "meta_bad": 3, "duplicate_keys": 1}


def test_infotable_value_problems_counts_each_column(session: Any) -> None:
    df = session.createDataFrame(
        [
            ("037833100", "1000", "50"),  # good
            ("03783310", "1000", "50"),  # 8-character cusip
            ("037833100", "-5", "50"),  # negative value
            ("037833100", "12.5", ""),  # decimal value, empty shares
            (None, None, None),  # all missing
        ],
        "cusip string, value string, sshprnamt string",
    )
    assert infotable_value_problems(df) == {"cusip": 2, "value": 3, "sshprnamt": 2}
