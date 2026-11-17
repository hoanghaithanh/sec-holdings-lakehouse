"""The real SEC samples in tests/fixtures/form13f unpack and load with every block check passing."""

from __future__ import annotations

from pathlib import Path

import pytest
from dagster import materialize

from sec_lakehouse.assets.form13f_bronze import bronze_form13f
from sec_lakehouse.assets.form13f_raw import raw_zip_path
from sec_lakehouse.resources import LakehouseStorage, SparkResource
from sec_lakehouse.sources.form13f.datasets import DATASETS_BY_KEY
from sec_lakehouse.sources.form13f.unpack import unpack

FIXTURES = Path(__file__).parent / "fixtures" / "form13f"

# Data lines per table in each sample (see tests/fixtures/form13f/README.md).
EXPECTED = {
    "2013q2": {
        "submission": 10,
        "coverpage": 10,
        "othermanager": 8,
        "signature": 10,
        "summarypage": 8,
        "othermanager2": 3,
        "infotable": 160,
    },
    "2022q4": {
        "submission": 10,
        "coverpage": 10,
        "othermanager": 3,
        "signature": 10,
        "summarypage": 8,
        "othermanager2": 4,
        "infotable": 160,
    },
    "2026-06_2026-08": {
        "submission": 10,
        "coverpage": 10,
        "othermanager": 0,
        "signature": 10,
        "summarypage": 10,
        "othermanager2": 1,
        "infotable": 200,
    },
}


def sample(key: str) -> Path:
    return FIXTURES / f"{key}_form13f_sample.zip"


@pytest.mark.parametrize("key", sorted(EXPECTED))
def test_real_sample_matches_the_declared_columns(key: str, tmp_path: Path) -> None:
    tables = unpack(sample(key), tmp_path)
    assert {t.spec.name: t.rows for t in tables} == EXPECTED[key]
    # Every declared column is present, figi included, even in the first data set.
    assert all(set(t.spec.columns) == set(t.header) for t in tables)
    assert next(t for t in tables if t.spec.name == "signature").member == "SIGNATURE.tsv"


@pytest.mark.parametrize("key", sorted(EXPECTED))
def test_real_sample_loads_with_block_checks_passing(
    key: str, tmp_path: Path, spark_resource: SparkResource
) -> None:
    storage = LakehouseStorage(root_uri=str(tmp_path / "lake"))
    storage.put_new(raw_zip_path(DATASETS_BY_KEY[key]), sample(key))
    result = materialize(
        [bronze_form13f],
        partition_key=key,
        resources={"spark": spark_resource, "storage": storage},
        raise_on_error=False,
    )
    assert result.success

    checks = {
        (e.asset_key.path[-1].removeprefix("form13f_"), e.check_name): e
        for e in result.get_asset_check_evaluations()
    }
    assert len(checks) == 43
    for (table, name), evaluation in checks.items():
        if name in {"dq1_has_rows", "dq3_key_not_null", "dq4_key_unique", "dq9_metadata_valid"}:
            assert evaluation.passed, (table, name)
        if name == "dq5_rows_match_file":
            assert evaluation.passed, table
            assert evaluation.metadata["written"].value == EXPECTED[key][table]
        if name == "dq6_accession_in_submission":
            assert evaluation.passed, table  # the sample keeps whole filings
    othermanager = checks[("othermanager", "dq2_row_count")].metadata["rows"].value
    assert othermanager == EXPECTED[key]["othermanager"]
