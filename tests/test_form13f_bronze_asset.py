from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from dagster import materialize
from form13f_zips import build_zip, table_lines

from sec_lakehouse.assets.form13f_bronze import BRONZE_KEYS, bronze_form13f
from sec_lakehouse.assets.form13f_raw import raw_zip_path
from sec_lakehouse.resources import LakehouseStorage, SparkResource
from sec_lakehouse.sources.form13f.datasets import DATASETS_BY_KEY
from sec_lakehouse.sources.form13f.schemas import TABLES

SPECS = {spec.name: spec for spec in TABLES}


def land(storage: LakehouseStorage, tmp_path: Path, key: str, data: bytes) -> None:
    """Put a zip in raw, as the raw asset would."""
    source = tmp_path / f"{key}.zip"
    source.write_bytes(data)
    storage.put_new(raw_zip_path(DATASETS_BY_KEY[key]), source)


def load(storage: LakehouseStorage, spark: SparkResource, key: str, ok: bool = True) -> Any:
    result = materialize(
        [bronze_form13f],
        partition_key=key,
        resources={"spark": spark, "storage": storage},
        raise_on_error=False,
    )
    assert result.success is ok
    return result


def read(spark: SparkResource, storage: LakehouseStorage, table: str) -> Any:
    # A fresh resource: each run's teardown stops the session the run used.
    fresh = spark.model_copy()
    return fresh.get_session().read.format("delta").load(storage.table_path("bronze", table))


@pytest.fixture
def storage(tmp_path: Path) -> LakehouseStorage:
    return LakehouseStorage(root_uri=str(tmp_path / "lake"))


def test_loads_all_seven_tables_as_strings(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    land(storage, tmp_path, "2013q2", build_zip(rows=3))
    result = load(storage, spark_resource, "2013q2")

    events = {e.asset_key: e for e in result.get_asset_materialization_events()}
    assert set(events) == set(BRONZE_KEYS.values())
    for spec in TABLES:
        meta = events[BRONZE_KEYS[spec.name]].materialization.metadata
        assert meta["row_count"].value == 3

        df = read(spark_resource, storage, f"form13f_{spec.name}")
        assert df.columns == [
            *spec.columns,
            "_dataset",
            "_source_file",
            "_source_line",
            "_ingested_at",
            "_run_id",
        ]
        types = dict(df.dtypes)
        assert all(types[c] == "string" for c in spec.columns)
        assert types["_source_line"] == "bigint" and types["_ingested_at"] == "timestamp"

    row = read(spark_resource, storage, "form13f_infotable").orderBy("_source_line").first()
    assert row["nameofissuer"] == "nameofissuer 0"
    assert row["_dataset"] == "2013q2"
    assert row["_source_line"] == 2
    assert row["_source_file"].endswith(
        "/raw/form13f_datasets/2013q2/2013q2_form13f.zip/INFOTABLE.tsv"
    )
    assert row["_run_id"] == result.run_id
    # Staging is scratch space and is cleaned up after a successful load.
    assert not (tmp_path / "lake/staging/form13f/2013q2").exists()


def test_rerun_replaces_only_its_partition(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    land(storage, tmp_path, "2013q2", build_zip(rows=2))
    land(storage, tmp_path, "2013q3", build_zip(rows=4))
    first_q2 = load(storage, spark_resource, "2013q2").run_id
    load(storage, spark_resource, "2013q3")
    rerun_q3 = load(storage, spark_resource, "2013q3").run_id

    df = read(spark_resource, storage, "form13f_submission")
    counts = {r["_dataset"]: r["count"] for r in df.groupBy("_dataset").count().collect()}
    assert counts == {"2013q2": 2, "2013q3": 4}  # AC: rerun leaves row counts unchanged
    runs = {
        r["_dataset"]: r["_run_id"] for r in df.select("_dataset", "_run_id").distinct().collect()
    }
    assert runs == {"2013q2": first_q2, "2013q3": rerun_q3}  # the other partition is untouched


def test_missing_optional_figi_loads_as_null(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    columns = [c for c in SPECS["infotable"].columns if c != "figi"]
    lines = ["\t".join(c.upper() for c in columns), "\t".join(f"{c} 0" for c in columns)]
    land(storage, tmp_path, "2013q2", build_zip(rows=1, overrides={"infotable": lines}))
    load(storage, spark_resource, "2013q2")

    row = read(spark_resource, storage, "form13f_infotable").first()
    assert row["figi"] is None
    assert row["cusip"] == "cusip 0"


def test_empty_values_stay_empty_strings(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    lines = table_lines(SPECS["submission"], 1)
    lines[1] = "0000950123-13-000001\t15-MAY-2013\t13F-HR\t\t"
    land(storage, tmp_path, "2013q2", build_zip(rows=1, overrides={"submission": lines}))
    load(storage, spark_resource, "2013q2")

    row = read(spark_resource, storage, "form13f_submission").first()
    assert row["cik"] == "" and row["periodofreport"] == ""


def test_wrong_field_count_fails_and_writes_nothing(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    lines = table_lines(SPECS["submission"], 2)
    lines[2] += "\textra"  # line 3 has one field too many
    land(storage, tmp_path, "2013q2", build_zip(rows=2, overrides={"submission": lines}))
    result = load(storage, spark_resource, "2013q2", ok=False)

    failure = next(e for e in result.all_events if e.is_step_failure)
    assert "lines ['3']" in str(failure.event_specific_data.error)
    assert not (tmp_path / "lake/bronze/form13f_submission").exists()


def test_header_mismatch_fails_before_any_table_is_written(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    lines = table_lines(SPECS["infotable"], 1)
    lines[0] += "\tNEWCOLUMN"
    lines[1] += "\tx"
    land(storage, tmp_path, "2013q2", build_zip(rows=1, overrides={"infotable": lines}))
    load(storage, spark_resource, "2013q2", ok=False)
    assert not (tmp_path / "lake/bronze").exists()
