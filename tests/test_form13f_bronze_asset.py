from __future__ import annotations

import time
from collections.abc import Callable
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


@pytest.mark.parametrize(
    "bad_line",
    [
        lambda line: line + "\textra",  # one field too many (a tab inside a value)
        lambda line: line.rsplit("\t", 1)[0],  # one field short (a line break inside a value)
        lambda line: "",  # a blank line
    ],
    ids=["too-many", "too-few", "blank"],
)
def test_wrong_field_count_fails_and_writes_nothing(
    tmp_path: Path,
    storage: LakehouseStorage,
    spark_resource: SparkResource,
    bad_line: Callable[[str], str],
) -> None:
    lines = table_lines(SPECS["submission"], 2)
    lines[2] = bad_line(lines[2])  # file line 3
    land(storage, tmp_path, "2013q2", build_zip(rows=2, overrides={"submission": lines}))
    result = load(storage, spark_resource, "2013q2", ok=False)

    failure = next(e for e in result.all_events if e.is_step_failure)
    assert "lines ['3']" in str(failure.event_specific_data.error)
    assert not (tmp_path / "lake/bronze").exists()


def test_bad_row_in_a_later_table_leaves_the_partition_as_it_was(
    tmp_path: Path, spark_resource: SparkResource
) -> None:
    """A failing rerun must not half-replace the partition: all 7 tables are checked first."""
    good = LakehouseStorage(root_uri=str(tmp_path / "lake"))
    land(good, tmp_path, "2013q2", build_zip(rows=2))
    first = load(good, spark_resource, "2013q2").run_id

    # Same partition, now with a broken last table: replace the raw zip as a test shortcut.
    lines = table_lines(SPECS["infotable"], 2)
    lines[1] += "\textra"
    raw = tmp_path / "lake" / raw_zip_path(DATASETS_BY_KEY["2013q2"])
    raw.unlink()
    land(good, tmp_path, "2013q2", build_zip(rows=5, overrides={"infotable": lines}))
    load(good, spark_resource, "2013q2", ok=False)

    for spec in TABLES:
        df = read(spark_resource, good, f"form13f_{spec.name}")
        assert df.count() == 2, spec.name
        assert {r["_run_id"] for r in df.select("_run_id").collect()} == {first}, spec.name


def test_header_only_table_loads_zero_rows(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    header = "\t".join(c.upper() for c in SPECS["othermanager"].columns)
    land(storage, tmp_path, "2013q2", build_zip(rows=2, overrides={"othermanager": [header]}))
    result = load(storage, spark_resource, "2013q2")
    load(storage, spark_resource, "2013q2")  # and reruns cleanly

    meta = {
        e.asset_key: e.materialization.metadata for e in result.get_asset_materialization_events()
    }
    assert meta[BRONZE_KEYS["othermanager"]]["row_count"].value == 0
    dq2 = evaluations(result)[("othermanager", "dq2_row_count")]
    assert dq2.passed and dq2.metadata["rows"].value == 0  # DQ1 does not apply here
    df = read(spark_resource, storage, "form13f_othermanager")
    assert df.count() == 0
    assert "_dataset" in df.columns


def test_values_survive_spark_unchanged(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    """AC8 on fixtures: what Spark stores equals the bytes in the file, field by field."""
    spec = SPECS["infotable"]
    values = {c: f"{c} 0" for c in spec.columns}
    values.update(
        nameofissuer='  Société Générale "SG" \\ Co.  ',
        cusip="00846U101",
        othermanager="",
        value="0001234",
        voting_auth_none="",
    )
    lines = table_lines(spec, 1)
    lines[1] = "\t".join(values[c] for c in spec.columns)
    lines.append(table_lines(spec, 2)[2])
    land(storage, tmp_path, "2013q2", build_zip(rows=1, overrides={"infotable": lines}))
    load(storage, spark_resource, "2013q2")

    rows = read(spark_resource, storage, "form13f_infotable").orderBy("_source_line").collect()
    assert {c: rows[0][c] for c in spec.columns} == values
    assert [r["_source_line"] for r in rows] == [2, 3]


def test_ingested_at_is_utc_on_a_non_utc_machine(
    tmp_path: Path,
    storage: LakehouseStorage,
    spark_resource: SparkResource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TZ", "America/Toronto")
    time.tzset()
    try:
        land(storage, tmp_path, "2013q2", build_zip(rows=1))
        before = time.time()
        load(storage, spark_resource, "2013q2")
        after = time.time()
    finally:
        monkeypatch.delenv("TZ")
        time.tzset()

    from pyspark.sql import functions as F

    df = read(spark_resource, storage, "form13f_submission")
    epoch = df.select(F.col("_ingested_at").cast("double").alias("t")).first()["t"]
    assert before - 1 <= epoch <= after + 1


def test_header_mismatch_fails_before_any_table_is_written(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    lines = table_lines(SPECS["infotable"], 1)
    lines[0] += "\tNEWCOLUMN"
    lines[1] += "\tx"
    land(storage, tmp_path, "2013q2", build_zip(rows=1, overrides={"infotable": lines}))
    load(storage, spark_resource, "2013q2", ok=False)
    assert not (tmp_path / "lake/bronze").exists()


def evaluations(result: Any) -> dict[tuple[str, str], Any]:
    """Check evaluations of a run, by (table, check name)."""
    return {
        (e.asset_key.path[-1].removeprefix("form13f_"), e.check_name): e
        for e in result.get_asset_check_evaluations()
    }


def test_checks_pass_and_warn_counts_are_recorded(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    info = table_lines(SPECS["infotable"], 3)
    orphan = info[1].split("\t")
    orphan[0] = "0000950123-13-999999"  # a filing SUBMISSION does not list
    info.append("\t".join(orphan))
    land(storage, tmp_path, "2013q2", build_zip(rows=3, overrides={"infotable": info}))
    first = evaluations(load(storage, spark_resource, "2013q2"))

    assert len(first) == 43  # every declared check ran
    blocking = {"dq1_has_rows", "dq3_key_not_null", "dq4_key_unique", "dq5_rows_match_file"}
    assert all(
        e.passed for (_, name), e in first.items() if name in blocking | {"dq9_metadata_valid"}
    )
    assert first[("othermanager", "dq2_row_count")].metadata["rows"].value == 3
    dq6 = first[("infotable", "dq6_accession_in_submission")]
    assert not dq6.passed and dq6.metadata["orphan_accessions"].value == 1
    assert first[("coverpage", "dq6_accession_in_submission")].passed
    # Fixture values such as "cusip 0" are not real CUSIPs or integers: DQ8 counts all 4 rows.
    dq8 = first[("infotable", "dq8_infotable_values")].metadata
    assert (dq8["bad_cusip"].value, dq8["bad_value"].value, dq8["bad_sshprnamt"].value) == (4, 4, 4)
    assert first[("submission", "dq7_accession_in_other_datasets")].passed

    # The next data set repeats the same 3 accession numbers: DQ7 counts them, the run succeeds.
    land(storage, tmp_path, "2013q3", build_zip(rows=3))
    second = evaluations(load(storage, spark_resource, "2013q3"))
    dq7 = second[("submission", "dq7_accession_in_other_datasets")]
    assert not dq7.passed and dq7.metadata["shared_accessions"].value == 3


def _duplicate_submission() -> dict[str, list[str]]:
    lines = table_lines(SPECS["submission"], 2)
    return {"submission": [*lines, lines[1]]}


def _empty_infotable() -> dict[str, list[str]]:
    return {"infotable": table_lines(SPECS["infotable"], 0)}


def _blank_signature_key() -> dict[str, list[str]]:
    lines = table_lines(SPECS["signature"], 2)
    lines[2] = "\t".join([" ", *lines[2].split("\t")[1:]])
    return {"signature": lines}


@pytest.mark.parametrize(
    ("overrides", "table", "check"),
    [
        (_duplicate_submission, "submission", "dq4_key_unique"),
        (_empty_infotable, "infotable", "dq1_has_rows"),
        (_blank_signature_key, "signature", "dq3_key_not_null"),
    ],
    ids=["duplicate-key", "empty-infotable", "blank-key"],
)
def test_blocking_check_fails_the_run_and_writes_nothing(
    tmp_path: Path,
    storage: LakehouseStorage,
    spark_resource: SparkResource,
    overrides: Callable[[], dict[str, list[str]]],
    table: str,
    check: str,
) -> None:
    land(storage, tmp_path, "2013q2", build_zip(rows=2, overrides=overrides()))
    result = load(storage, spark_resource, "2013q2", ok=False)

    assert not evaluations(result)[(table, check)].passed
    failure = next(e for e in result.all_events if e.is_step_failure)
    assert f"{check} on {table}" in str(failure.event_specific_data.error)
    assert not (tmp_path / "lake/bronze").exists()


def test_blocked_rerun_keeps_the_previous_load(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    land(storage, tmp_path, "2013q2", build_zip(rows=2))
    first = load(storage, spark_resource, "2013q2").run_id
    # A clean rerun does not count its own earlier rows as shared with another data set.
    rerun = evaluations(load(storage, spark_resource, "2013q2"))
    assert (
        rerun[("submission", "dq7_accession_in_other_datasets")].metadata["shared_accessions"].value
        == 0
    )
    kept = {s.name: read(spark_resource, storage, f"form13f_{s.name}").collect() for s in TABLES}

    raw = tmp_path / "lake" / raw_zip_path(DATASETS_BY_KEY["2013q2"])
    raw.unlink()
    land(storage, tmp_path, "2013q2", build_zip(rows=2, overrides=_duplicate_submission()))
    load(storage, spark_resource, "2013q2", ok=False)

    for spec in TABLES:
        rows = read(spark_resource, storage, f"form13f_{spec.name}").collect()
        assert rows == kept[spec.name], spec.name
        assert {r["_run_id"] for r in rows} != {first}  # the clean rerun's rows, not the failed one


def test_dq5_mismatch_fails_after_the_write(
    tmp_path: Path,
    storage: LakehouseStorage,
    spark_resource: SparkResource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dataclasses

    from sec_lakehouse.assets import form13f_bronze

    real_unpack = form13f_bronze.unpack

    def miscounting_unpack(*args: Any, **kwargs: Any) -> Any:
        return [dataclasses.replace(t, rows=t.rows + 1) for t in real_unpack(*args, **kwargs)]

    monkeypatch.setattr(form13f_bronze, "unpack", miscounting_unpack)
    land(storage, tmp_path, "2013q2", build_zip(rows=2))
    result = load(storage, spark_resource, "2013q2", ok=False)

    dq5 = evaluations(result)[("submission", "dq5_rows_match_file")]
    assert not dq5.passed
    assert (dq5.metadata["written"].value, dq5.metadata["file_lines"].value) == (2, 3)
    failure = next(e for e in result.all_events if e.is_step_failure)
    assert "wrote 2 rows but the file has 3" in str(failure.event_specific_data.error)
