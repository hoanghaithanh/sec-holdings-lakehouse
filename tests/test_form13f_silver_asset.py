"""Silver filings and quarantine, end to end: Bronze from a zip, then the Silver asset (S1).

Real samples come from tests/fixtures/form13f. Hand-made data sets (`filings_zip`) are marked as
such in the fixtures README: they hold made-up filings with chosen values, to hit one rule each.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from dagster import AssetCheckSeverity, materialize
from form13f_zips import build_zip, table_lines

from sec_lakehouse.assets.form13f_bronze import bronze_form13f
from sec_lakehouse.assets.form13f_raw import raw_zip_path
from sec_lakehouse.assets.form13f_silver import silver_form13f
from sec_lakehouse.resources import LakehouseStorage, SparkResource
from sec_lakehouse.sources.form13f import silver_schemas
from sec_lakehouse.sources.form13f.datasets import DATASETS_BY_KEY
from sec_lakehouse.sources.form13f.schemas import TABLES
from sec_lakehouse.sources.form13f.silver_schemas import FILING_COLUMNS, RUN_COLUMNS

FIXTURES = Path(__file__).parent / "fixtures" / "form13f"
SPECS = {spec.name: spec for spec in TABLES}
BUSINESS = [name for name, _, _ in FILING_COLUMNS if name not in RUN_COLUMNS]

SUBMISSION_DEFAULTS = {
    "filing_date": "15-MAY-2013",
    "submissiontype": "13F-HR",
    "cik": "0000000001",
    "periodofreport": "31-MAR-2013",
}
COVERPAGE_DEFAULTS = {
    "reportcalendarorquarter": "31-MAR-2013",
    "reporttype": "13F HOLDINGS REPORT",
    "filingmanager_name": "Made Up Capital LLC ",
}
SUMMARYPAGE_DEFAULTS = {
    "otherincludedmanagerscount": "0",
    "tableentrytotal": "10",
    "tablevaluetotal": "1000",
    "isconfidentialomitted": "N",
}


def accession(i: int) -> str:
    return f"0000950123-13-{i:06d}"


def _lines(table: str, rows: list[dict[str, str]]) -> list[str]:
    columns = SPECS[table].columns
    return ["\t".join(c.upper() for c in columns)] + [
        "\t".join(row.get(c, "") for c in columns) for row in rows
    ]


def filings_zip(
    filings: list[dict[str, str]],
    drop: dict[str, set[str]] | None = None,
    extra: dict[str, list[dict[str, str]]] | None = None,
) -> bytes:
    """A hand-made data set. Each filing dict names `accession_number` and overrides defaults.

    `drop` leaves an accession out of a table; `extra` adds rows (orphans) to a table.
    """
    drop = drop or {}
    extra = extra or {}
    tables: dict[str, list[dict[str, str]]] = {}
    for table, defaults in (
        ("submission", SUBMISSION_DEFAULTS),
        ("coverpage", COVERPAGE_DEFAULTS),
        ("summarypage", SUMMARYPAGE_DEFAULTS),
    ):
        columns = SPECS[table].columns
        tables[table] = [
            {**defaults, **{k: v for k, v in f.items() if k in columns}}
            for f in filings
            if f["accession_number"] not in drop.get(table, set())
        ] + extra.get(table, [])
    overrides = {name: _lines(name, rows) for name, rows in tables.items()}
    # The other four tables only need rows for Bronze's DQ1; they are not read by S1.
    for name in ("othermanager", "signature", "othermanager2", "infotable"):
        overrides[name] = table_lines(SPECS[name], 1)
    return build_zip(rows=1, overrides=overrides)


def filing(i: int, **values: str) -> dict[str, str]:
    return {"accession_number": accession(i), **values}


@pytest.fixture
def storage(tmp_path: Path) -> LakehouseStorage:
    return LakehouseStorage(root_uri=str(tmp_path / "lake"))


def land_bronze(
    storage: LakehouseStorage, spark: SparkResource, tmp_path: Path, key: str, data: bytes
) -> None:
    """Put the zip in raw (replacing any earlier copy, as a manual re-land would) and load it."""
    raw = Path(storage.uri(raw_zip_path(DATASETS_BY_KEY[key])))
    raw.unlink(missing_ok=True)
    source = tmp_path / f"{key}.zip"
    source.write_bytes(data)
    storage.put_new(raw_zip_path(DATASETS_BY_KEY[key]), source)
    result = materialize(
        [bronze_form13f],
        partition_key=key,
        resources={"spark": spark, "storage": storage},
        raise_on_error=False,
    )
    assert result.success


def run_silver(storage: LakehouseStorage, spark: SparkResource, key: str, ok: bool = True) -> Any:
    result = materialize(
        [silver_form13f],
        partition_key=key,
        resources={"spark": spark, "storage": storage},
        raise_on_error=False,
    )
    assert result.success is ok
    return result


def read(spark: SparkResource, storage: LakehouseStorage, table: str) -> Any:
    # A fresh resource: each run's teardown stops the session the run used.
    session = spark.model_copy().get_session()
    return session.read.format("delta").load(storage.table_path("silver", table))


def filings(spark: SparkResource, storage: LakehouseStorage) -> dict[str, Any]:
    return {r["accession_number"]: r for r in read(spark, storage, "form13f_filing").collect()}


def checks(result: Any) -> dict[tuple[str, str], Any]:
    return {(e.asset_key.path[-1], e.check_name): e for e in result.get_asset_check_evaluations()}


def materialized(result: Any) -> dict[str, Any]:
    return {
        e.asset_key.path[-1]: e.materialization.metadata
        for e in result.get_asset_materialization_events()
    }


@pytest.fixture
def lenient_quarantine() -> Iterator[None]:
    """Hand-made data sets are tiny, so one bad row is far above the 1% SDQ6 block."""
    overrides = silver_schemas.QUARANTINE_BLOCK_RATIO_OVERRIDES
    saved = dict(overrides)
    overrides.update({"2013q2": 1.0, "2013q3": 1.0})
    yield
    overrides.clear()
    overrides.update(saved)


# --- real samples ------------------------------------------------------------------------------


@pytest.mark.parametrize("key", ["2013q2", "2022q4", "2026-06_2026-08"])
def test_real_sample_becomes_typed_filings(
    key: str, tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    storage.put_new(raw_zip_path(DATASETS_BY_KEY[key]), FIXTURES / f"{key}_form13f_sample.zip")
    assert materialize(
        [bronze_form13f], partition_key=key, resources={"spark": spark_resource, "storage": storage}
    ).success
    result = run_silver(storage, spark_resource, key)

    rows = filings(spark_resource, storage)
    assert len(rows) == 10  # every real filing is clean: nothing quarantined
    assert read(spark_resource, storage, "form13f_quarantine").count() == 0
    df = read(spark_resource, storage, "form13f_filing")
    assert [(f.name, f.dataType.simpleString()) for f in df.schema.fields] == [
        (name, kind.replace("bigint", "bigint")) for name, kind, _ in FILING_COLUMNS
    ]  # AC6: the declared schema, no string dates or amounts
    for (table, name), evaluation in checks(result).items():
        if evaluation.severity == AssetCheckSeverity.ERROR:
            assert evaluation.passed, (table, name)
    meta = materialized(result)["form13f_filing"]
    assert meta["inserted"].value == 10 and meta["rows_for_dataset"].value == 10


def test_real_restatement_and_notice_values(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    for key in ("2013q2", "2022q4"):
        storage.put_new(raw_zip_path(DATASETS_BY_KEY[key]), FIXTURES / f"{key}_form13f_sample.zip")
        assert materialize(
            [bronze_form13f],
            partition_key=key,
            resources={"spark": spark_resource, "storage": storage},
        ).success
        run_silver(storage, spark_resource, key)
    rows = filings(spark_resource, storage)

    restatement = rows["0001085146-22-003667"]
    assert restatement["submission_type"] == "13F-HR/A"
    assert restatement["is_amendment"] is True
    assert restatement["amendment_no"] == 1
    assert restatement["amendment_type"] == "RESTATEMENT"
    assert restatement["filing_date"] == date(2022, 10, 31)
    assert restatement["period_of_report"] == date(2022, 6, 30)
    assert restatement["value_unit"] == "THOUSANDS"
    assert restatement["table_value_total_reported"] == Decimal(147712)
    assert restatement["table_value_total_usd"] == Decimal(147712000)
    assert restatement["_dataset"] == "2022q4"

    notice = rows["0000950123-13-003781"]  # 13F-NT/A, NEW HOLDINGS, no SUMMARYPAGE row
    assert notice["submission_type"] == "13F-NT/A"
    assert notice["amendment_type"] == "NEW HOLDINGS"
    assert notice["amendment_no"] == 2
    assert notice["period_of_report"] == date(2012, 9, 30)
    assert notice["table_entry_total"] is None and notice["is_confidential_omitted"] is None

    original = rows["0001566653-13-000012"]
    assert original["is_amendment"] is False and original["amendment_no"] is None
    assert original["cik"] == "0001566653"
    assert original["is_confidential_omitted"] is False


# --- hand-made data sets -----------------------------------------------------------------------


def test_value_unit_follows_the_filing_date(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    land_bronze(
        storage,
        spark_resource,
        tmp_path,
        "2013q2",
        filings_zip(
            [
                filing(1, filing_date="30-DEC-2022", periodofreport="30-SEP-2022"),
                filing(2, filing_date="03-JAN-2023", periodofreport="30-SEP-2022"),
            ]
        ),
    )
    run_silver(storage, spark_resource, "2013q2")
    rows = filings(spark_resource, storage)

    assert rows[accession(1)]["value_unit"] == "THOUSANDS"
    assert rows[accession(1)]["table_value_total_usd"] == Decimal(1_000_000)
    assert rows[accession(2)]["value_unit"] == "DOLLARS"  # the boundary day is in dollars
    assert rows[accession(2)]["table_value_total_usd"] == Decimal(1000)


def test_cleaning_rules(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    land_bronze(
        storage,
        spark_resource,
        tmp_path,
        "2013q2",
        filings_zip(
            [
                filing(
                    1,
                    submissiontype="13f-hr/a",
                    amendmentno="1",
                    amendmenttype="new holdings",
                    isamendment="N",  # contradicts the submission type: SDQ10 counts it
                    confdeniedexpired="X",  # not Y/N: null, counted
                    datereported="31-FEB-2013",  # optional bad date: null, counted
                    filingmanager_street1="  ",
                )
            ]
        ),
    )
    result = run_silver(storage, spark_resource, "2013q2")
    row = filings(spark_resource, storage)[accession(1)]

    assert row["submission_type"] == "13F-HR/A" and row["amendment_type"] == "NEW HOLDINGS"
    assert row["filing_manager_name"] == "Made Up Capital LLC"  # trimmed, case kept
    assert row["filing_manager_street1"] is None  # blank becomes null
    assert row["conf_denied_expired"] is None and row["date_reported"] is None
    sdq10 = checks(result)[("form13f_filing", "sdq10_unrecognised_values")]
    assert not sdq10.passed and sdq10.severity == AssetCheckSeverity.WARN
    assert sdq10.metadata["unrecognised_flags"].value == 1
    assert sdq10.metadata["isamendment_mismatch"].value == 1
    assert sdq10.metadata["bad_optional_dates"].value == 1


@pytest.mark.usefixtures("lenient_quarantine")
def test_each_reason_code_lands_in_quarantine_and_leaves_when_fixed(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    bad = [
        filing(1),  # clean
        filing(2, filing_date="2013-05-15"),  # BAD_DATE
        filing(3, periodofreport="15-MAR-2013"),  # BAD_PERIOD: not a quarter end
        filing(4, periodofreport="30-JUN-2013"),  # BAD_PERIOD: after the filing date
        filing(5, submissiontype="13F-XX"),  # BAD_SUBMISSION_TYPE
        filing(6, tablevaluetotal="12345678901234567"),  # BAD_INTEGER: 17 digits
        filing(7, filing_date="", tableentrytotal="-1"),  # two reasons, in the fixed order
        filing(8),  # no COVERPAGE row (dropped below): its required date is missing
    ]
    orphan = {"accession_number": accession(99), **COVERPAGE_DEFAULTS}
    land_bronze(
        storage,
        spark_resource,
        tmp_path,
        "2013q2",
        filings_zip(bad, drop={"coverpage": {accession(8)}}, extra={"coverpage": [orphan]}),
    )
    result = run_silver(storage, spark_resource, "2013q2")

    assert set(filings(spark_resource, storage)) == {accession(1)}
    quarantine = {
        r["accession_number"]: r
        for r in read(spark_resource, storage, "form13f_quarantine").collect()
    }
    codes = {a: (r["source_table"], r["reason_codes"]) for a, r in quarantine.items()}
    assert codes == {
        accession(2): ("submission", ["BAD_DATE"]),
        accession(3): ("submission", ["BAD_PERIOD"]),
        accession(4): ("submission", ["BAD_PERIOD"]),
        accession(5): ("submission", ["BAD_SUBMISSION_TYPE"]),
        accession(6): ("submission", ["BAD_INTEGER"]),
        accession(7): ("submission", ["BAD_DATE", "BAD_INTEGER"]),
        accession(8): ("submission", ["BAD_DATE"]),
        accession(99): ("coverpage", ["NO_FILING"]),
    }
    raw = json.loads(quarantine[accession(2)]["raw_row"])
    assert raw["submission"]["filing_date"] == "2013-05-15"  # the original string, replayable
    assert raw["coverpage"]["reportcalendarorquarter"] == "31-MAR-2013"
    assert "filing_date=2013-05-15" in quarantine[accession(2)]["reason_detail"]
    assert quarantine[accession(8)]["reason_detail"] == "no COVERPAGE row for this filing"
    found = checks(result)
    assert found[("form13f_filing", "sdq3_rows_conserved")].passed  # 8 = 1 + 7 + 0
    sdq6 = found[("form13f_filing", "sdq6_quarantine_rate")]
    assert not sdq6.passed and sdq6.severity == AssetCheckSeverity.WARN
    assert found[("form13f_quarantine", "quarantine_row_count")].metadata["rows"].value == 8

    # Re-land the data set with every row fixed: quarantine empties, all filings arrive.
    land_bronze(
        storage, spark_resource, tmp_path, "2013q2", filings_zip([filing(i) for i in range(1, 9)])
    )
    run_silver(storage, spark_resource, "2013q2")
    assert set(filings(spark_resource, storage)) == {accession(i) for i in range(1, 9)}
    assert read(spark_resource, storage, "form13f_quarantine").count() == 0


def test_quarantine_above_threshold_blocks_and_writes_nothing(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    land_bronze(
        storage,
        spark_resource,
        tmp_path,
        "2013q2",
        filings_zip([filing(1), filing(2, submissiontype="13F-XX")]),
    )
    result = run_silver(storage, spark_resource, "2013q2", ok=False)

    sdq6 = checks(result)[("form13f_filing", "sdq6_quarantine_rate")]
    assert not sdq6.passed and sdq6.severity == AssetCheckSeverity.ERROR
    assert sdq6.metadata["quarantine_ratio"].value == 0.5
    assert not Path(storage.table_path("silver", "form13f_filing")).exists()


def test_rerun_changes_nothing(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    land_bronze(storage, spark_resource, tmp_path, "2013q2", filings_zip([filing(1), filing(2)]))
    first = run_silver(storage, spark_resource, "2013q2").run_id
    second = run_silver(storage, spark_resource, "2013q2")

    meta = materialized(second)["form13f_filing"]
    assert (meta["inserted"].value, meta["updated"].value, meta["deleted"].value) == (0, 0, 0)
    assert {r["_silver_run_id"] for r in filings(spark_resource, storage).values()} == {first}


def test_relanded_data_set_updates_moves_and_deletes(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    land_bronze(
        storage,
        spark_resource,
        tmp_path,
        "2013q2",
        filings_zip([filing(1), filing(2), filing(3)]),
    )
    first = run_silver(storage, spark_resource, "2013q2").run_id
    land_bronze(
        storage,
        spark_resource,
        tmp_path,
        "2013q2",
        filings_zip(
            [
                filing(1),
                filing(2, periodofreport="31-DEC-2012", reportcalendarorquarter="31-DEC-2012"),
            ]
        ),
    )
    result = run_silver(storage, spark_resource, "2013q2")

    rows = filings(spark_resource, storage)
    assert set(rows) == {accession(1), accession(2)}  # 3 left Bronze, so it left Silver
    assert rows[accession(2)]["period_of_report"] == date(2012, 12, 31)  # moved, not duplicated
    assert rows[accession(1)]["_silver_run_id"] == first  # unchanged rows keep their run id
    meta = materialized(result)["form13f_filing"]
    assert (meta["inserted"].value, meta["updated"].value, meta["deleted"].value) == (0, 1, 1)
    assert meta["handed_back_to"].value == "none (gone from Bronze)"


@pytest.mark.parametrize("order", [("2013q2", "2013q3"), ("2013q3", "2013q2")])
def test_shared_accession_goes_to_the_latest_data_set_in_any_order(
    order: tuple[str, str], tmp_path: Path, spark_resource: SparkResource
) -> None:
    storage = LakehouseStorage(root_uri=str(tmp_path / "lake"))
    data = {
        "2013q2": filings_zip([filing(1), filing(2, filingmanager_name="Old Name")]),
        "2013q3": filings_zip([filing(2, filingmanager_name="New Name"), filing(3)]),
    }
    for key in ("2013q2", "2013q3"):  # complete Bronze first (Q8)
        land_bronze(storage, spark_resource, tmp_path, key, data[key])
    results = {key: run_silver(storage, spark_resource, key) for key in order}

    rows = filings(spark_resource, storage)
    content = sorted(tuple(r[c] for c in BUSINESS) for r in rows.values())
    assert [r[0] for r in content] == [accession(1), accession(2), accession(3)]
    assert rows[accession(2)]["_dataset"] == "2013q3"
    assert rows[accession(2)]["filing_manager_name"] == "New Name"
    assert rows[accession(1)]["_silver_run_id"] == results["2013q2"].run_id
    q2 = checks(results["2013q2"])[("form13f_filing", "sdq3_rows_conserved")]
    assert q2.passed and q2.metadata["superseded"].value == 1


def test_later_data_set_takes_over_an_accession_silver_already_has(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    # Bronze grows after Silver ran: 2013q3 arrives later and holds a newer copy of filing 2.
    land_bronze(storage, spark_resource, tmp_path, "2013q2", filings_zip([filing(1), filing(2)]))
    run_silver(storage, spark_resource, "2013q2")
    land_bronze(
        storage,
        spark_resource,
        tmp_path,
        "2013q3",
        filings_zip([filing(2, filingmanager_name="New Name")]),
    )
    result = run_silver(storage, spark_resource, "2013q3")

    rows = filings(spark_resource, storage)
    assert rows[accession(2)]["_dataset"] == "2013q3"
    assert rows[accession(2)]["filing_manager_name"] == "New Name"
    meta = materialized(result)["form13f_filing"]
    assert (meta["inserted"].value, meta["updated"].value) == (0, 1)
    assert meta["taken_over_from"].value == "2013q2"

    # Rerunning the earlier data set now leaves the taken-over row alone.
    rerun = run_silver(storage, spark_resource, "2013q2")
    meta = materialized(rerun)["form13f_filing"]
    assert (meta["inserted"].value, meta["updated"].value, meta["deleted"].value) == (0, 0, 0)
    assert filings(spark_resource, storage)[accession(2)]["_dataset"] == "2013q3"


def test_half_loaded_bronze_is_refused(
    tmp_path: Path, storage: LakehouseStorage, spark_resource: SparkResource
) -> None:
    run_silver(storage, spark_resource, "2013q2", ok=False)  # no Bronze at all

    land_bronze(storage, spark_resource, tmp_path, "2013q2", filings_zip([filing(1)]))
    from delta.tables import DeltaTable

    session = spark_resource.model_copy().get_session()
    DeltaTable.forPath(session, storage.table_path("bronze", "form13f_coverpage")).update(
        set={"_run_id": "'an-older-run'"}
    )
    result = run_silver(storage, spark_resource, "2013q2", ok=False)
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "half loaded" in str(failure.event_specific_data.error)
