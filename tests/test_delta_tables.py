"""The change set and the change-set MERGE (ADR-005), on a tiny partitioned Delta table."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from sec_lakehouse.delta_tables import (
    DELETE,
    INSERT,
    OP,
    UPDATE,
    change_set,
    date_literals,
    ensure_table,
    merge_change_set,
    replace_partition,
)
from sec_lakehouse.resources import SparkResource

Q1, Q2, Q3 = date(2024, 3, 31), date(2024, 6, 30), date(2024, 9, 30)
COLUMNS = ["k", "period", "v", "_dataset"]


@pytest.fixture
def session(spark_resource: SparkResource) -> Any:
    return spark_resource.model_copy().get_session()


def frame(session: Any, rows: list[tuple[str, date, str | None, str]]) -> Any:
    return session.createDataFrame(rows, "k string, period date, v string, _dataset string")


def test_date_literals_are_sorted_and_distinct() -> None:
    assert date_literals([Q2, Q1, Q2]) == "DATE'2024-03-31', DATE'2024-06-30'"


def test_change_set_classifies_rows(session: Any) -> None:
    old = frame(session, [("a", Q1, "x", "d1"), ("b", Q1, None, "d1"), ("c", Q1, "z", "d1")])
    new = frame(session, [("a", Q1, "x", "d1"), ("b", Q1, "y", "d2"), ("e", Q2, None, "d2")])

    changes = change_set(new, old, ["k"], COLUMNS)
    ops = {r["k"]: (r[OP], r["_old_dataset"]) for r in changes.collect()}

    # "a" is identical and dropped; a null that becomes a value is a change (null-safe compare).
    assert ops == {"b": (UPDATE, "d1"), "c": (DELETE, "d1"), "e": (INSERT, None)}


def test_merge_moves_a_row_to_its_new_partition_and_skips_others(
    session: Any, tmp_path: Path
) -> None:
    path = str(tmp_path / "t")
    schema = frame(session, []).schema
    ensure_table(session, path, schema, "period")
    ensure_table(session, path, schema, "period")  # a second call is a no-op
    frame(session, [("a", Q1, "x", "d1"), ("b", Q1, "y", "d1"), ("z", Q3, "q", "d0")]).write.format(
        "delta"
    ).mode("append").save(path)

    old = frame(session, [("a", Q1, "x", "d1"), ("b", Q1, "y", "d1")])
    new = frame(session, [("a", Q2, "x", "d1")])  # a's period changed, b left the data set
    changes = change_set(new, old, ["k"], COLUMNS)
    merge_change_set(session, path, changes, ["k"], "period", [Q1, Q2])

    rows = sorted((r["k"], r["period"]) for r in session.read.format("delta").load(path).collect())
    assert rows == [("a", Q2), ("z", Q3)]  # moved, not duplicated; Q3 untouched


def test_replace_partition_with_an_empty_frame_empties_it(session: Any, tmp_path: Path) -> None:
    path = str(tmp_path / "q")
    rows = frame(session, [("a", Q1, "x", "d1"), ("b", Q1, "y", "d2")])
    ensure_table(session, path, rows.schema, "_dataset")
    rows.write.format("delta").mode("append").save(path)

    replace_partition(rows.limit(0), path, "_dataset", "d1")

    left = [r["_dataset"] for r in session.read.format("delta").load(path).collect()]
    assert left == ["d2"]
