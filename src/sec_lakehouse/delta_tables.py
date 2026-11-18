"""Delta helpers for Silver: declared tables, change sets and the change-set MERGE (ADR-005).

A Silver run never rewrites rows that did not change. It first works out the change set, the
rows to insert, update or delete, and then `MERGE`s only those, with the affected Delta
partitions written as literals in the condition so Delta reads only those folders.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date
from functools import reduce
from typing import Any

INSERT, UPDATE, DELETE = "I", "U", "D"
OP = "_op"
OLD_DATASET = "_old_dataset"


def ensure_table(session: Any, path: str, schema: Any, partition_by: str) -> None:
    """Create the table from its declared schema if it does not exist yet.

    So the first write never infers a schema; later writes must match it (schema enforcement).
    """
    from delta.tables import DeltaTable

    (
        DeltaTable.createIfNotExists(session)
        .location(path)
        .addColumns(schema)
        .partitionedBy(partition_by)
        .execute()
    )


def date_literals(values: Iterable[date]) -> str:
    """`DATE'2024-03-31', DATE'2024-06-30'`: a literal list, which is what lets Delta prune."""
    return ", ".join(f"DATE'{value.isoformat()}'" for value in sorted(set(values)))


def change_set(
    new: Any,
    old: Any,
    keys: Sequence[str],
    compare: Sequence[str],
    dataset_column: str = "_dataset",
) -> Any:
    """Rows to insert, update or delete so that `old` becomes `new` (ADR-005, step 5).

    `new` and `old` have the same columns and are unique on `keys`. A key only in `new` is an
    insert, only in `old` a delete, in both with any `compare` column different (null-safe) an
    update; identical rows are dropped. Each change row carries the columns of `new` (of `old`
    for a delete), `_op`, and `_old_dataset` (the data set the row had before, or null).
    """
    from pyspark.sql import functions as F

    columns = new.columns
    n, o = new.alias("n"), old.alias("o")
    on = [F.col(f"n.{k}") == F.col(f"o.{k}") for k in keys]
    same = reduce(
        lambda a, b: a & b, (F.col(f"n.{c}").eqNullSafe(F.col(f"o.{c}")) for c in compare)
    )
    upserts = (
        n.join(o, on, "left")
        .where(F.col(f"o.{keys[0]}").isNull() | ~same)
        .select(
            *(F.col(f"n.{c}").alias(c) for c in columns),
            F.when(F.col(f"o.{keys[0]}").isNull(), INSERT).otherwise(UPDATE).alias(OP),
            F.col(f"o.{dataset_column}").alias(OLD_DATASET),
        )
    )
    deletes = old.join(new.select(*keys), list(keys), "left_anti").select(
        *columns, F.lit(DELETE).alias(OP), F.col(dataset_column).alias(OLD_DATASET)
    )
    return upserts.unionByName(deletes)


def merge_change_set(
    session: Any,
    path: str,
    changes: Any,
    keys: Sequence[str],
    partition_column: str,
    partitions: Iterable[date],
) -> None:
    """Apply a change set in one `MERGE` that reads only `partitions` of the target.

    `t.<partition> = s.<partition>` alone does not prune; a literal `IN` list does (checked on
    delta-spark 3.3.3, ADR-005). Every changed row's old and new partition must be listed, so a
    row whose partition changes is moved, not duplicated. Each target row matches at most one
    change row (both sides are unique on the keys), and Delta fails the `MERGE` if not.
    """
    from delta.tables import DeltaTable

    columns = [c for c in changes.columns if c not in (OP, OLD_DATASET)]
    assignments: dict[str, Any] = {c: f"s.`{c}`" for c in columns}
    condition = f"t.`{partition_column}` IN ({date_literals(partitions)}) AND " + " AND ".join(
        f"t.`{k}` = s.`{k}`" for k in keys
    )
    (
        DeltaTable.forPath(session, path)
        .alias("t")
        .merge(changes.alias("s"), condition)
        .whenMatchedDelete(condition=f"s.{OP} = '{DELETE}'")
        .whenMatchedUpdate(condition=f"s.{OP} <> '{DELETE}'", set=assignments)
        .whenNotMatchedInsert(condition=f"s.{OP} <> '{DELETE}'", values=assignments)
        .execute()
    )


def replace_partition(df: Any, path: str, partition_column: str, value: str) -> None:
    """Overwrite one partition in one commit; an empty `df` empties it (`replaceWhere`)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{partition_column} = '{value}'")
        .save(path)
    )
