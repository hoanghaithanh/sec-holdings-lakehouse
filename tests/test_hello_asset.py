from pathlib import Path

from dagster import materialize

from sec_lakehouse.assets.hello import SAMPLE_MANAGERS, hello_delta
from sec_lakehouse.resources import LakehouseStorage, SparkResource


def test_hello_delta_writes_a_delta_table(tmp_path: Path, spark_resource: SparkResource) -> None:
    result = materialize(
        [hello_delta],
        resources={"spark": spark_resource, "storage": LakehouseStorage(root_uri=str(tmp_path))},
    )
    assert result.success

    table_path = tmp_path / "bronze" / "hello_delta"
    assert (table_path / "_delta_log").is_dir()

    metadata = result.asset_materializations_for_node("hello_delta")[0].metadata
    assert metadata["row_count"].value == len(SAMPLE_MANAGERS)

    rows = spark_resource.get_session().read.format("delta").load(str(table_path)).collect()
    assert {row.cik for row in rows} == {cik for cik, _ in SAMPLE_MANAGERS}


def test_rerun_is_idempotent(tmp_path: Path, spark_resource: SparkResource) -> None:
    resources = {"spark": spark_resource, "storage": LakehouseStorage(root_uri=str(tmp_path))}
    materialize([hello_delta], resources=resources)
    result = materialize([hello_delta], resources=resources)

    metadata = result.asset_materializations_for_node("hello_delta")[0].metadata
    assert metadata["row_count"].value == len(SAMPLE_MANAGERS)
    assert metadata["delta_version"].value == 1
