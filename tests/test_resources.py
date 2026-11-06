import pytest

from sec_lakehouse.resources import LakehouseStorage, SparkResource


def test_table_path_per_layer() -> None:
    storage = LakehouseStorage(root_uri="s3a://lakehouse/")
    assert storage.table_path("bronze", "filings") == "s3a://lakehouse/bronze/filings"


@pytest.mark.parametrize("bad", ["", "a/b"])
def test_table_path_rejects_bad_names(bad: str) -> None:
    with pytest.raises(ValueError):
        LakehouseStorage(root_uri="data").table_path("raw", bad)


def test_spark_conf_local_has_no_s3() -> None:
    conf = SparkResource().spark_conf()
    assert conf["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert not any(key.startswith("spark.hadoop.fs.s3a") for key in conf)


def test_spark_conf_object_store() -> None:
    conf = SparkResource(
        s3_endpoint="http://object-store:9000", s3_access_key="k", s3_secret_key="s"
    ).spark_conf()
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://object-store:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"


def test_sec_resource_fails_at_start_without_user_agent() -> None:
    from dagster import build_init_resource_context

    from sec_lakehouse.resources import SecResource

    with pytest.raises(ValueError, match="SEC_USER_AGENT"):
        SecResource(user_agent="").setup_for_execution(build_init_resource_context())


def test_sec_resource_builds_client() -> None:
    from sec_lakehouse.resources import SecResource

    client = SecResource(user_agent="sec-lakehouse-tests ops@example.com").get_client()
    client.close()
