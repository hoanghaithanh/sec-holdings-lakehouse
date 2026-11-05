import pytest

from sec_lakehouse.config import Env, Settings


def test_defaults_to_local_dev() -> None:
    settings = Settings.from_env({})
    assert settings.env is Env.DEV
    assert settings.spark_master == "local[*]"
    assert settings.lakehouse_root == "data/lakehouse"
    assert not settings.uses_object_store


def test_uat_reads_object_store_settings() -> None:
    settings = Settings.from_env(
        {
            "LAKEHOUSE_ENV": "UAT",
            "SPARK_MASTER": "spark://spark-master:7077",
            "LAKEHOUSE_ROOT": "s3a://lakehouse/",
            "S3_ENDPOINT": "http://object-store:9000",
            "S3_ACCESS_KEY": "key",
            "S3_SECRET_KEY": "secret",
        }
    )
    assert settings.env is Env.UAT
    assert settings.lakehouse_root == "s3a://lakehouse"
    assert settings.uses_object_store


def test_rejects_unknown_env() -> None:
    with pytest.raises(ValueError, match="LAKEHOUSE_ENV"):
        Settings.from_env({"LAKEHOUSE_ENV": "staging"})


def test_object_store_requires_credentials() -> None:
    with pytest.raises(ValueError, match="S3_ACCESS_KEY"):
        Settings.from_env({"LAKEHOUSE_ROOT": "s3a://lakehouse"})


def test_uat_cannot_use_local_disk() -> None:
    with pytest.raises(ValueError, match="object store"):
        Settings.from_env({"LAKEHOUSE_ENV": "uat", "LAKEHOUSE_ROOT": "/tmp/lake"})


def test_parses_spark_extra_conf() -> None:
    settings = Settings.from_env(
        {"SPARK_EXTRA_CONF": "spark.driver.host=dagster-daemon, spark.executor.memory=18g"}
    )
    assert settings.spark_extra_conf == {
        "spark.driver.host": "dagster-daemon",
        "spark.executor.memory": "18g",
    }


def test_rejects_malformed_spark_conf() -> None:
    with pytest.raises(ValueError, match="SPARK_EXTRA_CONF"):
        Settings.from_env({"SPARK_EXTRA_CONF": "driver.host"})
