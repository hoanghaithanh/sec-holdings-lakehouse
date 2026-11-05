"""Dagster resource that builds a Delta-enabled SparkSession."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field, PrivateAttr

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Matches the Hadoop 3.3.4 build that ships with PySpark 3.5.
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


class SparkResource(ConfigurableResource):  # type: ignore[type-arg]
    """One SparkSession per run, configured for Delta Lake and, optionally, S3/MinIO."""

    master: str = "local[*]"
    app_name: str = "sec-lakehouse"
    s3_endpoint: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    shuffle_partitions: int = 8
    extra_conf: dict[str, str] = Field(default_factory=dict)

    _session: Any = PrivateAttr(default=None)

    def spark_conf(self) -> dict[str, str]:
        conf = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.session.timeZone": "UTC",
            "spark.ui.showConsoleProgress": "false",
        }
        if self.s3_endpoint:
            conf.update(
                {
                    "spark.hadoop.fs.s3a.endpoint": self.s3_endpoint,
                    "spark.hadoop.fs.s3a.access.key": self.s3_access_key or "",
                    "spark.hadoop.fs.s3a.secret.key": self.s3_secret_key or "",
                    "spark.hadoop.fs.s3a.path.style.access": "true",
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
                        self.s3_endpoint.startswith("https")
                    ).lower(),
                }
            )
        conf.update(self.extra_conf)
        return conf

    def get_session(self) -> SparkSession:
        if self._session is None:
            from delta import configure_spark_with_delta_pip
            from pyspark.sql import SparkSession

            builder = SparkSession.builder.master(self.master).appName(self.app_name)
            for key, value in self.spark_conf().items():
                builder = builder.config(key, value)
            extra_packages = [HADOOP_AWS_PACKAGE] if self.s3_endpoint else []
            self._session = configure_spark_with_delta_pip(
                builder, extra_packages=extra_packages
            ).getOrCreate()
        return self._session  # type: ignore[no-any-return]

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        if self._session is not None:
            self._session.stop()
            self._session = None
