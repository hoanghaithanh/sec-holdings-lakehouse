from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from sec_lakehouse.resources import SparkResource


@pytest.fixture(scope="session")
def spark_resource() -> Iterator[SparkResource]:
    # One JVM serves the whole suite (each run stops its session, not the JVM), and the Silver
    # tests build wide query plans; the 1 GB default heap ran out late in a full run.
    resource = SparkResource(
        master="local[2]",
        app_name="sec-lakehouse-test",
        shuffle_partitions=2,
        extra_conf={"spark.driver.memory": "2g"},
    )
    yield resource
    session = SparkSession.getActiveSession()
    if session is not None:
        session.stop()
