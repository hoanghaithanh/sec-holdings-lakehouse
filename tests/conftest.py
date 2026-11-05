from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from sec_lakehouse.resources import SparkResource


@pytest.fixture(scope="session")
def spark_resource() -> Iterator[SparkResource]:
    resource = SparkResource(master="local[2]", app_name="sec-lakehouse-test", shuffle_partitions=2)
    yield resource
    session = SparkSession.getActiveSession()
    if session is not None:
        session.stop()
