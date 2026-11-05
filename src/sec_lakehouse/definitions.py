"""Dagster code location. Resources are built from environment settings."""

from __future__ import annotations

from dagster import Definitions, EnvVar, load_assets_from_modules

from sec_lakehouse.assets import hello
from sec_lakehouse.config import Settings
from sec_lakehouse.resources import LakehouseStorage, SparkResource


def build_resources(settings: Settings) -> dict[str, object]:
    # Secrets go in as EnvVar so Dagster resolves them at run time and never shows them in the UI.
    access_key = EnvVar("S3_ACCESS_KEY") if settings.s3_access_key else None
    secret_key = EnvVar("S3_SECRET_KEY") if settings.s3_secret_key else None
    return {
        "spark": SparkResource(
            master=settings.spark_master,
            app_name=f"sec-lakehouse-{settings.env.value}",
            s3_endpoint=settings.s3_endpoint,
            s3_access_key=access_key,
            s3_secret_key=secret_key,
            extra_conf=settings.spark_extra_conf,
        ),
        "storage": LakehouseStorage(root_uri=settings.lakehouse_root),
    }


defs = Definitions(
    assets=load_assets_from_modules([hello]),
    resources=build_resources(Settings.from_env()),
)
