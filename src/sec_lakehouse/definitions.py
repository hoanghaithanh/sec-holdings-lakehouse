"""Dagster code location. Resources are built from environment settings."""

from __future__ import annotations

from dagster import Definitions, EnvVar, load_assets_from_modules

from sec_lakehouse.assets import form13f_bronze, form13f_raw, hello
from sec_lakehouse.config import Settings
from sec_lakehouse.jobs import form13f_bronze_job
from sec_lakehouse.resources import LakehouseStorage, SecResource, SparkResource


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
        "storage": LakehouseStorage(
            root_uri=settings.lakehouse_root,
            s3_endpoint=settings.s3_endpoint,
            s3_access_key=access_key,
            s3_secret_key=secret_key,
        ),
        # Resolved when a run starts, so validation and CI work without it (ADR-003).
        "sec": SecResource(
            user_agent=EnvVar("SEC_USER_AGENT"),
            max_requests_per_second=settings.sec_max_requests_per_second,
        ),
    }


defs = Definitions(
    assets=load_assets_from_modules([hello, form13f_raw, form13f_bronze]),
    jobs=[form13f_bronze_job],
    resources=build_resources(Settings.from_env()),
)
