"""Jobs: named selections of assets that run together (M1 spec, AC4)."""

from __future__ import annotations

from dagster import AssetSelection, define_asset_job

from sec_lakehouse.assets.form13f_bronze import BRONZE_KEYS
from sec_lakehouse.assets.form13f_raw import RAW_ZIP_ASSET

# Partitioned like its assets, so one run handles one data set: download the zip if raw lacks it,
# then load its 7 Bronze tables and run their checks. A backfill of this job over every partition
# is the full M1 load (docs/runbooks/form13f-bronze.md). The raw asset makes no request when the
# zip is already stored.
form13f_bronze_job = define_asset_job(
    "form13f_bronze_job",
    selection=AssetSelection.assets(RAW_ZIP_ASSET, *BRONZE_KEYS.values()),
    description="Land one SEC 13F data set in raw and load its 7 Bronze tables.",
)
