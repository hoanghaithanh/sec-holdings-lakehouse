"""Dagster resource that knows where each medallion layer lives."""

from __future__ import annotations

from typing import Literal

from dagster import ConfigurableResource

Layer = Literal["raw", "bronze", "silver", "gold"]


class LakehouseStorage(ConfigurableResource):  # type: ignore[type-arg]
    """Resolves table paths under one root: a local folder in dev, s3a://bucket elsewhere."""

    root_uri: str

    def table_path(self, layer: Layer, table: str) -> str:
        if not table or "/" in table:
            raise ValueError(f"invalid table name: {table!r}")
        return f"{self.root_uri.rstrip('/')}/{layer}/{table}"
