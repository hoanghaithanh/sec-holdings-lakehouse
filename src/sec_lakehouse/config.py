"""Environment-driven settings.

Every environment (dev on a laptop, uat on the homelab, prod in the cloud) runs the same code.
Only environment variables differ, so nothing is hard-coded to one machine.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from sec_lakehouse.sources.sec_http import DEFAULT_MAX_RPS, validate_max_rps


class Env(StrEnum):
    DEV = "dev"
    UAT = "uat"
    PROD = "prod"


@dataclass(frozen=True)
class Settings:
    env: Env
    spark_master: str
    lakehouse_root: str
    s3_endpoint: str | None
    s3_access_key: str | None
    s3_secret_key: str | None
    spark_extra_conf: dict[str, str]
    # The SEC User-Agent is not here: it is a secret-ish value read only at run time (ADR-003).
    sec_max_requests_per_second: int = DEFAULT_MAX_RPS

    @property
    def uses_object_store(self) -> bool:
        return self.lakehouse_root.startswith("s3a://")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        environ = os.environ if environ is None else environ

        raw_env = environ.get("LAKEHOUSE_ENV", Env.DEV.value).lower()
        try:
            env = Env(raw_env)
        except ValueError as exc:
            valid = ", ".join(e.value for e in Env)
            raise ValueError(f"LAKEHOUSE_ENV must be one of {valid}, got {raw_env!r}") from exc

        settings = cls(
            env=env,
            spark_master=environ.get("SPARK_MASTER", "local[*]"),
            lakehouse_root=environ.get("LAKEHOUSE_ROOT", "data/lakehouse").rstrip("/"),
            s3_endpoint=environ.get("S3_ENDPOINT") or None,
            s3_access_key=environ.get("S3_ACCESS_KEY") or None,
            s3_secret_key=environ.get("S3_SECRET_KEY") or None,
            spark_extra_conf=parse_spark_conf(environ.get("SPARK_EXTRA_CONF", "")),
            sec_max_requests_per_second=parse_max_rps(environ.get("SEC_MAX_REQUESTS_PER_SECOND")),
        )
        settings._validate()
        return settings

    def _validate(self) -> None:
        if self.uses_object_store and not (self.s3_access_key and self.s3_secret_key):
            raise ValueError(
                "LAKEHOUSE_ROOT is s3a:// but S3_ACCESS_KEY / S3_SECRET_KEY are not set"
            )
        if self.env is not Env.DEV and not self.uses_object_store:
            raise ValueError(f"{self.env.value} must store tables in an object store (s3a://)")


def parse_spark_conf(raw: str) -> dict[str, str]:
    """Parse "spark.a=1,spark.b=2" into a dict. Used for per-environment tuning."""
    conf: dict[str, str] = {}
    for item in filter(None, (part.strip() for part in raw.split(","))):
        key, sep, value = item.partition("=")
        if not sep or not key.strip().startswith("spark."):
            raise ValueError(
                f"SPARK_EXTRA_CONF entries must look like spark.key=value, got {item!r}"
            )
        conf[key.strip()] = value.strip()
    return conf


def parse_max_rps(raw: str | None) -> int:
    """SEC_MAX_REQUESTS_PER_SECOND: an integer from 1 to 10, default 5 (ADR-003)."""
    if raw is None or not raw.strip():
        return DEFAULT_MAX_RPS
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"SEC_MAX_REQUESTS_PER_SECOND must be an integer, got {raw!r}") from exc
    return validate_max_rps(value)
