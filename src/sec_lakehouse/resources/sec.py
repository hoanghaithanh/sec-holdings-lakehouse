"""Dagster resource for the polite sec.gov client (ADR-003)."""

from __future__ import annotations

from dagster import ConfigurableResource, InitResourceContext

from sec_lakehouse.sources.sec_http import (
    DEFAULT_MAX_RPS,
    SecClient,
    validate_max_rps,
    validate_user_agent,
)


class SecResource(ConfigurableResource):  # type: ignore[type-arg]
    """Builds a SecClient. `user_agent` comes from the SEC_USER_AGENT env var at run time."""

    user_agent: str
    max_requests_per_second: int = DEFAULT_MAX_RPS

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # Runs when a step starts, so a missing or bad value fails the run before any request.
        validate_user_agent(self.user_agent)
        validate_max_rps(self.max_requests_per_second)

    def get_client(self) -> SecClient:
        return SecClient(self.user_agent, self.max_requests_per_second)
