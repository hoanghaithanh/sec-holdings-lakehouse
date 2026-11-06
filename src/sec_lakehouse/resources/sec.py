"""Dagster resource for the polite sec.gov client (ADR-003)."""

from __future__ import annotations

from typing import Any

from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr

from sec_lakehouse.sources.sec_http import (
    DEFAULT_MAX_RPS,
    SecClient,
    validate_max_rps,
    validate_user_agent,
)


class SecResource(ConfigurableResource):  # type: ignore[type-arg]
    """One SecClient per step, so all requests in a step share one rate limiter.

    `user_agent` comes from the SEC_USER_AGENT env var, resolved when a run starts.
    """

    user_agent: str
    max_requests_per_second: int = DEFAULT_MAX_RPS

    _client: Any = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # Runs when a step starts, so a missing or bad value fails the run before any request.
        validate_user_agent(self.user_agent)
        validate_max_rps(self.max_requests_per_second)

    def get_client(self) -> SecClient:
        if self._client is None:
            self._client = SecClient(self.user_agent, self.max_requests_per_second)
        return self._client  # type: ignore[no-any-return]

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
