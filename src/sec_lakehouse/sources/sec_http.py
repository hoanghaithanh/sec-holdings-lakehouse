"""A polite HTTP client for sec.gov.

SEC's fair-access rules: at most 10 requests per second, and every request names the app and a
contact email in its User-Agent. This client enforces both, retries transient failures with
backoff, and streams large downloads without holding them in memory. See ADR-003.
"""

from __future__ import annotations

import hashlib
import logging
import random
import re
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import BinaryIO, TypeVar
from urllib.parse import urlsplit

import httpx

log = logging.getLogger(__name__)

T = TypeVar("T")

SEC_HOST = "www.sec.gov"
SEC_MAX_ALLOWED_RPS = 10
DEFAULT_MAX_RPS = 5

MAX_ATTEMPTS = 5
BACKOFF_SECONDS = (1.0, 2.0, 4.0, 8.0)
MAX_JITTER_SECONDS = 0.5
MAX_RETRY_AFTER_SECONDS = 60.0
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
CHUNK_SIZE = 1024 * 1024
# Added to the spacing between requests so clock rounding can never squeeze in an extra one.
RATE_SAFETY_SECONDS = 0.001

_EMAIL = re.compile(r"[^\s@]+@[^\s@]+\.[A-Za-z]{2,}")


class SecRequestError(RuntimeError):
    """A request to sec.gov failed for good (after retries, or with a non-retryable status)."""

    def __init__(self, message: str, *, url: str, status: int | None, attempts: int) -> None:
        super().__init__(message)
        self.message = message
        self.url = url
        self.status = status
        self.attempts = attempts

    def __str__(self) -> str:
        # Built on demand: the retry loop updates `attempts` after the error is created.
        return f"{self.message} (url={self.url}, status={self.status}, attempts={self.attempts})"


class SecBlockedError(SecRequestError):
    """SEC answered 403: retrying would make a block worse, so this fails at once."""


class SecNotFoundError(SecRequestError):
    """SEC answered 404."""


class _RetryableError(Exception):
    def __init__(
        self, reason: str, status: int | None = None, retry_after: float | None = None
    ) -> None:
        super().__init__(reason)
        self.status = status
        self.retry_after = retry_after


@dataclass(frozen=True)
class DownloadResult:
    url: str
    size: int
    sha256: str
    content_length: int | None
    attempts: int


def validate_user_agent(user_agent: str) -> str:
    """Check the User-Agent names an app and a contact email, as SEC requires."""
    value = user_agent.strip()
    if not value:
        raise ValueError("SEC_USER_AGENT is not set: SEC requires an app name and a contact email")
    if "\r" in value or "\n" in value:
        raise ValueError("SEC_USER_AGENT must be a single line")
    if not _EMAIL.search(value):
        raise ValueError("SEC_USER_AGENT must contain a contact email, e.g. 'my-app admin@x.org'")
    if not _EMAIL.sub("", value).strip():
        raise ValueError("SEC_USER_AGENT must name the app as well as the contact email")
    return value


def validate_max_rps(max_rps: int) -> int:
    if not 1 <= max_rps <= SEC_MAX_ALLOWED_RPS:
        raise ValueError(
            f"SEC_MAX_REQUESTS_PER_SECOND must be from 1 to {SEC_MAX_ALLOWED_RPS}, got {max_rps}"
        )
    return max_rps


class RateLimiter:
    """Spaces request starts at least 1/max_rps seconds apart. Thread-safe."""

    def __init__(
        self,
        max_rps: int,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._interval = 1.0 / validate_max_rps(max_rps) + RATE_SAFETY_SECONDS
        self._clock = clock
        self._sleep = sleep
        self._last: float | None = None
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            if self._last is not None:
                wait = self._last + self._interval - now
                if wait > 0:
                    self._sleep(wait)
                    now = self._clock()
            self._last = now


class SecClient:
    """HTTP client for sec.gov with a rate limit, a declared User-Agent and retries."""

    def __init__(
        self,
        user_agent: str,
        max_rps: int = DEFAULT_MAX_RPS,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] | None = None,
    ) -> None:
        self._limiter = RateLimiter(max_rps, clock=clock, sleep=sleep)
        self._sleep = sleep
        self._jitter = jitter or (lambda: random.uniform(0.0, MAX_JITTER_SECONDS))
        # Redirects are not followed, so the User-Agent (with its email) only ever goes to SEC.
        self._http = httpx.Client(
            transport=transport,
            headers={"User-Agent": validate_user_agent(user_agent)},
            timeout=httpx.Timeout(60.0, connect=10.0),
            follow_redirects=False,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> SecClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def get(self, url: str) -> bytes:
        """Fetch a small document (index pages, XML) into memory."""

        def attempt() -> bytes:
            with self._stream(url) as response:
                return response.read()

        return self._with_retries(url, attempt)

    def download(self, url: str, dest: BinaryIO) -> DownloadResult:
        """Stream a large file into `dest` (seekable; rewound on each retry), hashing as it goes."""
        attempts = 0

        def attempt() -> DownloadResult:
            nonlocal attempts
            attempts += 1
            dest.seek(0)
            dest.truncate()
            digest = hashlib.sha256()
            size = 0
            # identity: the bytes on disk are exactly the file, and Content-Length counts them.
            with self._stream(url, headers={"Accept-Encoding": "identity"}) as response:
                header = response.headers.get("Content-Length")
                content_length = int(header) if header and header.isdigit() else None
                encoding = response.headers.get("Content-Encoding", "identity").lower()
                if encoding != "identity":
                    raise SecRequestError(
                        f"SEC sent Content-Encoding {encoding!r} for a download",
                        url=url,
                        status=response.status_code,
                        attempts=attempts,
                    )
                for chunk in response.iter_raw(CHUNK_SIZE):
                    dest.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
            if content_length is not None and size != content_length:
                raise _RetryableError(f"stream cut off at {size} of {content_length} bytes")
            dest.flush()
            return DownloadResult(url, size, digest.hexdigest(), content_length, attempts)

        return self._with_retries(url, attempt)

    @contextmanager
    def _stream(self, url: str, headers: dict[str, str] | None = None) -> Iterator[httpx.Response]:
        _check_sec_url(url)
        self._limiter.acquire()
        try:
            with self._http.stream("GET", url, headers=headers) as response:
                status = response.status_code
                if status == 403:
                    raise SecBlockedError(
                        "SEC refused the request: check SEC_USER_AGENT and the request rate",
                        url=url,
                        status=status,
                        attempts=1,
                    )
                if status == 404:
                    raise SecNotFoundError(
                        "not found on sec.gov", url=url, status=status, attempts=1
                    )
                if status in RETRY_STATUSES:
                    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                    raise _RetryableError(f"HTTP {status}", status, retry_after)
                if status != 200:
                    raise SecRequestError(
                        "unexpected response from sec.gov", url=url, status=status, attempts=1
                    )
                yield response
        except httpx.TransportError as exc:
            # Timeouts, refused connections and streams cut off mid-body.
            raise _RetryableError(f"{type(exc).__name__}: {exc}") from exc

    def _with_retries(self, url: str, attempt: Callable[[], T]) -> T:
        for number in range(1, MAX_ATTEMPTS + 1):
            try:
                return attempt()
            except SecRequestError as exc:
                exc.attempts = number
                raise
            except _RetryableError as exc:
                if number == MAX_ATTEMPTS:
                    raise SecRequestError(
                        f"giving up: {exc}", url=url, status=exc.status, attempts=number
                    ) from exc
                wait = BACKOFF_SECONDS[number - 1] + self._jitter()
                if exc.retry_after is not None:
                    wait = max(wait, min(exc.retry_after, MAX_RETRY_AFTER_SECONDS))
                log.warning(
                    "SEC request failed (%s), retry %d in %.1fs: %s", exc, number, wait, url
                )
                self._sleep(wait)
        raise AssertionError("unreachable")  # pragma: no cover


def _check_sec_url(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.hostname != SEC_HOST:
        raise ValueError(f"SecClient only fetches https://{SEC_HOST}/ URLs, got {url!r}")


def _parse_retry_after(value: str | None) -> float | None:
    """Retry-After is either seconds or an HTTP date."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - time.time())
