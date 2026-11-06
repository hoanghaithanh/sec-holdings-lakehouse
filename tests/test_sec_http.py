from __future__ import annotations

import hashlib
import io
from collections.abc import Callable, Iterator

import httpx
import pytest

from sec_lakehouse.sources.sec_http import (
    MAX_ATTEMPTS,
    RateLimiter,
    SecBlockedError,
    SecClient,
    SecNotFoundError,
    SecRequestError,
    validate_user_agent,
)

UA = "sec-lakehouse-tests ops@example.com"
URL = "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2013q2_form13f.zip"


class FakeClock:
    """Time only moves when the code under test sleeps, so timing tests are exact and instant."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


Handler = Callable[[httpx.Request], httpx.Response]


def make_client(handler: Handler, clock: FakeClock, max_rps: int = 5) -> SecClient:
    return SecClient(
        UA,
        max_rps,
        transport=httpx.MockTransport(handler),
        clock=clock,
        sleep=clock.sleep,
        jitter=lambda: 0.0,
    )


def scripted(responses: list[httpx.Response], seen: list[httpx.Request]) -> Handler:
    queue = iter(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return next(queue)

    return handler


# --- User-Agent and config ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    ["", "   ", "sec-lakehouse", "ops@example.com", "app ops@example.com\nX-Evil: 1"],
)
def test_rejects_bad_user_agent(bad: str) -> None:
    with pytest.raises(ValueError, match="SEC_USER_AGENT"):
        validate_user_agent(bad)


def test_client_refuses_to_start_without_user_agent() -> None:
    with pytest.raises(ValueError, match="SEC_USER_AGENT"):
        SecClient("")


@pytest.mark.parametrize("bad", [0, 11, 50])
def test_rejects_rate_above_sec_limit(bad: int) -> None:
    with pytest.raises(ValueError, match="SEC_MAX_REQUESTS_PER_SECOND"):
        SecClient(UA, bad)


def test_sends_user_agent_and_only_talks_to_sec() -> None:
    seen: list[httpx.Request] = []
    clock = FakeClock()
    client = make_client(scripted([httpx.Response(200, content=b"ok")], seen), clock)

    assert client.get(URL) == b"ok"
    assert seen[0].headers["User-Agent"] == UA

    for other in ["https://example.com/x.zip", "http://www.sec.gov/x.zip"]:
        with pytest.raises(ValueError, match="only fetches"):
            client.get(other)
    assert len(seen) == 1


def test_does_not_follow_redirects() -> None:
    seen: list[httpx.Request] = []
    redirect = httpx.Response(301, headers={"Location": "https://example.com/"})
    client = make_client(scripted([redirect], seen), FakeClock())
    with pytest.raises(SecRequestError, match="unexpected response"):
        client.get(URL)
    assert len(seen) == 1


# --- Rate limit (AC9) --------------------------------------------------------------------------


@pytest.mark.parametrize("max_rps", [5, 10])
def test_never_more_than_max_rps_in_any_second(max_rps: int) -> None:
    clock = FakeClock()
    starts: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        starts.append(clock.now)
        return httpx.Response(200, content=b"x")

    client = make_client(handler, clock, max_rps=max_rps)
    for _ in range(50):
        client.get(URL)

    assert len(starts) == 50
    for start in starts:
        in_window = [t for t in starts if start <= t < start + 1.0]
        assert len(in_window) <= max_rps
    # And it is not needlessly slow: 50 requests take about 50 / max_rps seconds.
    assert starts[-1] == pytest.approx(49 / max_rps, rel=0.01)


def test_rate_limiter_counts_retries() -> None:
    clock = FakeClock()
    limiter = RateLimiter(10, clock=clock, sleep=clock.sleep)
    limiter.acquire()
    limiter.acquire()
    assert clock.sleeps == [pytest.approx(0.1, abs=0.002)]


# --- Retries and failures ----------------------------------------------------------------------


def test_retries_5xx_with_backoff_then_succeeds() -> None:
    seen: list[httpx.Request] = []
    clock = FakeClock()
    responses = [httpx.Response(503), httpx.Response(502), httpx.Response(200, content=b"ok")]
    client = make_client(scripted(responses, seen), clock)

    assert client.get(URL) == b"ok"
    assert len(seen) == 3
    backoffs = [s for s in clock.sleeps if s >= 1.0]
    assert backoffs == [1.0, 2.0]


def test_honours_longer_retry_after_and_caps_it() -> None:
    seen: list[httpx.Request] = []
    clock = FakeClock()
    responses = [
        httpx.Response(429, headers={"Retry-After": "7"}),
        httpx.Response(429, headers={"Retry-After": "600"}),
        httpx.Response(200, content=b"ok"),
    ]
    client = make_client(scripted(responses, seen), clock)

    assert client.get(URL) == b"ok"
    backoffs = [s for s in clock.sleeps if s >= 1.0]
    assert backoffs == [7.0, 60.0]


def test_gives_up_after_max_attempts() -> None:
    seen: list[httpx.Request] = []
    clock = FakeClock()
    client = make_client(scripted([httpx.Response(500)] * MAX_ATTEMPTS, seen), clock)

    with pytest.raises(SecRequestError, match="giving up") as info:
        client.get(URL)
    assert len(seen) == MAX_ATTEMPTS
    assert info.value.attempts == MAX_ATTEMPTS
    assert info.value.status == 500
    assert [s for s in clock.sleeps if s >= 1.0] == [1.0, 2.0, 4.0, 8.0]


def test_retries_timeouts() -> None:
    clock = FakeClock()
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadTimeout("slow", request=request)
        return httpx.Response(200, content=b"ok")

    assert make_client(handler, clock).get(URL) == b"ok"
    assert calls == 2


def test_403_fails_at_once() -> None:
    seen: list[httpx.Request] = []
    client = make_client(scripted([httpx.Response(403)], seen), FakeClock())
    with pytest.raises(SecBlockedError, match="check SEC_USER_AGENT") as info:
        client.get(URL)
    assert len(seen) == 1
    assert UA not in str(info.value)


def test_404_fails_at_once() -> None:
    seen: list[httpx.Request] = []
    client = make_client(scripted([httpx.Response(404)], seen), FakeClock())
    with pytest.raises(SecNotFoundError):
        client.get(URL)
    assert len(seen) == 1


# --- Streaming downloads -----------------------------------------------------------------------


class CutOffStream(httpx.SyncByteStream):
    """A body that drops the connection after the first chunk."""

    def __iter__(self) -> Iterator[bytes]:
        yield b"partial"
        raise httpx.RemoteProtocolError("peer closed connection")


def test_download_streams_and_hashes() -> None:
    body = b"PK" + bytes(range(256)) * 10_000
    seen: list[httpx.Request] = []
    response = httpx.Response(
        200, headers={"Content-Length": str(len(body))}, stream=httpx.ByteStream(body)
    )
    client = make_client(scripted([response], seen), FakeClock())

    dest = io.BytesIO()
    result = client.download(URL, dest)

    assert dest.getvalue() == body
    assert result.size == len(body)
    assert result.content_length == len(body)
    assert result.sha256 == hashlib.sha256(body).hexdigest()
    assert result.attempts == 1
    assert seen[0].headers["Accept-Encoding"] == "identity"


def test_download_retries_a_cut_off_stream_from_scratch() -> None:
    body = b"the whole file"
    seen: list[httpx.Request] = []
    responses = [
        httpx.Response(200, stream=CutOffStream()),
        httpx.Response(200, stream=httpx.ByteStream(body)),
    ]
    client = make_client(scripted(responses, seen), FakeClock())

    dest = io.BytesIO()
    result = client.download(URL, dest)

    assert dest.getvalue() == body  # no leftover bytes from the failed attempt
    assert result.attempts == 2
    assert result.sha256 == hashlib.sha256(body).hexdigest()


def test_download_retries_when_shorter_than_content_length() -> None:
    body = b"complete"
    seen: list[httpx.Request] = []
    short = httpx.Response(
        200, headers={"Content-Length": "100"}, stream=httpx.ByteStream(b"short")
    )
    client = make_client(
        scripted([short, httpx.Response(200, stream=httpx.ByteStream(body))], seen), FakeClock()
    )

    dest = io.BytesIO()
    result = client.download(URL, dest)

    assert dest.getvalue() == body
    assert result.attempts == 2


def test_error_reports_the_real_attempt_count() -> None:
    seen: list[httpx.Request] = []
    responses = [httpx.Response(503), httpx.Response(503), httpx.Response(403)]
    client = make_client(scripted(responses, seen), FakeClock())
    with pytest.raises(SecBlockedError, match="attempts=3") as info:
        client.get(URL)
    assert info.value.attempts == 3


def test_download_refuses_an_encoded_body() -> None:
    seen: list[httpx.Request] = []
    gzipped = httpx.Response(
        200, headers={"Content-Encoding": "gzip"}, stream=httpx.ByteStream(b"\x1f\x8b")
    )
    client = make_client(scripted([gzipped], seen), FakeClock())
    with pytest.raises(SecRequestError, match="Content-Encoding"):
        client.download(URL, io.BytesIO())
    assert len(seen) == 1
