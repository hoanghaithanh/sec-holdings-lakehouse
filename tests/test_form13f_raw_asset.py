from __future__ import annotations

import hashlib
import io
import zipfile
from collections.abc import Callable, Iterator
from pathlib import Path

import httpx
import pytest
from dagster import materialize

from sec_lakehouse.assets.form13f_raw import form13f_dataset_zip, raw_zip_path
from sec_lakehouse.resources import LakehouseStorage, SecResource
from sec_lakehouse.sources.form13f.datasets import DATASETS_BY_KEY
from sec_lakehouse.sources.sec_http import SecClient

UA = "sec-lakehouse-tests ops@example.com"
Handler = Callable[[httpx.Request], httpx.Response]
Install = Callable[[list[bytes]], list[httpx.Request]]
# The handler the mock SEC client answers with; set per test by the `sec_serves` fixture.
_mock: dict[str, Handler] = {}


class MockSecResource(SecResource):
    """SecResource whose client answers from the test's handler instead of sec.gov."""

    def get_client(self) -> SecClient:
        return SecClient(
            self.user_agent,
            10,
            transport=httpx.MockTransport(_mock["handler"]),
            sleep=lambda _: None,
            jitter=lambda: 0.0,
        )


def zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "SUBMISSION.tsv", "ACCESSION_NUMBER\tFILING_DATE\n0001-13-000001\t15-MAY-2013\n"
        )
    return buffer.getvalue()


@pytest.fixture
def sec_serves() -> Iterator[Install]:
    """Make the mock SEC answer each request with the next body, and record the requests."""

    def install(bodies: list[bytes]) -> list[httpx.Request]:
        seen: list[httpx.Request] = []
        queue = iter(bodies)

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, stream=httpx.ByteStream(next(queue)))

        _mock["handler"] = handler
        return seen

    yield install
    _mock.clear()


def run(storage: LakehouseStorage, key: str = "2013q2") -> dict[str, object]:
    result = materialize(
        [form13f_dataset_zip],
        partition_key=key,
        resources={"sec": MockSecResource(user_agent=UA), "storage": storage},
    )
    assert result.success
    (event,) = result.get_asset_materialization_events()
    metadata = event.materialization.metadata
    return {name: value.value for name, value in metadata.items()}


def test_downloads_once_and_reuses_raw(tmp_path: Path, sec_serves: Install) -> None:
    body = zip_bytes()
    seen = sec_serves([body])
    storage = LakehouseStorage(root_uri=str(tmp_path))
    dataset = DATASETS_BY_KEY["2013q2"]

    first = run(storage)
    assert len(seen) == 1
    assert str(seen[0].url) == dataset.url
    assert first["downloaded"] is True
    assert first["sha256"] == hashlib.sha256(body).hexdigest()
    assert (tmp_path / raw_zip_path(dataset)).read_bytes() == body

    second = run(storage)
    assert len(seen) == 1  # AC6: a rerun sends no request to SEC
    assert second["downloaded"] is False
    assert second["sha256"] == first["sha256"]
    assert second["size_bytes"] == len(body)


def test_uses_the_partitions_own_url(tmp_path: Path, sec_serves: Install) -> None:
    seen = sec_serves([zip_bytes()])
    run(LakehouseStorage(root_uri=str(tmp_path)), key="2026-06_2026-08")
    assert "/datastandardsinnovation/" in str(seen[0].url)
    assert (
        tmp_path / "raw/form13f_datasets/2026-06_2026-08/01jun2026-31aug2026_form13f.zip"
    ).is_file()


def test_corrupt_zip_is_downloaded_again(tmp_path: Path, sec_serves: Install) -> None:
    body = zip_bytes()
    seen = sec_serves([b"not a zip at all", body])
    meta = run(LakehouseStorage(root_uri=str(tmp_path)))
    assert len(seen) == 2
    assert meta["sha256"] == hashlib.sha256(body).hexdigest()


def test_corrupt_twice_fails_and_lands_nothing(tmp_path: Path, sec_serves: Install) -> None:
    sec_serves([b"broken", b"still broken"])
    storage = LakehouseStorage(root_uri=str(tmp_path))
    result = materialize(
        [form13f_dataset_zip],
        partition_key="2013q2",
        resources={"sec": MockSecResource(user_agent=UA), "storage": storage},
        raise_on_error=False,
    )
    assert not result.success
    assert not storage.exists(raw_zip_path(DATASETS_BY_KEY["2013q2"]))
