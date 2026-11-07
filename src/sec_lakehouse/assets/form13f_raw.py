"""Raw layer: each SEC Form 13F data-set zip, downloaded once and kept unchanged (ADR-002/003)."""

# No `from __future__ import annotations` here: Dagster inspects the context annotation at runtime.
import tempfile
import zipfile
import zlib
from datetime import UTC, datetime
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    MaterializeResult,
    MetadataValue,
    asset,
)

from sec_lakehouse.resources import LakehouseStorage, SecResource
from sec_lakehouse.sources.form13f.datasets import (
    DATASETS_BY_KEY,
    FORM13F_PARTITIONS,
    Form13fDataset,
)
from sec_lakehouse.sources.sec_http import DownloadResult, SecClient

RAW_ZIP_ASSET = AssetKey(["raw", "form13f_dataset_zip"])

# A zip that downloads fine but fails its integrity test is fetched again, at most this often.
MAX_DOWNLOADS = 2


class CorruptZipError(RuntimeError):
    pass


def raw_zip_path(dataset: Form13fDataset) -> str:
    return f"raw/form13f_datasets/{dataset.key}/{dataset.zip_name}"


def download_verified_zip(client: SecClient, url: str, dest: Path) -> DownloadResult:
    """Download `url` to `dest` and check it is a readable zip, retrying once if it is not."""
    for attempt in range(1, MAX_DOWNLOADS + 1):
        with dest.open("w+b") as handle:
            result = client.download(url, handle)
        try:
            with zipfile.ZipFile(dest) as archive:
                bad_member = archive.testzip()
        except (zipfile.BadZipFile, zlib.error, EOFError) as exc:
            # A bad CRC shows up as a bad member; damaged deflate data raises instead.
            bad_member = f"{type(exc).__name__}: {exc}"
        if bad_member is None:
            return result
        if attempt == MAX_DOWNLOADS:
            raise CorruptZipError(f"{url} failed the zip integrity test: {bad_member}")
    raise AssertionError("unreachable")  # pragma: no cover


@asset(
    key=RAW_ZIP_ASSET,
    partitions_def=FORM13F_PARTITIONS,
    group_name="form13f",
    compute_kind="python",
)
def form13f_dataset_zip(
    context: AssetExecutionContext, sec: SecResource, storage: LakehouseStorage
) -> MaterializeResult[None]:
    """One SEC Form 13F data-set zip per partition, stored once in raw and never overwritten.

    If the zip is already in raw, no request is sent to SEC at all: the stored copy is re-hashed
    and reported. A republished zip is re-landed by hand (runbook).
    """
    dataset = DATASETS_BY_KEY[context.partition_key]
    path = raw_zip_path(dataset)

    if storage.exists(path):
        context.log.info("Raw zip present, reusing %s", storage.uri(path))
        sha256, size = storage.sha256(path)
        downloaded = False
    else:
        with tempfile.TemporaryDirectory(prefix="form13f-") as tmp_dir:
            tmp = Path(tmp_dir) / dataset.zip_name
            result = download_verified_zip(sec.get_client(), dataset.url, tmp)
            storage.put_new(path, tmp)
        context.log.info("Downloaded %s (%d bytes) to %s", dataset.url, result.size, path)
        sha256, size = result.sha256, result.size
        downloaded = True

    return MaterializeResult(
        metadata={
            "source_url": MetadataValue.url(dataset.url),
            "path": MetadataValue.path(storage.uri(path)),
            "size_bytes": MetadataValue.int(size),
            "sha256": MetadataValue.text(sha256),
            "downloaded": MetadataValue.bool(downloaded),
            "checked_at": MetadataValue.text(datetime.now(UTC).isoformat(timespec="seconds")),
        },
        # Same bytes, same version: lets Dagster tell a real change from a re-run.
        data_version=DataVersion(sha256),
    )
