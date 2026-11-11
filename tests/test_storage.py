from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from sec_lakehouse.resources import LakehouseStorage


def write(tmp_path: Path, name: str, data: bytes) -> Path:
    path = tmp_path / name
    path.write_bytes(data)
    return path


# --- local disk ---------------------------------------------------------------------------------


def test_local_put_new_then_read(tmp_path: Path) -> None:
    storage = LakehouseStorage(root_uri=str(tmp_path / "lake"))
    source = write(tmp_path, "a.zip", b"zip bytes")

    assert not storage.exists("raw/x/a.zip")
    storage.put_new("raw/x/a.zip", source)

    assert storage.exists("raw/x/a.zip")
    assert b"".join(storage.iter_bytes("raw/x/a.zip")) == b"zip bytes"
    assert storage.sha256("raw/x/a.zip") == (hashlib.sha256(b"zip bytes").hexdigest(), 9)
    # No temp files left next to the stored one.
    assert [p.name for p in (tmp_path / "lake/raw/x").iterdir()] == ["a.zip"]


def test_local_put_new_refuses_to_overwrite(tmp_path: Path) -> None:
    storage = LakehouseStorage(root_uri=str(tmp_path))
    storage.put_new("raw/a.zip", write(tmp_path, "one", b"first"))
    with pytest.raises(FileExistsError, match="write-once"):
        storage.put_new("raw/a.zip", write(tmp_path, "two", b"second"))
    assert b"".join(storage.iter_bytes("raw/a.zip")) == b"first"


@pytest.mark.parametrize(
    "bad", ["", "/abs", "raw/../escape", "raw\\win", "C:/x", "C:x", "raw//a", "raw/./a"]
)
def test_rejects_paths_outside_the_root(tmp_path: Path, bad: str) -> None:
    with pytest.raises(ValueError, match="invalid storage path"):
        LakehouseStorage(root_uri=str(tmp_path)).exists(bad)


# --- S3 (fake client, no network) ---------------------------------------------------------------


class FakeS3:
    """Just enough of the boto3 S3 client for LakehouseStorage."""

    class exceptions:
        ClientError = ClientError

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:
        self.objects[(Bucket, Key)] = Path(Filename).read_bytes()

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        data = self.objects[(Bucket, Key)]

        class Body:
            def iter_chunks(self, size: int) -> list[bytes]:
                return [data[i : i + size] for i in range(0, len(data), size)]

            def close(self) -> None:
                pass

        return {"Body": Body()}

    def get_paginator(self, name: str) -> Any:
        objects = self.objects

        class Paginator:
            def paginate(self, Bucket: str, Prefix: str) -> list[dict[str, Any]]:
                keys = sorted(k for b, k in objects if b == Bucket and k.startswith(Prefix))
                # Two pages, to exercise pagination.
                return [
                    {"Contents": [{"Key": k} for k in keys[:1]]},
                    {"Contents": [{"Key": k} for k in keys[1:]]},
                ]

        assert name == "list_objects_v2"
        return Paginator()

    def delete_objects(self, Bucket: str, Delete: dict[str, Any]) -> dict[str, Any]:
        for item in Delete["Objects"]:
            del self.objects[(Bucket, item["Key"])]
        return {}


def s3_storage(root: str = "s3a://lakehouse") -> tuple[LakehouseStorage, FakeS3]:
    storage = LakehouseStorage(root_uri=root, s3_endpoint="http://object-store:9000")
    fake = FakeS3()
    storage._s3 = fake
    return storage, fake


def test_s3_put_new_uses_bucket_and_key(tmp_path: Path) -> None:
    storage, fake = s3_storage()
    assert not storage.exists("raw/a.zip")
    storage.put_new("raw/a.zip", write(tmp_path, "a", b"data"))
    assert fake.objects == {("lakehouse", "raw/a.zip"): b"data"}
    assert storage.exists("raw/a.zip")
    assert storage.sha256("raw/a.zip")[1] == 4
    assert storage.uri("raw/a.zip") == "s3a://lakehouse/raw/a.zip"


def test_s3_root_with_prefix(tmp_path: Path) -> None:
    storage, fake = s3_storage("s3a://bucket/uat/")
    storage.put_new("raw/a.zip", write(tmp_path, "a", b"x"))
    assert ("bucket", "uat/raw/a.zip") in fake.objects


def test_s3_put_new_refuses_to_overwrite(tmp_path: Path) -> None:
    storage, _ = s3_storage()
    storage.put_new("raw/a.zip", write(tmp_path, "a", b"x"))
    with pytest.raises(FileExistsError):
        storage.put_new("raw/a.zip", write(tmp_path, "b", b"y"))


def test_s3_other_errors_are_not_hidden() -> None:
    storage, fake = s3_storage()

    def denied(Bucket: str, Key: str) -> dict[str, Any]:
        raise ClientError({"Error": {"Code": "403"}}, "HeadObject")

    fake.head_object = denied  # type: ignore[method-assign]
    with pytest.raises(ClientError):
        storage.exists("raw/a.zip")


def test_local_put_new_loses_a_race_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = LakehouseStorage(root_uri=str(tmp_path / "lake"))
    # Another writer lands the file after our exists() check.
    monkeypatch.setattr(LakehouseStorage, "exists", lambda self, path: False)
    storage.put_new("raw/a.zip", write(tmp_path, "one", b"first"))
    with pytest.raises(FileExistsError):
        storage.put_new("raw/a.zip", write(tmp_path, "two", b"second"))
    assert (tmp_path / "lake/raw/a.zip").read_bytes() == b"first"
    assert [p.name for p in (tmp_path / "lake/raw").iterdir()] == ["a.zip"]


def test_s3_existing_object_is_not_uploaded_again(tmp_path: Path) -> None:
    storage, fake = s3_storage()
    storage.put_new("raw/a.zip", write(tmp_path, "a", b"x"))
    uploads: list[str] = []
    fake.upload_file = lambda Filename, Bucket, Key: uploads.append(Key)  # type: ignore[method-assign]
    with pytest.raises(FileExistsError):
        storage.put_new("raw/a.zip", write(tmp_path, "b", b"y"))
    assert uploads == []


def test_s3_client_uses_path_style_and_quiet_checksums() -> None:
    storage = LakehouseStorage(
        root_uri="s3a://lakehouse",
        s3_endpoint="http://object-store:9000",
        s3_access_key="k",
        s3_secret_key="s",
    )
    config = storage._s3_client().meta.config
    assert config.s3 is not None and config.s3["addressing_style"] == "path"
    assert config.request_checksum_calculation == "when_required"


def test_staging_can_be_overwritten_and_deleted(tmp_path: Path) -> None:
    storage = LakehouseStorage(root_uri=str(tmp_path / "lake"))
    storage.put_staging("staging/form13f/2013q2/a.tsv", write(tmp_path, "one", b"first"))
    storage.put_staging("staging/form13f/2013q2/a.tsv", write(tmp_path, "two", b"second"))
    assert b"".join(storage.iter_bytes("staging/form13f/2013q2/a.tsv")) == b"second"
    storage.delete_prefix("staging/form13f/2013q2")
    assert not storage.exists("staging/form13f/2013q2/a.tsv")
    storage.delete_prefix("staging/form13f/never-written")  # no error


@pytest.mark.parametrize("bad", ["raw/a.zip", "bronze/form13f_submission", "staging", "staging/"])
def test_only_staging_can_be_overwritten_or_deleted(tmp_path: Path, bad: str) -> None:
    storage = LakehouseStorage(root_uri=str(tmp_path))
    with pytest.raises(ValueError):
        storage.delete_prefix(bad)
    with pytest.raises(ValueError):
        storage.put_staging(bad, write(tmp_path, "x", b"x"))


def test_download_to_copies_a_stored_file(tmp_path: Path) -> None:
    storage = LakehouseStorage(root_uri=str(tmp_path / "lake"))
    storage.put_new("raw/a.zip", write(tmp_path, "a", b"zip"))
    storage.download_to("raw/a.zip", tmp_path / "copy.zip")
    assert (tmp_path / "copy.zip").read_bytes() == b"zip"


def test_s3_staging_overwrite_and_delete_prefix(tmp_path: Path) -> None:
    storage, fake = s3_storage()
    for name in ["a.tsv", "b.tsv", "c.tsv"]:
        storage.put_staging(f"staging/form13f/2013q2/{name}", write(tmp_path, name, b"x"))
    storage.put_staging("staging/form13f/2013q2/a.tsv", write(tmp_path, "a2", b"new"))
    storage.put_staging("staging/form13f/2013q2x/a.tsv", write(tmp_path, "sib", b"keep"))
    storage.put_new("raw/a.zip", write(tmp_path, "raw", b"raw"))
    assert fake.objects[("lakehouse", "staging/form13f/2013q2/a.tsv")] == b"new"

    storage.delete_prefix("staging/form13f/2013q2")

    assert sorted(k for _, k in fake.objects) == [
        "raw/a.zip",
        "staging/form13f/2013q2x/a.tsv",  # a sibling prefix is not deleted
    ]
