"""Dagster resource that knows where each medallion layer lives, and stores raw files.

The root is a local folder in dev (`data/lakehouse`) or an S3 bucket elsewhere
(`s3a://lakehouse`). Spark reads and writes tables through the root URI; plain files such as
raw SEC zips go through the small object API below (ADR-003), so no step needs Spark just to
copy a file.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlsplit

from dagster import ConfigurableResource
from pydantic import PrivateAttr

if TYPE_CHECKING:
    from types_boto3_s3 import S3Client

Layer = Literal["raw", "bronze", "silver", "gold"]

CHUNK_SIZE = 1024 * 1024


class LakehouseStorage(ConfigurableResource):  # type: ignore[type-arg]
    """Resolves table paths under one root and stores files there, write-once."""

    root_uri: str
    s3_endpoint: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None

    _s3: Any = PrivateAttr(default=None)

    # --- tables (Spark) ----------------------------------------------------------------------

    def table_path(self, layer: Layer, table: str) -> str:
        if not table or "/" in table:
            raise ValueError(f"invalid table name: {table!r}")
        return f"{self.root_uri.rstrip('/')}/{layer}/{table}"

    # --- files -------------------------------------------------------------------------------

    @property
    def is_object_store(self) -> bool:
        return self.root_uri.startswith("s3a://")

    def uri(self, path: str) -> str:
        """Full URI of a file under the root, for logs and Spark."""
        return f"{self.root_uri.rstrip('/')}/{_check_path(path)}"

    def exists(self, path: str) -> bool:
        path = _check_path(path)
        if not self.is_object_store:
            return self._local(path).is_file()
        bucket, key = self._bucket_key(path)
        client = self._s3_client()
        try:
            client.head_object(Bucket=bucket, Key=key)
        except client.exceptions.ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def put_new(self, path: str, source: str | os.PathLike[str]) -> None:
        """Store a local file at `path`. Refuses to overwrite: raw files are written once.

        The file appears whole or not at all, so a failed upload never leaves a partial file.
        """
        path = _check_path(path)
        if self.exists(path):
            raise FileExistsError(f"{self.uri(path)} already exists; raw files are write-once")
        if not self.is_object_store:
            target = self._local(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
            shutil.copyfile(source, tmp)
            try:
                os.link(tmp, target)  # atomic, and fails if another writer got there first
            finally:
                tmp.unlink()
            return
        bucket, key = self._bucket_key(path)
        # upload_file switches to multipart for big files; S3 shows the object only when done.
        self._s3_client().upload_file(os.fspath(source), bucket, key)

    def iter_bytes(self, path: str) -> Iterator[bytes]:
        """Stream a stored file in chunks."""
        path = _check_path(path)
        if not self.is_object_store:
            with self._local(path).open("rb") as handle:
                while chunk := handle.read(CHUNK_SIZE):
                    yield chunk
            return
        bucket, key = self._bucket_key(path)
        body = self._s3_client().get_object(Bucket=bucket, Key=key)["Body"]
        try:
            yield from body.iter_chunks(CHUNK_SIZE)
        finally:
            body.close()

    def sha256(self, path: str) -> tuple[str, int]:
        """SHA-256 and size of a stored file, read as a stream."""
        digest = hashlib.sha256()
        size = 0
        for chunk in self.iter_bytes(path):
            digest.update(chunk)
            size += len(chunk)
        return digest.hexdigest(), size

    # --- internals ---------------------------------------------------------------------------

    def _local(self, path: str) -> Path:
        return Path(self.root_uri) / path

    def _bucket_key(self, path: str) -> tuple[str, str]:
        parts = urlsplit(self.root_uri)
        prefix = parts.path.strip("/")
        return parts.netloc, f"{prefix}/{path}" if prefix else path

    def _s3_client(self) -> S3Client:
        if self._s3 is None:
            import boto3

            self._s3 = boto3.client(
                "s3",
                endpoint_url=self.s3_endpoint,
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
            )
        return self._s3  # type: ignore[no-any-return]


def _check_path(path: str) -> str:
    """Relative, forward-slash paths only, so nothing can escape the root."""
    if not path or path.startswith("/") or "\\" in path or ".." in path.split("/"):
        raise ValueError(f"invalid storage path: {path!r}")
    return path
