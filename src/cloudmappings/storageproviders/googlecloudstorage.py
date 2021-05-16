from typing import Dict

from google.cloud import storage
from google.cloud.exceptions import Conflict

from .storageprovider import StorageProvider


class GoogleCloudStorageProvider(StorageProvider):
    def __init__(
        self,
        project: str,
        bucket_name: str,
        credentials=None,
    ) -> None:
        self._client = storage.Client(
            project=project,
            credentials=credentials,
        )
        self._bucket = self._client.bucket(
            bucket_name=bucket_name,
        )

    def safe_name(self) -> str:
        return (
            "CloudStorageProvider=GoogleCloudStorage,"
            f"Project={self._client.project},"
            f"BucketName={self._bucket.name}"
        )

    def create_if_not_exists(self):
        exists = False
        try:
            self._client.create_bucket(
                bucket_or_name=self._bucket,
            )
        except Conflict:
            exists = True
        return exists

    def download_data(self, key: str, etag: str) -> bytes:
        b = self._bucket.get_blob(
            blob_name=key,
        )
        if etag is not None and (b is None or etag != b.md5_hash):
            self.raise_key_sync_error(key=key, etag=etag)
        return b.download_as_bytes(
            if_generation_match=b.generation,
        )

    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        b = self._bucket.get_blob(
            blob_name=key,
        )
        existing_etag = None if b is None else b.md5_hash
        if etag != existing_etag:
            self.raise_key_sync_error(key=key, etag=etag)
        if b is None:
            b = self._bucket.blob(
                blob_name=key,
            )
        b.upload_from_string(
            data=data,
            if_generation_match=b.generation,
        )
        return b.md5_hash

    def delete_data(self, key: str, etag: str) -> None:
        b = self._bucket.get_blob(
            blob_name=key,
        )
        if b is None or etag != b.md5_hash:
            self.raise_key_sync_error(key=key, etag=etag)
        self._bucket.delete_blob(
            blob_name=key,
            if_generation_match=b.generation,
        )

    def list_keys_and_etags(self, key_prefix: str) -> Dict[str, str]:
        keys_and_ids = {
            b.name: b.md5_hash
            for b in self._client.list_blobs(
                bucket_or_name=self._bucket,
                prefix=key_prefix,
            )
        }
        return keys_and_ids
