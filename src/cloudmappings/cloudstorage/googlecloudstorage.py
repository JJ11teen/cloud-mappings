from typing import Dict

from google.cloud import storage
from google.cloud.exceptions import Conflict

from .cloudstorage import CloudStorage, KeyCloudSyncError


class GoogleCloudStorage(CloudStorage):
    def __init__(
        self,
        project: str,
        credentials,
        bucket_name: str,
    ) -> None:
        self._client = storage.Client(
            project=project,
            credentials=credentials,
        )
        self._bucket = self._client.bucket(
            bucket_name=bucket_name,
        )

    def create_if_not_exists(self, metadata: Dict[str, str]):
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
            raise KeyCloudSyncError(key=key, etag=etag)
        return b.download_as_bytes(
            if_generation_match=b.generation,
        )

    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        b = self._bucket.get_blob(
            blob_name=key,
        )
        if b is not None and (etag is None or etag != b.md5_hash):
            raise KeyCloudSyncError(key=key, etag=etag)
        if b is None:
            b = self._bucket.blob(
                blob_name=key,
            )
        b.upload_from_string(
            data=data,
            if_generation_match=b.generation,
        )
        assert b.md5_hash is not None
        return b.md5_hash

    def delete_data(self, key: str, etag: str) -> None:
        b = self._bucket.get_blob(
            blob_name=key,
        )
        if b is None or etag != b.md5_hash:
            raise KeyCloudSyncError(key=key, etag=etag)
        self._bucket.delete_blob(
            blob_name=key,
            if_generation_match=b.generation,
        )

    def list_keys_and_ids(self, key_prefix: str) -> Dict[str, str]:
        keys_and_ids = {
            b.name: b.md5_hash
            for b in self._client.list_blobs(
                bucket_or_name=self._bucket,
                prefix=key_prefix,
            )
        }
        for md5 in keys_and_ids.values():
            assert md5 is not None
        return keys_and_ids
