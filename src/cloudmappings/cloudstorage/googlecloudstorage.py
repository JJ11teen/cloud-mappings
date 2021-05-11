from typing import Dict

from google.cloud import storage
from google.cloud.exceptions import Conflict

from .cloudstorage import CloudStorage


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
        self._bucket = self._client.get_bucket(bucket_name)

    def create_if_not_exists(self, metadata: Dict[str, str]):
        exists = False
        try:
            self._client.create_bucket(
                bucket_or_name=self._bucket,
            )
        except Conflict:
            exists = True
        self._bucket.versioning_enabled = True
        self._bucket.patch()
        return exists

    def download_data(self, key: str, etag: str) -> bytes:
        b = self._bucket.get_blob(
            blob_name=key,
        )
        return b.download_as_bytes(
            if_generation_match=etag,
        )

    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        b = self._bucket.get_blob(
            blob_name=key,
        )
        # TODO: can generation cannot be retrieved atomically?
        b.upload_from_string(
            data=data,
            if_generation_match=etag,
        )
        b.reload()
        return b.generation

    def delete_data(self, key: str, etag: str) -> None:
        self._bucket.delete_blob(
            blob_name=key,
            if_generation_match=etag,
        )

    def list_keys_and_ids(self, key_prefix: str) -> Dict[str, str]:
        return {
            b.name: b.generation
            for b in self._client.list_blobs(
                bucket_or_name=self._bucket,
                prefix=key_prefix,
            )
        }
