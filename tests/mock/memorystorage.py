import uuid
from typing import Dict

from cloudmappings.cloudstorage.cloudstorage import CloudStorage


class MemoryStorage(CloudStorage):
    def __init__(self, existing_data) -> None:
        self._data = existing_data

    def create_if_not_exists(self, metadata: Dict[str, str]) -> bool:
        exists = self._data is not None
        if not exists:
            self._data = {}
        return exists

    def download_data(self, key: str, etag: str) -> bytes:
        existing_etag, data = self._data[key]
        if etag != existing_etag:
            raise ValueError("Wrong Etag. Data in cloud has changed")
        return data

    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        new_etag = uuid.uuid4()
        existing_etag, _ = self._data.get(key, (None, None))
        if etag is not None and etag != existing_etag:
            raise ValueError("Wrong Etag. Data in cloud has changed")
        self._data[key] = (new_etag, data)
        return new_etag

    def delete_data(self, key: str, etag: str) -> None:
        existing_etag, _ = self._data[key]
        if etag != existing_etag:
            raise ValueError("Wrong Etag. Data in cloud has changed")
        del self._data[key]

    def list_keys_and_ids(self, key_prefix: str) -> Dict[str, str]:
        return {key: etag for key, (etag, _) in self._data.items() if key.startsWith(key_prefix)}
