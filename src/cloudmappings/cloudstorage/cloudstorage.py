from abc import ABC, abstractmethod
from typing import Dict


class KeyCloudSyncError(ValueError):
    def __init__(self, key: str, etag: str) -> None:
        super().__init__(f"Mapping is out of sync with cloud data. Key: '{key}', etag: '{etag}'")


class CloudStorage(ABC):
    @abstractmethod
    def create_if_not_exists(self, metadata: Dict[str, str]) -> bool:
        """Create a new parent resource for the data in cloud storage.
        Terminology various between cloud providers:
            * Azure Blob Storage: A Blob Container
            * GCP GCS: A GCS Bucket
            * AWS S3: An S3 Bucket
        :return: True if already exists, otherwise False.
        """
        pass

    @abstractmethod
    def download_data(self, key: str, etag: str) -> bytes:
        """Download data from cloud storage. Raise KeyCloudSyncError if etag does not match the
        latest version in the cloud.
        :param etag: Opaque string used to reference the downloaded data is as expected.
            If None, this method will download the latest data from cloud storage.
        :return: Data in bytes.
        """
        pass

    @abstractmethod
    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        """Upload data to cloud storage. Raise KeyCloudSyncError if etag does not match the latest
        version in the cloud.
        :param etag: Expected etag if key already exists. Otherwise None
        :return: Etag of newly uploaded data, as str.
        """
        pass

    @abstractmethod
    def delete_data(self, key: str, etag: str) -> None:
        """Delete data from cloud storage. Raise KeyCloudSyncError if etag does not match the
        latest version in the cloud.
        """
        pass

    @abstractmethod
    def list_keys_and_ids(self, key_prefix: str) -> Dict[str, str]:
        """List all keys within cloud storage, and their associated etags at the time of
        listing.
        :return: Dict mapping keys as str, to their etags as str.
        """
        pass
