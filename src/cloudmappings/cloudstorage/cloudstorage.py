from abc import ABC, abstractmethod
from typing import Dict


class CloudStorage(ABC):
    @abstractmethod
    def create_if_not_exists(self, metadata: Dict[str, str]) -> bool:
        """Create a new parent resource for the data in cloud storage.
        Terminology various between cloud providers:
            * AWS S3: A Bucket
            * Azure Blob Storage: A Container
            * GCP GCS: A Bucket
        :return: True if already exists, otherwise False.
        """
        pass

    @abstractmethod
    def download_data(self, key: str, etag: str) -> bytes:
        """Download data from cloud storage. Raise ValueError if etag does not match the
        latest version in the cloud.
        :return: Data in bytes.
        """
        pass

    @abstractmethod
    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        """Upload data to cloud storage. Raise ValueError if etag does not match the latest
        version in the cloud.
        :param etag: Expected etag if key already exists. Otherwise None
        :return: Etag of newly uploaded data, as str.
        """
        pass

    @abstractmethod
    def delete_data(self, key: str, etag: str) -> None:
        """Delete data from cloud storage. Raise ValueError if etag does not match the
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
