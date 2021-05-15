from abc import ABC, abstractmethod
from typing import Dict


class KeySyncError(ValueError):
    def __init__(self, cloudprovider: "StorageProvider", key: str, etag: str) -> None:
        super().__init__(
            f"Mapping is out of sync with cloud data.\n"
            f"Cloud storage: '{cloudprovider.safe_name()}'\n"
            f"Key: '{key}', etag: '{etag}'"
        )


class ValueSizeError(ValueError):
    def __init__(self, cloudprovider: "StorageProvider", key: str, size: int) -> None:
        super().__init__(
            f"Value is too big to fit in cloud."
            f"Cloud storage: '{cloudprovider.safe_name()}'\n"
            f"Key: '{key}', size: '{size}'"
        )


class StorageProvider(ABC):
    def raise_key_sync_error(self, key: str, etag: str):
        raise KeySyncError(cloudprovider=self, key=key, etag=etag)

    def raise_value_size_error(self, key: str, size: int):
        raise ValueSizeError(cloudprovider=self, key=key, size=size)

    @abstractmethod
    def safe_name(self) -> str:
        """Returns a human readable string identifying the current implementation, and which logical cloud resouce it is currently mapping to. Does not include any credential information.
        :return: String with identity information
        """
        pass

    @abstractmethod
    def create_if_not_exists(self) -> bool:
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
