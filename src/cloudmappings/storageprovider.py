from abc import ABC, abstractmethod
from typing import Dict
from urllib.parse import quote, unquote


class StorageProvider(ABC):
    """Provides a consistent interface for interacting with Cloud Storage Providers."""

    @abstractmethod
    def logical_name(self) -> str:
        """Returns a human readable string identifying the current implementation, and which
        logical cloud resouce it is currently mapping to. Does not include any credential or
        secret information.

        Returns
        -------
        str
            Identity information string
        """
        pass

    @abstractmethod
    def create_if_not_exists(self) -> bool:
        """Create a new parent resource for the data in cloud storage.

        Terminology various between cloud providers:
            * Azure Blob Storage: A Blob Container
            * GCP GCS: A GCS Bucket
            * AWS S3: An S3 Bucket

        Returns
        -------
        bool
            `True` if the parent cloud resouce already existed, otherwise `False`.
        """
        pass

    def encode_key(self, unsafe_key) -> str:
        """Encode a possibly unsafe input string input a safe value to use as a key in the
        storage service. Defaults to `urllib.parse.quote`

        Parameters
        ----------
        unsafe_key: str
            The unsafe key to encode

        Returns
        -------
        str
            Safely encoded key
        """
        return quote(unsafe_key, errors="strict")

    def decode_key(self, encoded_key) -> str:
        """Decodes a previously encoded key back to it's original value.
        Defaults to `urllib.parse.unquote`

        Parameters
        ----------
        encoded_key: str
            The encoded key to decode

        Returns
        -------
        str
            Decoded key
        """
        return unquote(encoded_key, errors="strict")

    @abstractmethod
    def download_data(self, key: str, etag: str) -> bytes:
        """Download data from cloud storage

        Downloads the data at the specified key and with the specified etag from cloud storage.

        If `None` is passed for etag the latest version in the cloud will be downloaded.
        Otherwise the etag will be used to ensure the data downloaded has a matching etag. If
        the etag does not match the latest cloud version, a `cloudmappings.errors.KeySyncError`
        will be raised.

        Parameters
        ----------
        key : str
            The encoded key specifying which data to download
        etag : str or None
            Etag of the expected latest value in the cloud, or `None`

        Raises
        ------
        KeySyncError
            If an etag is specified and does not match the latest version in the cloud.

        Returns
        -------
        bytes
            The data from the cloud
        """
        pass

    @abstractmethod
    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        """Upload data to cloud storage

        Uploads data at the specified key to cloud storage, only overwriting if the etag matches

        Parameters
        ----------
        key : str
            The encoded key specifying which data to download
        etag : str or None
            Etag of the expected value in the cloud, `None` if it is expected that there is no
            existing data in the cloud
        data : bytes
            The data to upload to the cloud, in bytes

        Raises
        ------
        KeySyncError
            When the etag specified does not match the value in the cloud
        ValueError
            When data is not bytes-like
        ValueSizeError
            When data is too large for storage provider

        Returns
        -------
        str
            Etag of the newly uploaded data
        """
        pass

    @abstractmethod
    def delete_data(self, key: str, etag: str) -> None:
        """Delete data from cloud storage.

        Deletes data at the specified key from cloud storage, only if the etag matches

        Parameters
        ----------
        key : str
            The encoded key specifying which data to delete
        etag : str or None
            Etag of the expected value in the cloud

        Raises
        ------
        KeySyncError
            When the etag specified does not match the value in the cloud
        """
        pass

    @abstractmethod
    def list_keys_and_etags(self, key_prefix: str) -> Dict[str, str]:
        """List keys and etags from the cloud storage.

        Queries all keys and their associated etags from the cloud. Returns a dictionary mapping
        each key to its latest etag. If a `key_prefix` is specified only the keys beginning with
        this prefix will be queried and returned.

        Parameters
        ----------
        key_prefix : str, optional
            An encoded prefix specifying a subset of keys to query. If not given, all keys will
            be queried

        Returns
        -------
        Dict[str, str]
            A dictionary mapping each key in the cloud to it's latest etag
        """
        pass
