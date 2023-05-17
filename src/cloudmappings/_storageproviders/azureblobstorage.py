import json
from typing import Any, Dict

from azure.core import MatchConditions
from azure.core.exceptions import (
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)
from azure.storage.blob import ContainerClient

from cloudmappings.errors import KeySyncError
from cloudmappings.storageprovider import StorageProvider


class AzureBlobStorageProvider(StorageProvider):
    def __init__(
        self,
        container_name: str,
        credential: Any = None,
        account_url: str = None,
        connection_string: str = None,
        create_container_metadata=None,
    ) -> None:
        if connection_string:
            self._container_client = ContainerClient.from_connection_string(
                conn_str=connection_string, container_name=container_name
            )
        else:
            self._container_client = ContainerClient(
                account_url=account_url,
                container_name=container_name,
                credential=credential,
            )
        self._create_container_metadata = create_container_metadata

    def logical_name(self) -> str:
        return (
            "CloudStorageProvider=AzureBlobStorage,"
            f"StorageAccountName={self._container_client.account_name},"
            f"ContainerName={self._container_client.container_name}"
        )

    def create_if_not_exists(self):
        try:
            self._container_client.create_container(metadata=self._create_container_metadata)
        except ResourceExistsError:
            return True
        return False

    def download_data(self, key: str, etag: str) -> bytes:
        args = dict(blob=key)
        if etag is not None:
            args.update(
                dict(
                    etag=etag,
                    match_condition=MatchConditions.IfNotModified,
                )
            )
        try:
            return self._container_client.download_blob(**args).readall()
        except ResourceModifiedError as e:
            raise KeySyncError(storage_provider_name=self.logical_name(), key=key, etag=etag) from e
        except ResourceNotFoundError as e:
            if etag is None:
                return None
            raise KeySyncError(storage_provider_name=self.logical_name(), key=key, etag=etag) from e

    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        if not isinstance(data, bytes):
            raise ValueError(f"Data must be bytes like, got {type(data)}")
        expecting_blob = etag is not None
        args = dict(overwrite=expecting_blob)
        if expecting_blob:
            args.update(
                dict(
                    etag=etag,
                    match_condition=MatchConditions.IfNotModified,
                )
            )
        bc = self._container_client.get_blob_client(blob=key)
        try:
            response = bc.upload_blob(
                data=data,
                **args,
            )
        except (ResourceExistsError, ResourceModifiedError) as e:
            raise KeySyncError(storage_provider_name=self.logical_name(), key=key, etag=etag) from e
        return json.loads(response["etag"])

    def delete_data(self, key: str, etag: str) -> None:
        try:
            self._container_client.delete_blob(
                blob=key,
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except ResourceModifiedError as e:
            raise KeySyncError(storage_provider_name=self.logical_name(), key=key, etag=etag) from e

    def list_keys_and_etags(self, key_prefix: str) -> Dict[str, str]:
        # If the container has hierarchical namespaces enabled, this call
        # will return files as well as subdirectories.
        # Unforunately there is no serverside api to filter, so we
        # rely on checking the content_type & content_md5 hash
        # If both are None, we assume the listing is a dir skip it
        return {
            b.name: b.etag
            for b in self._container_client.list_blobs(name_starts_with=key_prefix)
            if b.content_settings.content_type is not None or b.content_settings.content_md5 is not None
        }
