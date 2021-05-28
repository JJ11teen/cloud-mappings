from typing import Dict
import json

from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError, ResourceModifiedError
from azure.storage.blob import ContainerClient
from azure.identity import DefaultAzureCredential

from .storageprovider import StorageProvider


class AzureBlobStorageProvider(StorageProvider):
    def __init__(
        self,
        account_url: str,
        container_name: str,
        credential=DefaultAzureCredential(),
        create_container_metadata=None,
    ) -> None:
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
        try:
            return self._container_client.download_blob(
                blob=key,
                etag=etag,
                match_condition=MatchConditions.IfNotModified if etag is not None else None,
            ).readall()
        except ResourceModifiedError as e:
            self.raise_key_sync_error(key=key, etag=etag, inner_exception=e)

    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        expecting_blob = etag is not None
        args = {"overwrite": expecting_blob}
        if expecting_blob:
            args["etag"] = etag
            args["match_condition"] = MatchConditions.IfNotModified
        bc = self._container_client.get_blob_client(blob=key)
        try:
            response = bc.upload_blob(
                data=data,
                **args,
            )
        except (ResourceExistsError, ResourceModifiedError) as e:
            self.raise_key_sync_error(key=key, etag=etag, inner_exception=e)
        return json.loads(response["etag"])

    def delete_data(self, key: str, etag: str) -> None:
        try:
            self._container_client.delete_blob(
                blob=key,
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except ResourceModifiedError as e:
            self.raise_key_sync_error(key=key, etag=etag, inner_exception=e)

    def list_keys_and_etags(self, key_prefix: str) -> Dict[str, str]:
        return {b.name: b.etag for b in self._container_client.list_blobs(name_starts_with=key_prefix)}
