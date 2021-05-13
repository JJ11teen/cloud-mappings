from typing import Dict

from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import ContainerClient

from .cloudstorage import CloudStorage


class AzureBlobStorage(CloudStorage):
    def __init__(
        self,
        account_url: str,
        container_name: str,
        credential,
    ) -> None:
        self._container_client = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=credential,
        )

    def create_if_not_exists(self, metadata: Dict[str, str]):
        try:
            self._container_client.create_container(metadata=metadata)
        except ResourceExistsError:
            return True
        return False

    def download_data(self, key: str, etag: str) -> bytes:
        return self._container_client.download_blob(
            blob=key,
            etag=etag,
            match_condition=MatchConditions.IfNotModified if etag is not None else None,
        ).readall()

    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        expecting_blob = etag is not None
        args = {"overwrite": expecting_blob}
        if expecting_blob:
            args["etag"] = etag
            args["match_condition"] = MatchConditions.IfNotModified
        bc = self._container_client.get_blob_client(blob=key)
        response = bc.upload_blob(
            data=data,
            **args,
        )
        return response["etag"]

    def delete_data(self, key: str, etag: str) -> None:
        self._container_client.delete_blob(
            blob=key,
            etag=etag,
            match_condition=MatchConditions.IfNotModified,
        )

    def list_keys_and_ids(self, key_prefix: str) -> Dict[str, str]:
        return {b.name: b.etag for b in self._container_client.list_blobs(name_starts_with=key_prefix)}
