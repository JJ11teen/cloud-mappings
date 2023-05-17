from typing import Any, Dict
from urllib.parse import quote, unquote

from azure.core import MatchConditions
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
)
from azure.data.tables import TableClient, UpdateMode

from cloudmappings.errors import KeySyncError, ValueSizeError
from cloudmappings.storageprovider import StorageProvider


def _chunk_bytes(data: bytes) -> Dict[str, bytes]:
    # Max property size in azure tables is 64KiB
    max_property_size = 64 * 1024
    return {f"d_{k}": data[i : i + max_property_size] for k, i in enumerate(range(0, len(data), max_property_size))}


def _dechunk_entity(entity: Dict[str, bytes]) -> bytes:
    return b"".join([v for k, v in entity.items() if k.startswith("d_")])


class AzureTableStorageProvider(StorageProvider):
    def __init__(
        self,
        table_name: str,
        credential: Any = None,
        endpoint: str = None,
        connection_string: str = None,
    ) -> None:
        if connection_string is not None:
            self._table_client = TableClient.from_connection_string(conn_str=connection_string, table_name=table_name)
        else:
            self._table_client = TableClient(
                endpoint=endpoint,
                table_name=table_name,
                credential=credential,
            )

    def encode_key(self, unsafe_key) -> str:
        return quote(unsafe_key, safe="", errors="strict")

    def decode_key(self, encoded_key) -> str:
        return unquote(encoded_key, errors="strict")

    def logical_name(self) -> str:
        return (
            "CloudStorageProvider=AzureTableStorage,"
            f"StorageAccountName={self._table_client.account_name},"
            f"TableName={self._table_client.table_name}"
        )

    def create_if_not_exists(self):
        try:
            self._table_client.create_table()
        except ResourceExistsError:
            return True
        return False

    def download_data(self, key: str, etag: str) -> bytes:
        try:
            entity = self._table_client.get_entity(
                partition_key=key,
                row_key="cm",
            )
        except ResourceNotFoundError as e:
            if etag is None:
                return None
            raise KeySyncError(storage_provider_name=self.logical_name(), key=key, etag=etag) from e
        else:
            if etag is not None and etag != entity.metadata["etag"]:
                raise KeySyncError(storage_provider_name=self.logical_name(), key=key, etag=etag)
            return _dechunk_entity(entity)

    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        if not isinstance(data, bytes):
            raise ValueError(f"Data must be bytes like, got {type(data)}")
        entity = {
            "PartitionKey": key,
            "RowKey": "cm",
            **_chunk_bytes(data=data),
        }
        try:
            if etag is None:  # Not expecting existing data
                response = self._table_client.create_entity(entity=entity)
            else:
                response = self._table_client.update_entity(
                    entity=entity,
                    mode=UpdateMode.REPLACE,
                    etag=etag,
                    match_condition=MatchConditions.IfNotModified,
                )
        except ResourceExistsError as e:
            raise KeySyncError(storage_provider_name=self.logical_name(), key=key, etag=etag) from e
        except HttpResponseError as e:
            if "update condition specified in the request was not satisfied" in e.exc_msg or (
                "etag value" in e.exc_msg and "is not valid" in e.exc_msg
            ):
                raise KeySyncError(storage_provider_name=self.logical_name(), key=key, etag=etag) from e
            elif (
                e.model is not None
                and e.model.additional_properties is not None
                and "odata.error" in e.model.additional_properties
                and "code" in e.model.additional_properties["odata.error"]
                and e.model.additional_properties["odata.error"]["code"] == "EntityTooLarge"
            ):
                raise ValueSizeError(storage_provider_name=self.logical_name(), key=key) from e
            else:
                raise e
        return response["etag"]

    def delete_data(self, key: str, etag: str) -> None:
        try:
            self._table_client.delete_entity(
                partition_key=key,
                row_key="cm",
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except HttpResponseError as e:
            if "update condition specified in the request was not satisfied" in e.exc_msg or (
                "etag value" in e.exc_msg and "is not valid" in e.exc_msg
            ):
                raise KeySyncError(storage_provider_name=self.logical_name(), key=key, etag=etag) from e
            else:
                raise e

    def list_keys_and_etags(self, key_prefix: str) -> Dict[str, str]:
        if not key_prefix:
            query = self._table_client.list_entities()
        else:
            key_prefix_stop = key_prefix[:-1] + chr(ord(key_prefix[-1]) + 1)
            query = self._table_client.query_entities(
                f"PartitionKey ge '{key_prefix}' and PartitionKey lt '{key_prefix_stop}'"
            )
        return {e["PartitionKey"]: e.metadata["etag"] for e in query}
