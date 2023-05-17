from typing import Dict, Iterator, Optional, TypeVar

from cloudmappings.cloudmapping import CloudMapping
from cloudmappings.serialisers import CloudMappingSerialisation
from cloudmappings.storageprovider import StorageProvider

T = TypeVar("T")


class CloudMappingInternal(CloudMapping[T]):
    _storage_provider: StorageProvider
    _etags: Dict[str, str]
    _serialisation: CloudMappingSerialisation[T]
    _key_prefix: Optional[str]

    def _encode_key(self, mapping_key: str) -> str:
        if not isinstance(mapping_key, str):
            raise TypeError(f"Key must be of type 'str'. Got key of type: {type(mapping_key)}")
        with_prefix = self._key_prefix + mapping_key if self._key_prefix else mapping_key
        return self._storage_provider.encode_key(unsafe_key=with_prefix)

    def _decode_key(self, key_from_provider: str) -> str:
        decoded = self._storage_provider.decode_key(key_from_provider)
        if self._key_prefix and decoded.startswith(self._key_prefix):
            decoded = decoded[len(self._key_prefix) :]
        return decoded

    def sync_with_cloud(self, key_prefix: str = "") -> None:
        key_prefix = self._encode_key(key_prefix)
        self._etags.update(
            {self._decode_key(k): i for k, i in self._storage_provider.list_keys_and_etags(key_prefix).items()}
        )

    @property
    def etags(self) -> Dict[str, str]:
        return self._etags

    @property
    def serialisation(self) -> CloudMappingSerialisation[T]:
        return self._serialisation

    @property
    def key_prefix(self) -> str:
        return self._key_prefix

    def __getitem__(self, key: str) -> T:
        if not self.read_blindly and key not in self._etags:
            raise KeyError(key)
        value = self._storage_provider.download_data(
            key=self._encode_key(key), etag=None if self.read_blindly else self._etags[key]
        )
        if self.read_blindly and value is None:
            if self.read_blindly_error:
                raise KeyError(key)
            return self.read_blindly_default
        if self._serialisation:
            value = self._serialisation.loads(value)
        return value

    def __setitem__(self, key: str, value: T) -> None:
        if self._serialisation:
            value = self._serialisation.dumps(value)
        self._etags[key] = self._storage_provider.upload_data(
            key=self._encode_key(key),
            etag=self._etags.get(key, None),
            data=value,
        )

    def __delitem__(self, key: str) -> None:
        if key not in self._etags:
            raise KeyError(key)
        self._storage_provider.delete_data(key=self._encode_key(key), etag=self._etags[key])
        del self._etags[key]

    def __contains__(self, key: str) -> bool:
        if not self.read_blindly:
            return key in self._etags
        encoded_key = self._encode_key(key)
        return encoded_key in self._storage_provider.list_keys_and_etags(encoded_key)

    def keys(self) -> Iterator[str]:
        return iter(self._etags.keys())

    __iter__ = keys

    def __len__(self) -> int:
        return len(self._etags)

    def __repr__(self) -> str:
        return f"cloudmapping<{self._storage_provider.logical_name()}>"
