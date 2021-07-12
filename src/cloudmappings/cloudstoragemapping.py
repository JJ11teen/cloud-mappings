from functools import partial
from typing import MutableMapping, Dict

from .storageproviders.storageprovider import StorageProvider


class CloudMapping(MutableMapping):
    _etags: Dict[str, str]

    def __init__(
        self,
        storage_provider: StorageProvider,
        sync_initially: bool = True,
        read_blindly: bool = False,
    ) -> None:
        self._storage_provider = storage_provider
        self._etags = {}
        self._read_blindly = read_blindly
        if self._storage_provider.create_if_not_exists() and sync_initially:
            self.sync_with_cloud()

    def _encode_key(self, unsafe_key: str) -> str:
        if not isinstance(unsafe_key, str):
            raise TypeError("Key must be of type 'str'. Got key:", unsafe_key)
        return self._storage_provider.encode_key(unsafe_key=unsafe_key)

    def sync_with_cloud(self, key_prefix: str = None) -> None:
        key_prefix = None if key_prefix is None else self._encode_key(key_prefix)
        self._etags.update(
            {
                self._storage_provider.decode_key(k): i
                for k, i in self._storage_provider.list_keys_and_etags(key_prefix).items()
            }
        )

    @property
    def etags(self):
        return self._etags

    def get_read_blindly(self) -> bool:
        return self._read_blindly

    def set_read_blindly(self, read_blindly: bool):
        self._read_blindly = read_blindly

    def __getitem__(self, key: str) -> bytes:
        if not self._read_blindly and key not in self._etags:
            raise KeyError(key)
        return self._storage_provider.download_data(
            key=self._encode_key(key), etag=None if self._read_blindly else self._etags[key]
        )

    def __setitem__(self, key: str, value: bytes) -> None:
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
        return key in self._etags

    def keys(self):
        return iter(self._etags.keys())

    __iter__ = keys

    def __len__(self) -> int:
        return len(self._etags)

    def __repr__(self) -> str:
        return f"cloudmapping<{self._storage_provider.logical_name()}>"

    @classmethod
    def with_buffers(cls, input_buffers, output_buffers, *args, **kwargs) -> "CloudMapping":
        from zict import Func

        if len(input_buffers) != len(output_buffers):
            raise ValueError("Must have an equal number of input buffers as output buffers")

        raw_mapping = cls(*args, **kwargs)
        mapping = raw_mapping

        for dump, load in zip(input_buffers[::-1], output_buffers):
            mapping = Func(dump, load, mapping)

        mapping.sync_with_cloud = raw_mapping.sync_with_cloud
        mapping.etags = raw_mapping.etags
        mapping.get_read_blindly = raw_mapping.get_read_blindly
        mapping.set_read_blindly = raw_mapping.set_read_blindly
        return mapping

    @classmethod
    def with_pickle(cls, *args, **kwargs) -> "CloudMapping":
        import pickle

        return cls.with_buffers([pickle.dumps], [pickle.loads], *args, **kwargs)

    @classmethod
    def with_json(cls, encoding="utf-8", *args, **kwargs) -> "CloudMapping":
        import json

        return cls.with_buffers(
            [json.dumps, partial(bytes, encoding=encoding)],
            [partial(str, encoding=encoding), json.loads],
            *args,
            **kwargs,
        )

    @classmethod
    def with_json_zlib(cls, encoding="utf-8", *args, **kwargs) -> "CloudMapping":
        import json, zlib

        return cls.with_buffers(
            [json.dumps, partial(bytes, encoding=encoding), zlib.compress],
            [zlib.decompress, partial(str, encoding=encoding), json.loads],
            *args,
            **kwargs,
        )
