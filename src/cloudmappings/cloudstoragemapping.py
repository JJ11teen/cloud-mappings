from functools import partial
from typing import MutableMapping, Dict

from .storageproviders.storageprovider import StorageProvider


class CloudMapping(MutableMapping):
    _etags: Dict[str, str]

    def __init__(
        self,
        storage_provider: StorageProvider,
        sync_initially: bool = True,
    ) -> None:
        self._storage_provider = storage_provider
        self._etags = {}
        if self._storage_provider.create_if_not_exists() and sync_initially:
            self.sync_with_cloud()

    def _encode_key(self, unsafe_key: str) -> str:
        if not isinstance(unsafe_key, str):
            raise TypeError("Key must be of type 'str'. Got key:", unsafe_key)
        return self._storage_provider.encode_key(unsafe_key=unsafe_key)

    def sync_with_cloud(self, key: str = None) -> None:
        prefix_key = self._encode_key(key) if key is not None else None
        self._etags.update(
            {
                self._storage_provider.decode_key(k): i
                for k, i in self._storage_provider.list_keys_and_etags(prefix_key).items()
            }
        )

    @property
    def etags(self):
        return self._etags

    def __getitem__(self, key: str) -> bytes:
        if key not in self._etags:
            raise KeyError(key)
        return self._storage_provider.download_data(key=self._encode_key(key), etag=self._etags[key])

    def __setitem__(self, key: str, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise ValueError("Value must be bytes like")
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
