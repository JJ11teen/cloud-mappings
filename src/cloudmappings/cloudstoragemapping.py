from typing import MutableMapping, Dict
from urllib.parse import quote, unquote

from .storageproviders.storageprovider import StorageProvider


def _safe_key(key: str) -> str:
    if not isinstance(key, str):
        raise TypeError("Key must be of type 'str'. Got key:", key)
    return quote(key)


def _unsafe_key(key: str) -> str:
    return unquote(key)


class CloudMapping(MutableMapping):
    etags: Dict[str, str]

    def __init__(
        self,
        storageprovider: StorageProvider,
        sync_initially: bool = True,
    ) -> None:
        self._storageprovider = storageprovider
        self.etags = {}
        if self._storageprovider.create_if_not_exists() and sync_initially:
            self.sync_with_cloud()

    def sync_with_cloud(self, key: str = None) -> None:
        prefix_key = _safe_key(key) if key is not None else None
        self.etags.update({_unsafe_key(k): i for k, i in self._storageprovider.list_keys_and_etags(prefix_key).items()})

    def __getitem__(self, key: str) -> bytes:
        if key not in self.etags:
            raise KeyError(key)
        return self._storageprovider.download_data(key=_safe_key(key), etag=self.etags[key])

    def __setitem__(self, key: str, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise ValueError("Value must be bytes like")
        self.etags[key] = self._storageprovider.upload_data(
            key=_safe_key(key),
            etag=self.etags.get(key, None),
            data=value,
        )

    def __delitem__(self, key: str) -> None:
        if key not in self.etags:
            raise KeyError(key)
        self._storageprovider.delete_data(key=_safe_key(key), etag=self.etags[key])
        del self.etags[key]

    def __contains__(self, key: str) -> bool:
        return key in self.etags

    def keys(self):
        return iter(self.etags.keys())

    __iter__ = keys

    def __len__(self) -> int:
        return len(self.etags)

    def __repr__(self) -> str:
        return f"cloudmapping<{self._storageprovider.safe_name()}>"

    @classmethod
    def with_buffers(cls, io_buffers, *args, **kwargs) -> MutableMapping:
        from zict import Func

        if len(io_buffers) % 2 != 0:
            raise ValueError("Must have an equal number of input buffers as output buffers")

        raw_mapping = cls(*args, **kwargs)
        mapping = raw_mapping

        for dump, load in zip(io_buffers[::2], io_buffers[1::2]):
            mapping = Func(dump, load, mapping)

        mapping.sync_with_cloud = raw_mapping.sync_with_cloud
        return mapping

    @classmethod
    def with_pickle(cls, *args, **kwargs) -> MutableMapping:
        import pickle

        return cls.with_buffers([pickle.dumps, pickle.loads], *args, **kwargs)

    @classmethod
    def with_json(cls, *args, **kwargs) -> MutableMapping:
        import json

        return cls.with_buffers([json.dumps, json.loads], *args, **kwargs)

    @classmethod
    def with_json_zlib(cls, *args, **kwargs) -> MutableMapping:
        import json, zlib

        return cls.with_buffers(
            [json.dumps, json.loads, zlib.compress, zlib.decompress],
            *args,
            **kwargs,
        )
