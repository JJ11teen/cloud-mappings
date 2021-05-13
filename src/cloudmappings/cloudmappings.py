from typing import MutableMapping, Dict
from urllib.parse import quote, unquote

from .cloudstorage.cloudstorage import CloudStorage


def _safe_key(key: str) -> str:
    if not isinstance(key, str):
        raise TypeError("Key must be of type 'str'. Got key:", key)
    return quote(key)


def _unsafe_key(key: str) -> str:
    return unquote(key)


class CloudStorageMapping(MutableMapping):
    etags: Dict[str, str]

    def __init__(
        self,
        cloudstorage: CloudStorage,
        metadata: Dict[str, str],
    ) -> None:
        self._cloudstorage = cloudstorage
        self.etags = {}
        if self._cloudstorage.create_if_not_exists(metadata=metadata):
            self.sync_with_cloud()

    def sync_with_cloud(self, key: str = None) -> None:
        prefix_key = _safe_key(key) if key is not None else None
        self.etags.update({_unsafe_key(k): i for k, i in self._cloudstorage.list_keys_and_ids(prefix_key).items()})

    def __getitem__(self, key: str) -> bytes:
        if key not in self.etags:
            raise KeyError(key)
        return self._cloudstorage.download_data(key=_safe_key(key), etag=self.etags[key])

    def __setitem__(self, key: str, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise ValueError("Value must be bytes like")
        self.etags[key] = self._cloudstorage.upload_data(
            key=_safe_key(key),
            etag=self.etags.get(key, None),
            data=value,
        )

    def __delitem__(self, key: str) -> None:
        if key not in self.etags:
            raise KeyError(key)
        self._cloudstorage.delete_data(key=_safe_key(key), etag=self.etags[key])
        del self.etags[key]

    def __contains__(self, key: str) -> bool:
        return key in self.etags

    def keys(self):
        return iter(self.etags.keys())

    __iter__ = keys

    def __len__(self) -> int:
        return len(self.etags)

    @classmethod
    def create_with_buffers(cls, *io_buffers, **kwargs) -> MutableMapping:
        from zict import Func

        if len(io_buffers) % 2 != 0:
            raise ValueError("Must have an equal number of input buffers as output buffers")

        raw_mapping = cls(**kwargs)
        mapping = raw_mapping

        for dump, load in zip(io_buffers[::2], io_buffers[1::2]):
            mapping = Func(dump, load, mapping)

        mapping.sync_with_cloud = raw_mapping.sync_with_cloud
        return mapping

    @classmethod
    def create_with_pickle(
        cls,
        **kwargs,
    ) -> MutableMapping:
        import pickle

        return cls.create_with_buffers(pickle.dumps, pickle.loads, **kwargs)

    @classmethod
    def create_with_zlib_pickle(
        cls,
        **kwargs,
    ) -> MutableMapping:
        import zlib, pickle

        return cls.create_with_buffers(zlib.compress, zlib.decompress, pickle.dumps, pickle.loads, **kwargs)
