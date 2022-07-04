from functools import partial
from typing import Any, Callable, Dict, List, MutableMapping

from .storageproviders.storageprovider import StorageProvider


class CloudMapping(MutableMapping):
    """A cloud-mapping, a `MutableMapping` implementation backed by common cloud storage solutions.

    Parameters
    ----------
    storage_provider : StorageProvider
        The storage provider to use as the backing for the cloud-mapping.
    sync_initially : bool, default=True
        Whether to call `sync_with_cloud` initially
    read_blindly : bool, default=False
        Whether to read blindly or not by default. See `read_blindly` attribute for more
        information.
    read_blindly_default : Any, default=None
        The value to return when read_blindly is enabled and the key does not have
        a value in the cloud.
    ordered_dumps_funcs : List[Callable]
        An ordered list of functions to pass values through before saving bytes to the cloud.
        The last function must return a bytes-like object.
    ordered_loads_funcs : List[Callable]
        An ordered list of functions to pass values through before saving bytes to the cloud.
        The first function must expect a bytes-like object as its input.
    """

    read_blindly: bool
    """ Whether the cloud-mapping is currently set to read from the cloud blindly.

        When read blindly is `False`, a cloud-mapping will raise a KeyError if a key that it
        doesn't know about is accessed. If a key that it does know about is accessed but then
        found to be out of sync with the cloud, a `cloudmappings.errors.KeySyncError` will be
        raised.

        When read blindly is `True`, a cloud-mapping will return the latest cloud version
        for any key accessed, including keys it has no prior knowledge of (ie not in it's etag
        dict). If there is no value for a key in the cloud, the current value of
        `read_blindly_default` will be returned.

        When read blindly is `True` a cloud-mapping will not raise `KeyValue` or
        `cloudmappings.errors.KeySyncError` errors for read/get operations.

        By default a cloud-mapping is instantiated with read blindly set to `False`.
    """

    read_blindly_default: Any
    """The value to return when read_blindly is `True` and the key does not have
        a value in the cloud.
    """

    def __init__(
        self,
        storage_provider: StorageProvider,
        sync_initially: bool = True,
        read_blindly: bool = False,
        read_blindly_default: Any = None,
        ordered_dumps_funcs: List[Callable] = None,
        ordered_loads_funcs: List[Callable] = None,
    ) -> None:
        """A cloud-mapping, a `MutableMapping` implementation backed by common cloud storage solutions.

        Parameters
        ----------
        storage_provider : StorageProvider
            The storage provider to use as the backing for the cloud-mapping.
        sync_initially : bool, default=True
            Whether to call `sync_with_cloud` initially
        read_blindly : bool, default=False
            Whether to read blindly or not by default. See `read_blindly` attribute for more
            information
        read_blindly_default : Any, default=None
            The value to return when read_blindly is enabled and the key does not have
            a value in the cloud
        ordered_dumps_funcs : List[Callable], default=None
            An ordered list of functions to pass values through before saving bytes to the cloud.
            The last function must return a bytes-like object.
        ordered_loads_funcs : List[Callable], default=None
            An ordered list of functions to pass values through before saving bytes to the cloud.
            The first function must expect a bytes-like object as its input.
        """
        self._storage_provider = storage_provider
        self._etags = {}
        self._ordered_dumps_funcs = ordered_dumps_funcs if ordered_dumps_funcs is not None else []
        self._ordered_loads_funcs = ordered_loads_funcs if ordered_loads_funcs is not None else []

        self.read_blindly = read_blindly
        self.read_blindly_default = read_blindly_default

        if self._storage_provider.create_if_not_exists() and sync_initially:
            self.sync_with_cloud()

    def _encode_key(self, unsafe_key: str) -> str:
        if not isinstance(unsafe_key, str):
            raise TypeError("Key must be of type 'str'. Got key:", unsafe_key)
        return self._storage_provider.encode_key(unsafe_key=unsafe_key)

    def sync_with_cloud(self, key_prefix: str = None) -> None:
        """Synchronise this cloud-mapping's etags with the cloud.

        This allows a cloud-mapping to reflect the most recent updates to the cloud resource,
        including those made by other instances or users. This can allow destructive operations
        as a user may sync to get the latest updates, and then overwrite or delete keys.

        Consider calling this if you are encountering a `cloudmappings.errors.KeySyncError`,
        and you are sure you would like to force the operation anyway.

        This is called by default on instantiation of a cloud-mapping.

        Parameters
        ----------
        key_prefix : str, optional
            Only sync keys beginning with the specified prefix
        """
        key_prefix = None if key_prefix is None else self._encode_key(key_prefix)
        self._etags.update(
            {
                self._storage_provider.decode_key(k): i
                for k, i in self._storage_provider.list_keys_and_etags(key_prefix).items()
            }
        )

    @property
    def etags(self) -> Dict:
        """An internal dictionary of etags used to ensure the cloud-mapping is in sync with
        the cloud storage resource. The dict is itself a mapping, mapping keys to their etags.

        This dictionary is used as the cloud-mapping's expected view of the cloud. It is used
        to determine if a key exists, and ensure that the value at each key is expected.

        See: https://en.wikipedia.org/wiki/HTTP_ETag
        """
        return self._etags

    def __getitem__(self, key: str) -> Any:
        if not self.read_blindly and key not in self._etags:
            raise KeyError(key)
        value = self._storage_provider.download_data(
            key=self._encode_key(key), etag=None if self.read_blindly else self._etags[key]
        )
        if self.read_blindly and value is None:
            return self.read_blindly_default
        for loads in self._ordered_loads_funcs:
            value = loads(value)
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        for dumps in self._ordered_dumps_funcs:
            value = dumps(value)
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
    def with_pickle(cls, *args, **kwargs) -> "CloudMapping":
        """Create a cloud-mapping instance that pickles values using pythons `pickle`

        Parameters
        ----------
        *args : tuple, optional
            Additional positional arguments to pass to the CloudMapping constructor
        **kwargs : dict, optional
            Additional keyword arguments to pass to the CloudMapping constructor

        Returns
        -------
        CloudMapping
            A new cloud-mapping setup with pickle serialisation
        """
        import pickle

        kwargs.update(
            dict(
                ordered_dumps_funcs=[pickle.dumps],
                ordered_loads_funcs=[pickle.loads],
            )
        )
        return cls(*args, **kwargs)

    @classmethod
    def with_json(cls, encoding="utf-8", *args, **kwargs) -> "CloudMapping":
        """Create a cloud-mapping instance that serialises values to JSON strings

        Parameters
        ----------
        encoding : str, default="utf-8"
            The string encoding to use, passed to bytes() and str() for dumps and loads respectively.
        *args : tuple, optional
            Additional positional arguments to pass to the CloudMapping constructor
        **kwargs : dict, optional
            Additional keyword arguments to pass to the CloudMapping constructor

        Returns
        -------
        CloudMapping
            A new cloud-mapping setup with JSON string serialisation
        """
        import json

        kwargs.update(
            dict(
                ordered_dumps_funcs=[partial(json.dumps, sort_keys=True), partial(bytes, encoding=encoding)],
                ordered_loads_funcs=[partial(str, encoding=encoding), json.loads],
            )
        )
        return cls(*args, **kwargs)

    @classmethod
    def with_json_zlib(cls, encoding="utf-8", *args, **kwargs) -> "CloudMapping":
        """Create a cloud-mapping instance that serialises values to compressed JSON strings

        Uses zlib to compress values after serialising them JSON strings.

        Parameters
        ----------
        encoding : str, default="utf-8"
            The string encoding to use, passed to bytes() and str() for dumps and loads
            respectively.
        *args : tuple, optional
            Additional positional arguments to pass to the CloudMapping constructor
        **kwargs : dict, optional
            Additional keyword arguments to pass to the CloudMapping constructor

        Returns
        -------
        CloudMapping
            A new cloud-mapping setup with zlib compression and JSON string serialisation
        """
        import json
        import zlib

        kwargs.update(
            dict(
                ordered_dumps_funcs=[
                    partial(json.dumps, sort_keys=True),
                    partial(bytes, encoding=encoding),
                    zlib.compress,
                ],
                ordered_loads_funcs=[
                    zlib.decompress,
                    partial(str, encoding=encoding),
                    json.loads,
                ],
            )
        )
        return cls(*args, **kwargs)
