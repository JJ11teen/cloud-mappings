from functools import partial
from typing import Callable, Dict, List, MutableMapping

from .storageproviders.storageprovider import StorageProvider


class CloudMapping(MutableMapping):
    """A cloud-mapping, a `MutableMapping` implementation backed by common cloud storage solutions.

    Parameters
    ----------
    storage_provider : StorageProvider
        The storage provider to use as the backing for the cloud-mapping
    sync_initially : bool, default=True
        Whether to call `sync_with_cloud` initially
    read_blindly : bool, default=False
        Whether to read blindly or not by default. See `get_read_blindly` for more information

    Attributes
    ----------
    etags : dict
        A mapping of known keys to their expected etags.

    Methods
    -------
    sync_with_cloud()
        Synchronise the cloud-mapping with what is in the underlying cloud resource
    get_read_blindly()
        Get whether the cloud-mapping is currently set to read from the cloud blindly
    set_read_blindly(read_blindly: bool)
        Set whether the cloud-mapping should read from the cloud blindly or not
    """

    _etags: Dict[str, str]

    def __init__(
        self,
        storage_provider: StorageProvider,
        sync_initially: bool = True,
        read_blindly: bool = False,
    ) -> None:
        """A cloud-mapping, a `MutableMapping` implementation backed by common cloud storage solutions.

        Parameters
        ----------
        storage_provider : StorageProvider
            The storage provider to use as the backing for the cloud-mapping
        sync_initially : bool, default=True
            Whether to call `sync_with_cloud` initially
        read_blindly : bool, default=False
            Whether to read blindly or not by default. See `get_read_blindly` for more information
        """
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

    def get_read_blindly(self) -> bool:
        """Get whether the cloud-mapping is currently set to read keys it doesn't know about
        blindly or not.

        When read blindly is `False`, a cloud-mapping will raise a KeyError if a key that it
        doesn't know about is accessed. If a key that it does know about is accessed but then
        found to be out of sync with the cloud, a `cloudmappings.errors.KeySyncError` will be
        raised.

        When read blindly is `True`, a cloud-mapping will return the latest cloud version
        for any key accessed, including keys it has no prior knowledge of (ie not in it's etag
        dict). If there is no value for a key in the cloud `None` will be returned.

        When read blindly is `True` a cloud-mapping will not raise `KeyValue` or
        `cloudmappings.errors.KeySyncError` errors for read/get operations.

        By default a cloud-mapping is instantiated with read blindly set to `False`.

        Returns
        -------
        bool
            Current read blindly setting
        """
        return self._read_blindly

    def set_read_blindly(self, read_blindly: bool) -> None:
        """Set whether the cloud-mapping should read keys it doesn't know about blindly or
        not.

        When read blindly is `False`, a cloud-mapping will raise a KeyError if a key that it
        doesn't know about is accessed. If a key that it does know about is accessed but then
        found to be out of sync with the cloud, a `cloudmappings.errors.KeySyncError` will be
        raised.

        When read blindly is `True`, a cloud-mapping will return the latest cloud version
        for any key accessed, including keys it has no prior knowledge of (ie not in it's etag
        dict). If there is no value for a key in the cloud `None` will be returned.

        When read blindly is `True` a cloud-mapping will not raise `KeyValue` or
        `cloudmappings.errors.KeySyncError` errors for read/get operations.

        By default a cloud-mapping is instantiated with read blindly set to `False`.

        Parameters
        ----------
        read_blindly : bool
            The value to set read_blindly to
        """
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
    def with_serialisers(
        cls,
        ordered_dumps_funcs: List[Callable],
        ordered_loads_funcs: List[Callable],
        *args,
        **kwargs,
    ) -> "CloudMapping":
        """Create a cloud-mapping instance with serialisation.

        Creates a cloud-mapping which will pass all data input through the specified
        `ordered_dumps_funcs` functions when setting, and inversely runs the bytes from the
        cloud through the specified `ordered_loads_funcs` functions when getting.

        Uses `zict` internally: https://zict.readthedocs.io/en/latest/

        Parameters
        ----------
        ordered_dumps_funcs : List[Callable]
            An ordered list of functions to pass values through before saving bytes to the cloud.
            The last function must return a bytes-like object.
        ordered_loads_funcs : List[Callable]
            An ordered list of functions to pass values through before saving bytes to the cloud.
            The first function must expect a bytes-like object as its input.
        *args : tuple, optional
            Additional positional arguments to pass to the CloudMapping constructor
        **kwargs : dict, optional
            Additional keyword arguments to pass to the CloudMapping constructor

        Raises
        ------
        ValueError
            If the number of `ordered_dumps_funcs` does not match the number of
            `ordered_loads_funcs`

        Returns
        -------
        CloudMapping
            A new cloud-mapping setup with the specified serialisation functions
        """
        from zict import Func

        if len(ordered_dumps_funcs) != len(ordered_loads_funcs):
            raise ValueError("Must have an equal number of dumps functions as loads functions")

        raw_mapping = cls(*args, **kwargs)
        mapping = raw_mapping

        for dump, load in zip(ordered_dumps_funcs[::-1], ordered_loads_funcs):
            mapping = Func(dump, load, mapping)

        mapping.sync_with_cloud = raw_mapping.sync_with_cloud
        mapping.etags = raw_mapping.etags
        mapping.get_read_blindly = raw_mapping.get_read_blindly
        mapping.set_read_blindly = raw_mapping.set_read_blindly
        return mapping

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

        return cls.with_serialisers([pickle.dumps], [pickle.loads], *args, **kwargs)

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

        return cls.with_serialisers(
            [partial(json.dumps, sort_keys=True), partial(bytes, encoding=encoding)],
            [partial(str, encoding=encoding), json.loads],
            *args,
            **kwargs,
        )

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

        return cls.with_serialisers(
            [partial(json.dumps, sort_keys=True), partial(bytes, encoding=encoding), zlib.compress],
            [zlib.decompress, partial(str, encoding=encoding), json.loads],
            *args,
            **kwargs,
        )
