from abc import ABC, abstractmethod
from typing import Any, Dict, MutableMapping, Optional, TypeVar

from cloudmappings.serialisers import CloudMappingSerialisation
from cloudmappings.storageprovider import StorageProvider

T = TypeVar("T")


class CloudMapping(MutableMapping[str, T], ABC):
    """A cloud-mapping, a `MutableMapping` implementation backed by common cloud storage solutions.
    Implements the `MutableMapping` interface, can be used just as a standard `dict()`.
    """

    read_blindly: bool
    """ Whether the `CloudMapping` will read from the cloud without synchronising.

        When `read_blindly=False`, a `CloudMapping` will raise a `KeyError` unless a key has been
        previously written using the same `CloudMapping` instance, or `.sync_with_cloud` has been
        called and the key was in the cloud. If the value in the cloud has changed since being written
        or synchronised, a `cloudmappings.errors.KeySyncError` will be raised.

        When `read_blindly=True`, a `CloudMapping` will directly query the cloud for any key
        accessed, regardless of if it has previously written a value to that key. It will always get
        the latest value from the cloud, and never raise a `cloudmappings.errors.KeySyncError` for
        read operations. If there is no value for a key in the cloud, and `read_blindly_error=True`, a
        `KeyError` will be raised. If there is no value for a key in the cloud and
        `read_blindly_error=False`, `read_blindly_default` will be returned.

        Read blindly only impacts __get__ (`d[key]`) and __contains__ (`in`) operations.

        By default a `CloudMapping` is instantiated with read blindly set to `False`.
    """

    read_blindly_error: bool
    """ Whether to raise a `KeyValue` error when `read_blindly=True` and a key does not have
        a value in the cloud. If `True`, this takes prescedence over `read_blindly_default`.
    """

    read_blindly_default: Any
    """ The value to return when `read_blindly=True`, a key does not have a value in the cloud,
        and `read_blindly_error=False`.
    """

    @abstractmethod
    def sync_with_cloud(self, key_prefix: str = None) -> None:
        """Synchronise this `CloudMapping` with the cloud.

        This allows a `CloudMapping` to reflect the most recent updates to the cloud resource,
        including those made by other instances or users. This can allow destructive operations
        as a user may synchronise to get the latest updates, and then overwrite or delete values.

        Consider calling this if you are encountering a `cloudmappings.errors.KeySyncError`,
        and you are sure you would like to force the operation anyway.

        This is called by default on instantiation of a `CloudMapping`.

        Parameters
        ----------
        key_prefix : str, optional
            Only sync keys beginning with the specified prefix, the key_prefix configured on the
            mapping is prepended in combination with this parameter.
        """
        pass

    @property
    @abstractmethod
    def storage_provider(self) -> StorageProvider:
        """The underlying StorageProvider for this CloudMapping. This can be used to create another
        CloudMapping instance (for example for a different view of the data), or just to directly
        interact with the Cloud resource.
        """

    @property
    @abstractmethod
    def etags(self) -> Dict[str, str]:
        """An internal dictionary of etags used to ensure the `CloudMapping` is in sync with
        the cloud storage resource. The dict maps keys to their last synchronised etags.

        This dictionary is used as the `CloudMapping's expected view of the cloud. It is used
        to determine if a key exists, and ensure that the value of each key is expected.

        See: https://en.wikipedia.org/wiki/HTTP_ETag
        """
        pass

    @property
    @abstractmethod
    def serialisation(self) -> CloudMappingSerialisation[T]:
        """Gets the serialiser configured to use for serialising and deserialising values."""
        pass

    @property
    @abstractmethod
    def key_prefix(self) -> Optional[str]:
        """Gets the key prefix configured to prepend to keys in the cloud. It is also used to
        filter what is synchronised, resulting in the `CloudMapping` mapping to a subset of the
        cloud resource."""
