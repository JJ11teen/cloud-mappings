from abc import ABC, abstractmethod
from typing import Any, Dict, MutableMapping, TypeVar

from cloudmappings.serialisation import CloudMappingSerialisation

T = TypeVar("T")


class CloudMapping(MutableMapping[str, T], ABC):
    """A cloud-mapping, a `MutableMapping` implementation backed by common cloud storage solutions.
    Implements the `MutableMapping` interface, can be used just as a standard `dict()`.
    """

    read_blindly: bool
    """ Whether the cloud-mapping is currently set to read from the cloud blindly.

        When read blindly is `False`, a cloud-mapping will raise a KeyError if a key that it
        doesn't know about is accessed. If a key that it does know about is accessed but then
        found to be out of sync with the cloud, a `cloudmappings.errors.KeySyncError` will be
        raised.

        When read blindly is `True`, a cloud-mapping will return the latest cloud version
        for any key accessed, including keys it has no prior knowledge of (ie not in it's etag
        dict). If there is no value for a key in the cloud, whether a `KeyValue` error is
        raised is controlled by the `read_blindly_error` flag. If `False`, the current value of
        `read_blindly_default` will be returned.

        When read blindly is `True` a cloud-mapping will not raise `cloudmappings.errors.KeySyncError`
        errors for read/get operations.

        By default a cloud-mapping is instantiated with read blindly set to `False`.
    """

    read_blindly_error: bool
    """Whether to raise a `KeyValue` error when read_blindly is `True` and the key does not have
        a value in the cloud. If `True`, this takes prescedence over `read_blindly_default`.
    """

    read_blindly_default: Any
    """The value to return when read_blindly is `True`, the key does not have a value in the cloud,
        and read_blindly_error is `False`.
    """

    @abstractmethod
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
        pass

    @property
    @abstractmethod
    def etags(self) -> Dict[str, str]:
        """An internal dictionary of etags used to ensure the cloud-mapping is in sync with
        the cloud storage resource. The dict is itself a mapping, mapping keys to their etags.

        This dictionary is used as the cloud-mapping's expected view of the cloud. It is used
        to determine if a key exists, and ensure that the value at each key is expected.

        See: https://en.wikipedia.org/wiki/HTTP_ETag
        """
        pass

    @property
    @abstractmethod
    def serialisation(self) -> CloudMappingSerialisation[T]:
        """Gets the serialiser the mapping is configured to use for serialising and
        deserialising values."""
        pass
