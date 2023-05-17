from typing import Any, Optional, TypeVar

from cloudmappings._cloudmappinginternal import CloudMappingInternal
from cloudmappings.cloudmapping import CloudMapping
from cloudmappings.serialisers import CloudMappingSerialisation
from cloudmappings.serialisers.core import pickle
from cloudmappings.storageprovider import StorageProvider

T = TypeVar("T")


class CloudStorage:
    def __init__(self, storage_provider: StorageProvider) -> None:
        self._storage_provider = storage_provider

    @property
    def storage_provider(self) -> StorageProvider:
        return self._storage_provider

    def create_mapping(
        self,
        sync_initially: bool = True,
        read_blindly: bool = False,
        read_blindly_error: bool = False,
        read_blindly_default: Any = None,
        serialisation: CloudMappingSerialisation[T] = pickle(),
        key_prefix: Optional[str] = None,
    ) -> CloudMapping[T]:
        """A cloud-mapping, a `MutableMapping` implementation backed by common cloud storage solutions.

        Parameters
        ----------
        sync_initially : bool, default=True
            Whether to call `sync_with_cloud` initially
        read_blindly : bool, default=False
            Whether the `CloudMapping` will read from the cloud without synchronising.
            When `read_blindly=False`, a `CloudMapping` will raise a `KeyError` unless a key has been
            previously written using the same `CloudMapping` instance, or `.sync_with_cloud` has been
            called and the key was in the cloud. If the value in the cloud has changed since being
            written or synchronised, a `cloudmappings.errors.KeySyncError` will be raised.
            When `read_blindly=True`, a `CloudMapping` will directly query the cloud for any key
            accessed, regardless of if it has previously written a value to that key. It will always get
            the latest value from the cloud, and never raise a `cloudmappings.errors.KeySyncError` for
            read operations. If there is no value for a key in the cloud, and `read_blindly_error=True`,
            a `KeyError` will be raised. If there is no value for a key in the cloud and
            `read_blindly_error=False`, `read_blindly_default` will be returned.
        read_blindly_error : bool, default=False
            Whether to raise a `KeyValue` error when `read_blindly=True` and a key does not have
            a value in the cloud. If `True`, this takes prescedence over `read_blindly_default`.
        read_blindly_default : Any, default=None
            The value to return when `read_blindly=True`, a key does not have a value in the cloud,
            and `read_blindly_error=False`.
        serialiser : CloudMappingSerialiser, default=pickle()
            CloudMappingSerialiser to use, defaults to `pickle`.
        key_prefix : Optional[str], default=None
            Prefix to apply to keys in cloud storage. Enables `CloudMapping`s to map to a subdirectory
            within a cloud storage service, as opposed to the whole resource.
        """
        mapping = CloudMappingInternal()
        mapping._storage_provider = self.storage_provider
        mapping._etags = {}
        mapping._serialisation = serialisation
        mapping._key_prefix = key_prefix

        mapping.read_blindly = read_blindly
        mapping.read_blindly_error = read_blindly_error
        mapping.read_blindly_default = read_blindly_default

        if self.storage_provider.create_if_not_exists() and sync_initially:
            mapping.sync_with_cloud()

        return mapping


class AzureBlobStorage(CloudStorage):
    def __init__(
        self,
        container_name: str,
        credential: Any = None,
        account_url: str = None,
        connection_string: str = None,
        create_container_metadata=None,
    ) -> None:
        """A cloud-mapping backed by an Azure Blob Storage Container

        Note that if you're using hierarchical namespace, directories and blobs cannot share the
        same key.

        Parameters
        ----------
        container_name : str
            The name of the Azure Storage Blob Container to use within the Azure Storage Account
        credential : Any
            A credential object from `azure.identity`
        account_url : str, default=None
            The url of the Azure Storage Account to use
        connection_string : str, default=None
            A connection string to use for the Azure Blob Storage Container. Takes precedence over
            `account_url` and `credential` if given

        See Also
        --------
        cloud-mapping : `CloudMapping`
        """
        from cloudmappings._storageproviders.azureblobstorage import (
            AzureBlobStorageProvider,
        )

        super().__init__(
            AzureBlobStorageProvider(
                container_name=container_name,
                credential=credential,
                account_url=account_url,
                connection_string=connection_string,
                create_container_metadata=create_container_metadata,
            )
        )


class AzureTableStorage(CloudStorage):
    def __init__(
        self,
        table_name: str,
        credential: Any = None,
        endpoint: str = None,
        connection_string: str = None,
    ) -> None:
        """A cloud-mapping backed by an Azure Table Storage Table

        Note that Azure Table Storage has a 1MB size limit per entity. This mapping distributes
        bytes across dummy attributes within an entity to ensure attribute level limits are not
        hit. This results in a 1MB limit for each value stored in this mapping.

        Parameters
        ----------
        table_name : str
            The name of the Azure Storage Table to use within the Azure Storage Account
        credential : Any
            A credential object from `azure.identity`
        endpoint : str, default=None
            The table endpoint of the Azure Storage Account to use
        connection_string : str, default=None
            A connection string to use for the Azure Table Storage Table. Takes precedence over
            `endpoint` and `credential` if given

        See Also
        --------
        cloud-mapping : `CloudMapping`
        """
        from cloudmappings._storageproviders.azuretablestorage import (
            AzureTableStorageProvider,
        )

        super().__init__(
            AzureTableStorageProvider(
                table_name=table_name,
                credential=credential,
                endpoint=endpoint,
                connection_string=connection_string,
            )
        )


class GoogleCloudStorage(CloudStorage):
    def __init__(
        self,
        bucket_name: str,
        project: str,
        credentials: Any = None,
    ) -> None:
        """A cloud-mapping backed by a Google Cloud Storage Bucket

        Parameters
        ----------
        bucket_name : str
            The name of the Storage Bucket to use within Google Cloud Storage
        project : str
            The GCP project to use
        credentials : optional
            A credentials object from various google client libraries

        See Also
        --------
        cloud-mapping : `CloudMapping`
        """
        from cloudmappings._storageproviders.googlecloudstorage import (
            GoogleCloudStorageProvider,
        )

        super().__init__(GoogleCloudStorageProvider(bucket_name=bucket_name, project=project, credentials=credentials))


class AWSS3Storage(CloudStorage):
    def __init__(
        self,
        bucket_name: str,
        silence_warning: bool = False,
    ) -> None:
        """A cloud-mapping backed by an AWS S3 Bucket

        Note that AWS S3 does not support service-side atomic requests, which means there is
        a race condition when multiple clients are uploading data at the same time. Because of
        this, *AWS S3 is not recommended for concurrent use*. Consider using Azure or GCP is
        you need concurrent access. A warning about this will be logged by default when using
        `AWSS3Mapping`. This warning can be silenced with `silence_warning=True`.

        Parameters
        ----------
        bucket_name : str
            The name of the S3 Bucket to use within AWS
        silence_warning : bool, default=False
            Whether to silence the warning logged about using S3 backed cloud-mappings concurrently

        See Also
        --------
        cloud-mapping : `CloudMapping`
        """
        from cloudmappings._storageproviders.awss3storage import AWSS3StorageProvider

        super().__init__(AWSS3StorageProvider(bucket_name=bucket_name, silence_warning=silence_warning))
