from typing import Any, Optional, TypeVar

from cloudmappings._cloudmappinginternal import CloudMappingInternal
from cloudmappings.cloudmapping import CloudMapping
from cloudmappings.serialisers import CloudMappingSerialisation
from cloudmappings.serialisers.default import pickle
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
        storage_provider : StorageProvider
            The storage provider to use as the backing for the cloud-mapping.
        sync_initially : bool, default=True
            Whether to call `sync_with_cloud` initially
        read_blindly : bool, default=False
            Whether to read blindly or not by default. See `read_blindly` attribute for more
            information.
        read_blindly_error : bool, default=False
            Whether to raise `KeyError`s when read_blindly is enabled and the key does not have a value
            in the cloud.
        read_blindly_default : Any, default=None
            The value to return when read_blindly is enabled, the key does not have a value in the
            cloud, and read_blindly_error is `False`.
        serialiser : CloudMappingSerialiser
            CloudMappingSerialiser to use, defaults to `pickle`.
        key_prefix : Optional[str], default=None
            Prefix to apply to keys in cloud storage. Enables mappings to be a subdirectory within a
            cloud storage service.
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
        credential: Any,
        account_url: str = None,
        connection_string: str = None,
        create_container_metadata=None,
    ) -> None:
        """A cloud-mapping backed by an Azure Blob Storage Container

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
        credential: Any,
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
        from cloudmappings._storageproviders.awss3 import AWSS3Provider

        super().__init__(AWSS3Provider(bucket_name=bucket_name, silence_warning=silence_warning))
