from .cloudstoragemapping import CloudMapping


def _parse_cloud_mapping_kwargs(kwargs: dict):
    cloud_mapping_kwargs = dict(
        sync_initially=kwargs.pop("sync_initially", True),
        read_blindly=kwargs.pop("read_blindly", False),
        read_blindly_default=kwargs.pop("read_blindly_default", None),
        ordered_dumps_funcs=kwargs.pop("ordered_dumps_funcs", None),
        ordered_loads_funcs=kwargs.pop("ordered_loads_funcs", None),
    )
    return cloud_mapping_kwargs, kwargs


class AzureBlobMapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
        """A cloud-mapping backed by an Azure Blob Storage Container

        Parameters
        ----------
        container_name : str
            The name of the Azure Storage Blob Container to use within the Azure Storage Account
        account_url : str, default=None
            The url of the Azure Storage Account to use
        credential : default=DefaultAzureCredential()
            A credential object from `azure.identity`
        connection_string : str, default=None
            A connection string to use for the Azure Blob Storage Container. Takes precedence over
            `account_url` and `credential` if given

        See Also
        --------
        cloud-mapping : `CloudMapping`
        """
        from .storageproviders.azureblobstorage import AzureBlobStorageProvider

        cm_kwargs, provider_kwargs = _parse_cloud_mapping_kwargs(kwargs)
        super().__init__(storage_provider=AzureBlobStorageProvider(*args, **provider_kwargs), **cm_kwargs)


class AzureTableMapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
        """A cloud-mapping backed by an Azure Table Storage Table

        Note that Azure Table Storage has a 1MB size limit per entity. This mapping distributes
        bytes across dummy attributes within an entity to ensure attribute level limits are not
        hit. This results in a 1MB limit for each value stored in this mapping.

        Parameters
        ----------
        table_name : str
            The name of the Azure Storage Table to use within the Azure Storage Account
        endpoint : str, default=None
            The table endpoint of the Azure Storage Account to use
        credential : default=DefaultAzureCredential()
            A credential object from `azure.identity`
        connection_string : str, default=None
            A connection string to use for the Azure Table Storage Table. Takes precedence over
            `endpoint` and `credential` if given

        See Also
        --------
        cloud-mapping : `CloudMapping`
        """
        from .storageproviders.azuretablestorage import AzureTableStorageProvider

        cm_kwargs, provider_kwargs = _parse_cloud_mapping_kwargs(kwargs)
        super().__init__(storage_provider=AzureTableStorageProvider(*args, **provider_kwargs), **cm_kwargs)


class GoogleCloudStorageMapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
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
        from .storageproviders.googlecloudstorage import GoogleCloudStorageProvider

        cm_kwargs, provider_kwargs = _parse_cloud_mapping_kwargs(kwargs)
        super().__init__(storage_provider=GoogleCloudStorageProvider(*args, **provider_kwargs), **cm_kwargs)


class AWSS3Mapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
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
        from .storageproviders.awss3 import AWSS3Provider

        cm_kwargs, provider_kwargs = _parse_cloud_mapping_kwargs(kwargs)
        super().__init__(storage_provider=AWSS3Provider(*args, **provider_kwargs), **cm_kwargs)
