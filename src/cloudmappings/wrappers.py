from .cloudstoragemapping import CloudMapping


def _parse_cloud_mapping_kwargs(kwargs):
    cloud_mapping_kwargs = {}

    try:
        cloud_mapping_kwargs["sync_initially"] = kwargs.pop("sync_initially")
    except KeyError:
        pass
    try:
        cloud_mapping_kwargs["read_blindly"] = kwargs.pop("read_blindly")
    except KeyError:
        pass

    return cloud_mapping_kwargs, kwargs


class AzureBlobMapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
        from .storageproviders.azureblobstorage import AzureBlobStorageProvider

        cm_kwargs, provider_kwargs = _parse_cloud_mapping_kwargs(kwargs)
        super().__init__(storage_provider=AzureBlobStorageProvider(*args, **provider_kwargs), **cm_kwargs)


class AzureTableMapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
        from .storageproviders.azuretablestorage import AzureTableStorageProvider

        cm_kwargs, provider_kwargs = _parse_cloud_mapping_kwargs(kwargs)
        super().__init__(storage_provider=AzureTableStorageProvider(*args, **provider_kwargs), **cm_kwargs)


class GoogleCloudStorageMapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
        from .storageproviders.googlecloudstorage import GoogleCloudStorageProvider

        cm_kwargs, provider_kwargs = _parse_cloud_mapping_kwargs(kwargs)
        super().__init__(storage_provider=GoogleCloudStorageProvider(*args, **provider_kwargs), **cm_kwargs)


class AWSS3Mapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
        from .storageproviders.awss3 import AWSS3Provider

        cm_kwargs, provider_kwargs = _parse_cloud_mapping_kwargs(kwargs)
        super().__init__(storage_provider=AWSS3Provider(*args, **provider_kwargs), **cm_kwargs)
