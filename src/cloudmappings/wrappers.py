from .cloudstoragemapping import CloudMapping


class AzureBlobMapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
        from .storageproviders.azureblobstorage import AzureBlobStorageProvider

        super().__init__(storage_provider=AzureBlobStorageProvider(*args, **kwargs))


class AzureTableMapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
        from .storageproviders.azuretablestorage import AzureTableStorageProvider

        super().__init__(storage_provider=AzureTableStorageProvider(*args, **kwargs))


class GoogleCloudStorageMapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
        from .storageproviders.googlecloudstorage import GoogleCloudStorageProvider

        super().__init__(storage_provider=GoogleCloudStorageProvider(*args, **kwargs))


class AWSS3Mapping(CloudMapping):
    def __init__(self, *args, **kwargs) -> None:
        from .storageproviders.awss3 import AWSS3Provider

        super().__init__(storage_provider=AWSS3Provider(*args, **kwargs))
