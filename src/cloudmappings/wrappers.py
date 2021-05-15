from .cloudstoragemapping import CloudStorageMapping


class AzureBlobMapping(CloudStorageMapping):
    def __init__(self, *args, **kwargs) -> None:
        from .storageproviders.azureblobstorage import AzureBlobStorageProvider

        super().__init__(storageprovider=AzureBlobStorageProvider(*args, **kwargs))


class GoogleCloudStorageMapping(CloudStorageMapping):
    def __init__(self, *args, **kwargs) -> None:
        from .storageproviders.googlecloudstorage import GoogleCloudStorageProvider

        super().__init__(storageprovider=GoogleCloudStorageProvider(*args, **kwargs))


class AWSS3Mapping(CloudStorageMapping):
    def __init__(self, *args, **kwargs) -> None:
        from .storageproviders.awss3 import AWSS3Provider

        super().__init__(storageprovider=AWSS3Provider(*args, **kwargs))
