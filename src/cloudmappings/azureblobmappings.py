from typing import Dict

from .cloudmappings import CloudStorageMapping


class AzureBlobMapping(CloudStorageMapping):
    def __init__(
        self,
        account_url: str,
        container_name: str,
        credential=None,
        metadata: Dict[str, str] = None,
    ) -> None:
        from .storageproviders.azureblobstorage import AzureBlobStorageProvider

        azureblobstorage = AzureBlobStorageProvider(
            account_url=account_url,
            container_name=container_name,
            credential=credential,
        )
        super().__init__(cloudstorage=azureblobstorage, metadata=metadata)
