from typing import Dict

from .cloudstoragemapping import CloudStorageMapping


class AzureBlobMapping(CloudStorageMapping):
    def __init__(
        self,
        account_url: str,
        container_name: str,
        credential=None,
        metadata: Dict[str, str] = None,
    ) -> None:
        from .cloudstorage.azureblobstorage import AzureBlobStorage

        azureblobstorage = AzureBlobStorage(
            account_url=account_url,
            container_name=container_name,
            credential=credential,
        )
        super().__init__(azureblobstorage, metadata)
