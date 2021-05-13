from typing import Dict

from .cloudmappings import CloudStorageMapping


class GoogleCloudStorageMapping(CloudStorageMapping):
    def __init__(
        self,
        project: str,
        bucket_name: str,
        credentials=None,
        metadata: Dict[str, str] = None,
    ) -> None:
        from .cloudstorage.googlecloudstorage import GoogleCloudStorage

        azureblobstorage = GoogleCloudStorage(
            project=project,
            credentials=credentials,
            bucket_name=bucket_name,
        )
        super().__init__(azureblobstorage, metadata)
