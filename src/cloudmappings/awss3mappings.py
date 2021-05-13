from typing import Dict

from .cloudmappings import CloudStorageMapping


class AWSS3Mapping(CloudStorageMapping):
    def __init__(
        self,
        bucket_name: str,
        metadata: Dict[str, str] = None,
    ) -> None:
        from .cloudstorage.awss3 import AWSS3

        aws_s3 = AWSS3(
            bucket_name=bucket_name,
        )
        super().__init__(cloudstorage=aws_s3, metadata=metadata)
