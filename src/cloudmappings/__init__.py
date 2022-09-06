from .wrappers import (
    AWSS3Mapping,
    AzureBlobMapping,
    AzureTableMapping,
    GoogleCloudStorageMapping,
)

__all__ = [
    "AzureBlobMapping",
    "AzureTableMapping",
    "GoogleCloudStorageMapping",
    "AWSS3Mapping",
]
__version__ = "1.2.3"
