from cloudmappings.cloudmapping import CloudMapping

from cloudmappings.wrappers import (
    AWSS3Mapping,
    AzureBlobMapping,
    AzureTableMapping,
    GoogleCloudStorageMapping,
)

__all__ = [
    "CloudMapping",
    "AzureBlobMapping",
    "AzureTableMapping",
    "GoogleCloudStorageMapping",
    "AWSS3Mapping",
]
__version__ = "1.2.3"
