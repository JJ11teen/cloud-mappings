from cloudmappings.cloudmapping import CloudMapping
from cloudmappings.cloudstorage import (
    AWSS3Storage,
    AzureBlobStorage,
    AzureTableStorage,
    GoogleCloudStorage,
)

__all__ = [
    "CloudMapping",
    "AWSS3Storage",
    "AzureBlobStorage",
    "AzureTableStorage",
    "GoogleCloudStorage",
]
__version__ = "2.0.0-alpha"
