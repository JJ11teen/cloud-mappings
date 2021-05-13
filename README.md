# cloud-mappings
MutableMapping interfaces for common cloud storage providers

For now only, `AzureBlobMapping` is implemented.

## Installation

with pip:
```
pip install cloud-mappings
```

## Basic Usage

**Step 1**: Instantiate a mapping backed by your chosen cloud storage provider

**Step 2**: Use it just like a standard `dict()`

AzureBlobMapping:
```python
from cloudmappings import AzureBlobMapping

cm = AzureBlobMapping.create_with_pickle(
    account_url="AZURE_BLOB_STORAGE_URL",
    container_name="CONTAINER_NAME",
    credential=AZURE_CREDENTIAL_OBJECT,
)
```

GoogleCloudStorageMapping:
```python
from cloudmappings import GoogleCloudStorageMapping

cm = GoogleCloudStorageMapping.create_with_pickle(
    project="GCP_PROJECT",
    credentials=GCP_CREDENTIALS_OBJECT,
    bucket_name="BUCKET_NAME",
)
```

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)