# cloud-mappings
MutableMapping implementations for common cloud storage providers

![Build](https://github.com/JJ11teen/cloud-mappings/actions/workflows/build.yaml/badge.svg)

For now `AzureBlobMapping` and `GoogleCloudStorageMapping` are implemented. AWS S3 is next on the list.

## Installation

with pip:
```
pip install cloud-mappings
```

## Basic Usage

**Step 1**: Instantiate a mapping backed by your chosen cloud storage provider

**Step 2**: Use it just like a standard `dict()`

### AzureBlobMapping:
```python
from cloudmappings import AzureBlobMapping

cm = AzureBlobMapping.with_pickle(
    account_url="AZURE_BLOB_STORAGE_URL",
    container_name="CONTAINER_NAME",
    credential=AZURE_CREDENTIAL_OBJECT,
)
```

### GoogleCloudStorageMapping:
```python
from cloudmappings import GoogleCloudStorageMapping

cm = GoogleCloudStorageMapping.with_pickle(
    project="GCP_PROJECT",
    credentials=GCP_CREDENTIALS_OBJECT,
    bucket_name="BUCKET_NAME",
)
```

### AWSS3Mapping:
```python
from cloudmappings import AWSS3Mapping

cm = AWSS3Mapping.with_pickle(
    bucket_name="AWS_BUCKET_NAME",
    silence_warning=False,
)
```
Note that AWS S3 does not support server atomic requests, so it is not recommended for concurrent use. A warning is printed out by default but may be silenced by passing `silence_warning=True`.



[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)