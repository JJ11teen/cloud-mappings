# cloud-mappings
MutableMapping interfaces for common cloud storage providers

For now only, `AzureBlobMapping` is implemented.

## Installation

with pip:
```
pip install cloud-mappings
```

## Basic Usage

AzureBlobMapping:
```python
from cloudmappings import AzureBlobMapping

mutable_mapping = AzureBlobMapping(
    account_url="AZURE_BLOB_STORAGE_URL",
    container_name="CONTAINER_NAME",
    credential=AZURE_CREDENTIAL_OBJECT,
)
```


[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)