# cloud-mappings
MutableMapping implementations for common cloud storage providers

[![Build](https://github.com/JJ11teen/cloud-mappings/actions/workflows/build.yaml/badge.svg)](https://github.com/JJ11teen/cloud-mappings/actions/workflows/build.yaml)
[![PyPI version](https://badge.fury.io/py/cloud-mappings.svg)](https://pypi.org/project/cloud-mappings/)

For now [Azure Blob Storage](https://azure.microsoft.com/en-au/services/storage/blobs), [Google Cloud Storage](https://cloud.google.com/storage/), and [AWS S3](https://aws.amazon.com/s3/) are implemented. Contributions of new providers are welcome.

## Installation

with pip:
```
pip install cloud-mappings
```

By default, `cloud-mappings` doesn't install any of the required storage providers dependencies. If you would like to install them alongside `cloud-mappings` you may run any combination of:
```
pip install cloud-mappings[azure,gcp,aws]
```

## Instantiation

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
Note that AWS S3 does not support server-side atomic requests, so it is not recommended for concurrent use. A warning is printed out by default but may be silenced by passing `silence_warning=True`.

## Usage

Use it just like a standard `dict()`!
```python
cm["key"] = 1000
cm["key"] # returns 1000
del cm["key"]
"key" in cm # returns false
```

### Cloud Sync

Each `cloud-mapping` keeps an internal dict of [etags](https://en.wikipedia.org/wiki/HTTP_ETag) which it uses to ensure it is only reading/overwriting/deleting data it expects to. If the value in storage is not what the `cloud-mapping` expects, a `cloudmappings.errors.KeySyncError()` will be thrown. If you know what you are doing and want your operation to go through anyway, you will need to sync your `cloud-mapping` with the cloud by calling either `.sync_with_cloud()` to sync all keys or `.sync_with_cloud(key)` to sync a specific key. By default `.sync_with_cloud()` is called on instantiation of a `cloud-mapping` if the underlying provider storage already exists. You may skip this initial sync by passing an additional `sync_initially=False` parameter when you instantiate your `cloud-mapping`.

### Serialisation

If you don't call `.with_pickle()` and instead pass your providers configuration directly to the `cloud-mapping` class, you will get a "raw" `cloud-mapping` which only accepts byte-likes as values. You may build your own serialisation either using [zict](https://zict.readthedocs.io/en/latest/); or by calling `.with_buffers([dumps_1, loads_1, dumps_2, loads_2, ...])` where `dumps` and `loads` are the ordered functions to serialise and deserialise your data respectively.

The following exist as common starting points: `.with_pickle()`, `.with_json()`, `.with_json_zlib()`.

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)