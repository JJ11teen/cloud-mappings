# cloud-mappings
MutableMapping implementations for common cloud storage providers

[![Build](https://github.com/JJ11teen/cloud-mappings/actions/workflows/build.yaml/badge.svg)](https://github.com/JJ11teen/cloud-mappings/actions/workflows/build.yaml)
[![PyPI version](https://badge.fury.io/py/cloud-mappings.svg)](https://pypi.org/project/cloud-mappings/)

For now [Azure Blob Storage](https://azure.microsoft.com/en-au/services/storage/blobs), [Azure Table Storage](https://azure.microsoft.com/en-au/services/storage/tables), [Google Cloud Storage](https://cloud.google.com/storage/), and [AWS S3](https://aws.amazon.com/s3/) are implemented. Contributions of new providers are welcome.

## Installation

with pip:
```
pip install cloud-mappings
```

By default, `cloud-mappings` doesn't install any of the required storage providers dependencies. If you would like to install them alongside `cloud-mappings` you may run any combination of:
```
pip install cloud-mappings[azureblob,azuretable,gcpstorage,awss3]
```

## Instantiation

### AzureBlobMapping:
```python
from cloudmappings import AzureBlobMapping

cm = AzureBlobMapping.with_pickle(
    container_name="CONTAINER_NAME",
    account_url="AZURE_BLOB_STORAGE_URL",
    credential=AZURE_CREDENTIAL_OBJECT,
    connection_string="AZURE_BLOB_STORAGE_CONNECTION_STRING",
)
```

### AzureTableMapping:
```python
from cloudmappings import AzureTableMapping

cm = AzureTableMapping.with_pickle(
    table_name="TABLE_NAME",
    endpoint="AZURE_TABLE_ENDPOINT",
    credential=AZURE_CREDENTIAL_OBJECT,
    connection_string="AZURE_TABLE_CONNECTION_STRING",
)
```
Note that Azure Table Storage has a 1MB size limit per entity.

### GoogleCloudStorageMapping:
```python
from cloudmappings import GoogleCloudStorageMapping

cm = GoogleCloudStorageMapping.with_pickle(
    bucket_name="BUCKET_NAME",
    project="GCP_PROJECT",
    credentials=GCP_CREDENTIALS_OBJECT,
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

### Etags

Each `cloud-mapping` keeps an internal dict of [etags](https://en.wikipedia.org/wiki/HTTP_ETag) which it uses to ensure it is only reading/overwriting/deleting data it expects to. If the value in storage is not what the `cloud-mapping` expects, a `cloudmappings.errors.KeySyncError()` will be thrown.

If you would like to enable read (get) operations without ensuring etags, you can set `read_blindly=True`. This can be set in the constructor, or dynamically on the cloud-mapping instance. Blindly reading a value that doesn't exist in the cloud will return the mapping's current value of `read_blindly_default` (which itself defaults to `None`).

If you know what you are doing and you want an operation other than get to go through despite etags, you will need to sync your `cloud-mapping` with the cloud by calling either `.sync_with_cloud()` to sync all keys or `.sync_with_cloud(key_prefix)` to sync a specific key or subset of keys. By default `.sync_with_cloud()` is called on instantiation of a `cloud-mapping` if the underlying provider storage already exists. You may skip this initial sync by passing an additional `sync_initially=False` parameter when you instantiate your `cloud-mapping`.

The `etags` property on a `cloud-mapping` can be manually inspected and adjusted for advanced use cases, but it is not recommended if your use case can be accomplished with the above methods.

### Serialisation

If you don't call `.with_pickle()` and instead pass your providers configuration directly to the `CloudMapping` class, you will get a "raw" `cloud-mapping` which accepts only byte-likes as values. Along with the `.with_pickle()` serialisation utility, `.with_json()` and `.with_json_zlib()` also exist. You may build your own serialisation by constructing your cloud-mapping with `ordered_dumps_funcs=[dumps_1, dumps_2, ..., dumps_N]` and `ordered_loads_funcs=[loads_1, loads_2, ..., loads_N]`, where `dumps` and `loads` are the ordered functions to serialise and parse your data respectively.





# Development

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This project uses `.devcontainer` to describe the environment to use for development. You may use the environment described in this directory (it integrates automatically with vscode's 'remote containers' extension), or you may create your own environment with the same dependencies.

## Dependencies
Install development dependencies with:

`pip install -e .[azureblob,azuretable,gcpstorage,awss3,tests]`

## Tests
Set environment variables for each provider:
* Azure Blob: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_BLOB_STORAGE_ACCOUNT_URL`, `AZURE_BLOB_STORAGE_HIERARCHICAL_ACCOUNT_URL` (the tests assume the same secret is used for both)
* Azure Table: `AZURE_TABLE_STORAGE_CONNECTION_STRING`
* GCP Storage: `GOOGLE_APPLICATION_CREDENTIALS` (path to credentials file), `GOOGLE_CLOUD_STORAGE_PROJECT`
* AWS S3: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

Run tests with:
```bash
pytest --test_container_id <container-suffix-to-use-for-tests>
```
The testing container will be prefixed by "pytest", and the commit sha is used within build & release workflows. Note that if the container specified already exists one test will fail.