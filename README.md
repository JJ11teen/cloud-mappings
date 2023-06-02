# cloud-mappings
MutableMapping implementations for common cloud storage providers - easily store things in the cloud through a simple dictionary interface!

[![Build](https://github.com/JJ11teen/cloud-mappings/actions/workflows/build.yaml/badge.svg)](https://github.com/JJ11teen/cloud-mappings/actions/workflows/build.yaml)
[![PyPI version](https://badge.fury.io/py/cloud-mappings.svg)](https://pypi.org/project/cloud-mappings/)

For now [Azure Blob Storage](https://azure.microsoft.com/en-au/services/storage/blobs), [Azure Table Storage](https://azure.microsoft.com/en-au/services/storage/tables), [Google Cloud Storage](https://cloud.google.com/storage/), and [AWS S3](https://aws.amazon.com/s3/) are implemented. Contributions of new providers are welcome.

## Use Cases

* Easily read/write to a bucket without needing to learn boto3 (same for Azure, GCP)
* Ensure consistent serialisation/deserialisation with a simple interface
* Safely use a cloud storage concurrently, for example multiple data scientists working in notebooks or multiple nodes in a distributed compute cluster
* Built a robust multi-layer data caching solution by combining with [zict](https://zict.readthedocs.io/en/latest/index.html)

## Installation

with pip:
```
pip install cloud-mappings
```

By default, `cloud-mappings` doesn't install any of the required storage providers dependencies, allowing you to version them as you see fit. If you would like to install the versions with `cloud-mappings` you may run any combination of:
```
pip install cloud-mappings[azureblob,azuretable,gcpstorage,awss3]
```

## Usage

Use it just like a standard python dictionary! (One you would create with `dict()` or `{}`). Under the hood each write to the dictionary is serialised and saved to the cloud, and each read is downloaded and deserialised. Iterating keys, values and items all work as expected.
```python
cm["key"] = 1000
cm["key"] # returns 1000
del cm["key"]
"key" in cm # returns false
for k, v in cm.items():
    print(k, v) # prints: key 1000
```

## Initialisation

### AzureBlobStorage:
```python
from azure.identity import AzureDefaultCredential
from cloudmappings import AzureBlobStorage

cm = AzureBlobStorage(
    account_url="BLOB_ACCOUNT_URL",
    container_name="CONTAINER_NAME",
    credential=AzureDefaultCredential(),
).create_mapping()
```

### AzureTableStorage:
```python
from azure.identity import AzureDefaultCredential
from cloudmappings import AzureTableStorage

cm = AzureTableStorage(
    table_name="TABLE_NAME",
    endpoint="AZURE_TABLE_ENDPOINT",
    credential=AzureDefaultCredential(),
).create_mapping()
```
Note that Azure Table Storage has a 1MB size limit per entity.

### GoogleCloudStorage:
```python
from cloudmappings import GoogleCloudStorage

cm = GoogleCloudStorage(
    bucket_name="BUCKET_NAME",
    project="GCP_PROJECT",
).create_mappings()
```

### AWSS3Storage:
```python
from cloudmappings import AWSS3Storage

cm = AWSS3Storage(
    bucket_name="AWS_BUCKET_NAME",
    silence_warning=False,
).create_mapping()
```
Note that AWS S3 does not support server-side atomic requests, so it is not recommended for concurrent use. A warning is printed out by default but may be silenced by passing `silence_warning=True`.

# API Docs

## CloudStorage class

A `CloudStorage` object is the entrypoint for this library. You create one but instantiating one for the cloud storage provider you wish to use, currently `AWSS3Storage`, `AzureBlobStorage`, `AzureTableStorage`, `GoogleCloudStorage`. The parameters vary for each, and map to the details required for locating and authenticating the cloud resource they represent. A simple example for each is provided above. From a `CloudStorage` instance, (multiple) `CloudMapping[T]`s may be created by calling `.create_mapping()`:

```python
CloudStorage.create_mapping(
    sync_initially: bool = True,
    read_blindly: bool = False,
    read_blindly_error: bool = False,
    read_blindly_default: Any = None,
    serialisation: CloudMappingSerialisation[T] = pickle(),
    key_prefix: Optional[str] = None,
) -> CloudMapping[T]:
```
Parameters:
* `sync_initially: bool = True`
  * Whether to call `sync_with_cloud` initially
* `read_blindly: bool = False`
  * Whether the `CloudMapping` will read from the cloud without synchronising.
  * When `read_blindly=False`, a `CloudMapping` will raise a `KeyError` unless a key has been previously written using the same `CloudMapping` instance, or `.sync_with_cloud` has been called and the key was in the cloud. If the value in the cloud has changed since being written or synchronised, a `cloudmappings.errors.KeySyncError` will be raised.
  * When `read_blindly=True`, a `CloudMapping` will directly query the cloud for any key accessed, regardless of if it has previously written a value to that key. It will always get the latest value from the cloud, and never raise a `cloudmappings.errors.KeySyncError` for read operations. If there is no value for a key in the cloud, and `read_blindly_error=True`, a `KeyError` will be raised. If there is no value for a key in the cloud and `read_blindly_error=False`, `read_blindly_default` will be returned.
* `read_blindly_error : bool = False`
  * Whether to raise a `KeyValue` error when `read_blindly=True` and a key does not have a value in the cloud. If `True`, this takes prescedence over `read_blindly_default`.
* `read_blindly_default : Any = None`
  * The value to return when `read_blindly=True`, a key does not have a value in the cloud, and `read_blindly_error=False`.
* `serialiser: CloudMappingSerialiser = pickle()`
  * CloudMappingSerialiser to use, defaults to `pickle`. Is also used to determine the type hint for the `CloudMapping[T]`.
* `key_prefix: Optional[str] = None`
  * Prefix to apply to keys in cloud storage. Enables `CloudMapping`s to map to a subdirectory within a cloud storage service, as opposed to the whole resource.

When no arguments are passed, the created `CloudMapping[T]` will:
* Have a type of `CloudMapping[Any]`, equivalent to `dict[str, Any]`
* Sync initially, meaning it will query the cloud and fetch a list of keys that exist
* Raise `KeyError`s if a key is read before being written (unless explicitly synchronised)
* Use `pickle` for serialisation (both reads and writes)
* Apply no prefix to keys, meaning the keys used in python map 1:1 with keys in the cloud


## CloudMapping class

The `CloudMapping[T]` object is the primary construct of this library and is returned from `.create_mapping()`. It implements the `MutableMapping[str, T]` interface (meaning you can use it as a dictionary), but additionally it provides a few extra cloud-specific options and functions.

### Mutable Properties:
See the parameters of `CloudStorage.create_mapping()` above for their descriptions.
* `read_blindly: bool`
* `read_blindly_error: bool`
* `read_blindly_default: Any`

### Immutable Properties:
* `storage_provider: StorageProvider`
  * An object that provides a consistent interface to the underlying storage provider (eg methods to read and write bytes to specific paths).
* `etags: dict[str, str]`
  * An internal dictionary of etags used to ensure the `CloudMapping` is in sync with the cloud storage resource. The dict maps keys to their last synchronised etags.
  * This dictionary is used as the `CloudMapping's expected view of the cloud. It is used to determine if a key exists, and ensure that the value of each key is expected.
  * See: https://en.wikipedia.org/wiki/HTTP_ETag
* `serialisation: CloudMappingSerialisation[T]`
  * Gets the serialiser configured to use for serialising and deserialising values.
* `key_prefix: Optional[str]`
  * Gets the key prefix configured to prepend to keys in the cloud. It is also used to filter what is synchronised, resulting in the `CloudMapping` mapping to a subset of the cloud resource.
### Methods:
* `sync_with_cloud(self, key_prefix: str = None) -> None`
  * Synchronise this `CloudMapping` with the cloud.
  * This allows a `CloudMapping` to reflect the most recent updates to the cloud resource, including those made by other instances or users. This can allow destructive operations as a user may synchronise to get the latest updates, and then overwrite or delete values.
  * Consider calling this if you are encountering a `cloudmappings.errors.KeySyncError`, and you are sure you would like to force the operation anyway.
  * This is called by default on instantiation of a `CloudMapping`.
  * Parameters:
    * `key_prefix : str, optional`
      * Only sync keys beginning with the specified prefix, the key_prefix configured on the mapping is prepended in combination with this parameter.

## CloudMappingSerialisation class

The `CloudMappingSerialisation` class is simple dataclass that combines serialisation and deserialisation. It has two properties, a dumps and a loads function. Values are passed through the dumps function when being written to the `CloudMapping` and saved to the cloud, and values are passed through the loads function when being loaded from the cloud and read from the `CloudMapping`.

A `CloudMappingSerialisation` may be created directly with singular dumps and loads functions. A `CloudMappingSerialisation.from_chain()` helper method exists for when you would like multiple functions to be chained during serialisation and deserialisation.

Some common `CloudMappingSerialisation`s are also provided out of the box.

### Immutable Properties
* `dumps: Callable`
  * Function to dump values through when writing to the cloud.
  * Must return a bytes-like object.
* `loads: Callable`
  * Function to load values through when reading from the cloud.
  * Must accept a bytes-like object as its input.

### Static Methods
* `from_chain(ordered_dumps_funcs: List[Callable], ordered_loads_funcs: List[Callable]) -> CloudMappingSerialisation[T]`
  * Creates a CloudMappingSerialisation by chaining consecutive dumps and loads functions together
  * Parameters:
    * `ordered_dumps_funcs: List[Callable]`
      * An ordered list of functions to pass values through before saving bytes to the cloud.
      * The last function must return a bytes-like object.
    * `ordered_loads_funcs: List[Callable]`
      * An ordered list of functions to pass values through before saving bytes to the cloud.
      * The first function must accept a bytes-like object as its input.

### Common Serialisations Provided
* `cloudmappings.serialisers.core`
  * Provides functions for serialisers that have no additional dependencies
  * `none() -> CloudMappingSerialisation[bytes]`
    * This serialiser performs no serialisation, and just passes raw bytes
    * It is implemented as `None`, and `None` can be used directly, however
    using this serialiser enables type hints to correctly determine the that the mapping should be `CloudMapping[bytes]`.
  * `pickle(protocol: int = None) -> CloudMappingSerialisation[Any]`
    * Serialiser that pickles values using pythons `pickle`
    * Parameters:
      * `protocol: int = None`
      *  The pickle protocol to use, defaults to None which internally default to `pickle.DEFAULT_PROTOCOL`
  * `raw_string(encoding: str = "utf-8") -> CloudMappingSerialisation[str]`
    * Serialiser that only encodes raw string values
    * Parameters:
      * `encoding: str = "utf-8"`
        * The string encoding to use, passed to bytes() and str() for dumps and loads respectively
  * `json(encoding: str = "utf-8") -> CloudMappingSerialisation[Any]`
    * Serialiser that saves objects as JSON strings
    * Parameters:
      * `encoding: str = "utf-8"`
        * The string encoding to use, passed to bytes() and str() for dumps and loads respectively
  * `json_zlib(encoding: str = "utf-8") -> CloudMappingSerialisation[Any]`
    * Serialiser that saves values as compressed JSON strings, it uses zlib to compress values after serialising them as JSON strings.
    * Parameters:
      * `encoding: str = "utf-8"`
        * The string encoding to use, passed to bytes() and str() for dumps and loads respectively
* `cloudmappings.serialisers.pandas`
  * Provides functions for serialisers that use [pandas](https://pandas.pydata.org/) as an additional dependency
  * `csv() -> CloudMappingSerialisation[DataFrame]`
    * Serialiser that uses pandas to serialise DataFrames as csvs

## Concurrent Use

Being able to upload/download easily without learning the various cloud sdks is only one benefit of cloud-mappings! `cloud-mappings` is also designed to support concurrent use providing safety and functionality not provided by the cloud sdks.

| | Session 1 | Session 2 |
| --- | --- | --- |
| Both sessions create their cloud mappings, | `cm = ....` | `cm = ....` |
| referencing the same cloud storage | | |
| Session 1 writes some data to a key | `cm["key] = "Session 1 data"` | |
| Session 2 attempts to write over it | | `cm["key] = "Session 2 data"` |
| Session 2 gets a Error | | `KeySyncError` |

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
pytest --test-container-id <container-suffix-to-use-for-tests>
```
The testing container will be prefixed by "pytest", and the commit sha is used within build & release workflows. Note that if the container specified already exists one test will fail.
