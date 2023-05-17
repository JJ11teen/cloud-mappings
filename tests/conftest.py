import logging
import os
from uuid import uuid4

import pytest
from azure.identity import DefaultAzureCredential

from cloudmappings._storageproviders.awss3storage import AWSS3StorageProvider
from cloudmappings._storageproviders.azureblobstorage import AzureBlobStorageProvider
from cloudmappings._storageproviders.azuretablestorage import AzureTableStorageProvider
from cloudmappings._storageproviders.googlecloudstorage import (
    GoogleCloudStorageProvider,
)
from cloudmappings.cloudmapping import CloudMapping
from cloudmappings.cloudstorage import CloudStorage
from cloudmappings.storageprovider import StorageProvider


def pytest_addoption(parser):
    parser.addoption(
        "--test-container-id",
        action="store",
        required=True,
        help="Suffix to add to container resources used for this test run. Use commit hash in cicd",
    )


@pytest.fixture(scope="session")
def run_id() -> str:
    test_run_id = str(uuid4())
    logging.warning(f"Using keys with the prefix: {test_run_id}")
    return test_run_id


@pytest.fixture(scope="function")
def test_id() -> str:
    return str(uuid4())


@pytest.fixture(scope="function")
def test_prefix(run_id: str, test_id: str) -> str:
    return f"{run_id}/{test_id}"


@pytest.fixture(scope="session")
def test_container_name(pytestconfig) -> str:
    logging.warning(f"Using cloud containers with the name: {test_container_name}")
    return f"pytest{pytestconfig.getoption('test_container_id')}"


@pytest.fixture(scope="session")
def azure_blob_storage_account_url() -> str:
    return os.environ["AZURE_BLOB_STORAGE_ACCOUNT_URL"]


@pytest.fixture(scope="session")
def azure_blob_storage_hierarchical_account_url() -> str:
    return os.environ["AZURE_BLOB_STORAGE_HIERARCHICAL_ACCOUNT_URL"]


@pytest.fixture(scope="session")
def azure_table_storage_connection_string() -> str:
    return os.environ["AZURE_TABLE_STORAGE_CONNECTION_STRING"]


@pytest.fixture(scope="session")
def gcp_storage_project() -> str:
    return os.environ["GOOGLE_CLOUD_STORAGE_PROJECT"]


@pytest.fixture(
    scope="session",
    params=[
        "azure_blob_storage",
        "azure_blob_storage_hierarchical",
        "azure_table_storage",
        "google_cloud_storage",
        "aws_s3",
    ],
)
def storage_provider(
    request,
    test_container_name,
    azure_blob_storage_account_url,
    azure_blob_storage_hierarchical_account_url,
    azure_table_storage_connection_string,
    gcp_storage_project,
) -> StorageProvider:
    if request.param == "azure_blob_storage":
        return AzureBlobStorageProvider(
            account_url=azure_blob_storage_account_url,
            container_name=test_container_name,
            credential=DefaultAzureCredential(),
        )
    elif request.param == "azure_blob_storage_hierarchical":
        return AzureBlobStorageProvider(
            account_url=azure_blob_storage_hierarchical_account_url,
            container_name=test_container_name,
            credential=DefaultAzureCredential(),
        )
    elif request.param == "azure_table_storage":
        return AzureTableStorageProvider(
            connection_string=azure_table_storage_connection_string,
            table_name=test_container_name,
            credential=DefaultAzureCredential(),
        )
    elif request.param == "google_cloud_storage":
        return GoogleCloudStorageProvider(
            project=gcp_storage_project,
            bucket_name=test_container_name,
        )
    elif request.param == "aws_s3":
        return AWSS3StorageProvider(
            bucket_name=test_container_name,
        )
    raise ValueError(f"Test requested unknown storage provider '{request.param}'")


@pytest.fixture()
def cloud_storage(storage_provider: StorageProvider) -> CloudStorage:
    return CloudStorage(storage_provider=storage_provider)


@pytest.fixture(scope="function")
def cloud_mapping(cloud_storage: CloudStorage, test_prefix: str) -> CloudMapping:
    return cloud_storage.create_mapping(key_prefix=f"{test_prefix}/")


@pytest.fixture(scope="function")
def cloud_mapping_two(cloud_storage: CloudStorage, test_prefix: str) -> CloudMapping:
    return cloud_storage.create_mapping(key_prefix=f"{test_prefix}/")
