import os
from uuid import uuid4
import logging

import pytest

from cloudmappings.storageproviders.azureblobstorage import AzureBlobStorageProvider
from cloudmappings.storageproviders.azuretablestorage import AzureTableStorageProvider
from cloudmappings.storageproviders.googlecloudstorage import GoogleCloudStorageProvider
from cloudmappings.storageproviders.awss3 import AWSS3Provider


def pytest_addoption(parser):
    parser.addoption(
        "--azure_blob_storage_account_url",
        action="store",
        help="Azure Blob Storage Account URL",
    )
    parser.addoption(
        "--azure_table",
        action="store_true",
        help="Run Azure Table Storage Tests",
    )
    parser.addoption(
        "--gcp_storage_project",
        action="store",
        help="GCP Project Id",
    )
    parser.addoption(
        "--aws_s3",
        action="store_true",
        help="Run AWS S3 Tests",
    )
    parser.addoption(
        "--test_container_id",
        action="store",
        required=True,
        help="Suffix to add to container resources used for this test run. Use commit hash in cicd",
    )


storage_providers = {}
test_run_id = str(uuid4())


def pytest_configure(config):
    test_container_name = f"pytest{config.getoption('test_container_id')}"
    logging.warning(f"Using cloud containers with the name: {test_container_name}")
    logging.warning(f"Using keys with the prefix: {test_run_id}")

    azure_blob_storage_account_url = config.getoption("azure_blob_storage_account_url")
    if azure_blob_storage_account_url is not None:
        storage_providers["azure_blob"] = AzureBlobStorageProvider(
            account_url=azure_blob_storage_account_url,
            container_name=test_container_name,
        )

    if config.getoption("azure_table") is not None:
        storage_providers["azure_table"] = AzureTableStorageProvider(
            connection_string=os.environ["AZURE_TABLE_STORAGE_CONNECTION_STRING"],
            table_name=test_container_name,
        )

    gcp_storage_project = config.getoption("gcp_storage_project")
    if gcp_storage_project is not None:
        storage_providers["gcp_storage"] = GoogleCloudStorageProvider(
            project=gcp_storage_project,
            bucket_name=test_container_name,
        )

    if config.getoption("aws_s3"):
        storage_providers["aws_s3"] = AWSS3Provider(
            bucket_name=test_container_name,
        )


def pytest_generate_tests(metafunc):
    if "storage_provider" in metafunc.fixturenames:
        metafunc.parametrize(
            "storage_provider",
            storage_providers.values(),
            ids=storage_providers.keys(),
            scope="session",
        )


@pytest.fixture
def test_id():
    return test_run_id
