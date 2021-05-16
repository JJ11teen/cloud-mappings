from uuid import uuid4
import logging

import pytest

from cloudmappings.storageproviders.azureblobstorage import AzureBlobStorageProvider
from cloudmappings.storageproviders.googlecloudstorage import GoogleCloudStorageProvider
from cloudmappings.storageproviders.awss3 import AWSS3Provider


def pytest_addoption(parser):
    parser.addoption(
        "--azure_storage_account_url",
        action="store",
        help="Azure Storage Account URL",
    )
    parser.addoption(
        "--gcp_project",
        action="store",
        help="GCP Project Id",
    )
    parser.addoption(
        "--aws",
        action="store_true",
        help="Run AWS Tests",
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
    test_container_name = f"pytest-{config.getoption('test_container_id')}"
    logging.warning(f"Using cloud containers with the name: {test_container_name}")
    logging.warning(f"Using keys with the prefix: {test_run_id}")

    azure_storage_account_url = config.getoption("azure_storage_account_url")
    if azure_storage_account_url is not None:
        storage_providers["azure"] = AzureBlobStorageProvider(
            account_url=azure_storage_account_url,
            container_name=test_container_name,
        )

    gcp_project = config.getoption("gcp_project")
    if gcp_project is not None:
        storage_providers["gcp"] = GoogleCloudStorageProvider(
            project=gcp_project,
            bucket_name=test_container_name,
        )

    if config.getoption("aws"):
        storage_providers["aws"] = AWSS3Provider(
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
