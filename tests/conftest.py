import logging

from cloudmappings.storageproviders.azureblobstorage import AzureBlobStorageProvider
from cloudmappings.storageproviders.googlecloudstorage import GoogleCloudStorageProvider
from cloudmappings.storageproviders.awss3 import AWSS3Provider
from cloudmappings.cloudstoragemapping import CloudMapping


def pytest_addoption(parser):
    parser.addoption(
        "--azure_account_url",
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


storage_providers = []
cloud_mappings = []
cloud_mapping_dupes = []


def pytest_configure(config):
    test_container_name = f"pytest-{config.getoption('test_container_id')}"
    logging.info(f"Using cloud containers with the name: {test_container_name}")

    azure_account_url = config.getoption("azure_account_url")
    if azure_account_url is not None:
        storage_providers.append(
            AzureBlobStorageProvider(
                account_url=azure_account_url,
                container_name=test_container_name,
            )
        )

    gcp_project = config.getoption("gcp_project")
    if gcp_project is not None:
        storage_providers.append(
            GoogleCloudStorageProvider(
                project=gcp_project,
                bucket_name=test_container_name,
            )
        )

    if config.getoption("aws"):
        storage_providers.append(
            AWSS3Provider(
                bucket_name=test_container_name,
            )
        )

    cloud_mappings.extend([CloudMapping(storageprovider=p) for p in storage_providers])
    cloud_mapping_dupes.extend([CloudMapping(storageprovider=p) for p in storage_providers])


def pytest_generate_tests(metafunc):
    if "storage_provider" in metafunc.fixturenames:
        metafunc.parametrize(
            "storage_provider",
            storage_providers,
            scope="session",
        )

    if "cloud_mapping" in metafunc.fixturenames:
        if "cloud_mapping_dupe" in metafunc.fixturenames:
            metafunc.parametrize(
                ["cloud_mapping", "cloud_mapping_dupe"],
                zip(cloud_mappings, cloud_mapping_dupes),
                scope="session",
            )
        else:
            metafunc.parametrize(
                "cloud_mapping",
                cloud_mappings,
                scope="session",
            )
