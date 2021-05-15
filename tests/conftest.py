from ast import parse
from cloudmappings.storageproviders.azureblobstorage import AzureBlobStorageProvider
from cloudmappings.storageproviders.googlecloudstorage import GoogleCloudStorageProvider
from cloudmappings.storageproviders.awss3 import AWSS3Provider
from cloudmappings.cloudstoragemapping import CloudStorageMapping


def pytest_addoption(parser):
    parser.addoption(
        "--azure_account_url_with_sas",
        action="store",
        help="Azure Account URL signed with SAS",
    )
    parser.addoption(
        "--azure_container_name",
        action="store",
        help="Azure Container Name",
    )
    parser.addoption(
        "--gcp_project",
        action="store",
        help="GCP Project Id",
    )
    parser.addoption(
        "--gcp_bucket_name",
        action="store",
        help="GCP Bucket Name",
    )
    parser.addoption(
        "--aws_bucket_name",
        action="store",
        help="AWS Bucket Name",
    )


storage_providers = []
cloud_mappings = []


def pytest_configure(config):
    azure_account_url = config.getoption("azure_account_url_with_sas")
    azure_container_name = config.getoption("azure_container_name")
    if azure_account_url is not None or azure_container_name is not None:
        storage_providers.append(
            AzureBlobStorageProvider(
                account_url=azure_account_url,
                container_name=azure_container_name,
            )
        )

    gcp_project = config.getoption("gcp_project")
    gcp_bucket_name = config.getoption("gcp_bucket_name")
    if gcp_project is not None or gcp_bucket_name is not None:
        storage_providers.append(
            GoogleCloudStorageProvider(
                project=gcp_project,
                bucket_name=gcp_bucket_name,
            )
        )

    aws_bucket_name = config.getoption("aws_bucket_name")
    if aws_bucket_name is not None:
        storage_providers.append(
            AWSS3Provider(
                bucket_name=aws_bucket_name,
            )
        )

    cloud_mappings.extend([CloudStorageMapping(storageprovider=provider) for provider in storage_providers])


def pytest_generate_tests(metafunc):
    if "storage_provider" in metafunc.fixturenames:
        metafunc.parametrize(
            "storage_provider",
            storage_providers,
            scope="session",
        )

    if "cloud_mapping" in metafunc.fixturenames:
        metafunc.parametrize(
            "cloud_mapping",
            cloud_mappings,
            scope="session",
        )
