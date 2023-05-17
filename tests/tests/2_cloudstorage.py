from azure.identity import DefaultAzureCredential

from cloudmappings import (
    AWSS3Storage,
    AzureBlobStorage,
    AzureTableStorage,
    GoogleCloudStorage,
)
from cloudmappings._storageproviders.awss3storage import AWSS3StorageProvider
from cloudmappings._storageproviders.azureblobstorage import AzureBlobStorageProvider
from cloudmappings._storageproviders.azuretablestorage import AzureTableStorageProvider
from cloudmappings._storageproviders.googlecloudstorage import (
    GoogleCloudStorageProvider,
)
from cloudmappings.cloudstorage import CloudStorage
from cloudmappings.serialisers.core import pickle


class CloudStorageTests:
    def test_azure_blob_storage(self, azure_blob_storage_account_url, test_container_name):
        storage = AzureBlobStorage(
            account_url=azure_blob_storage_account_url,
            container_name=test_container_name,
            credential=DefaultAzureCredential(),
        )
        assert isinstance(storage.storage_provider, AzureBlobStorageProvider)

    def test_azure_table_storage(self, azure_table_storage_connection_string, test_container_name):
        storage = AzureTableStorage(
            connection_string=azure_table_storage_connection_string,
            table_name=test_container_name,
            credential=DefaultAzureCredential(),
        )
        assert isinstance(storage.storage_provider, AzureTableStorageProvider)

    def test_google_cloud_storage(self, gcp_storage_project, test_container_name):
        storage = GoogleCloudStorage(
            project=gcp_storage_project,
            bucket_name=test_container_name,
        )
        assert isinstance(storage.storage_provider, GoogleCloudStorageProvider)

    def test_aws_s3_storage(self, test_container_name):
        storage = AWSS3Storage(
            bucket_name=test_container_name,
        )
        assert isinstance(storage.storage_provider, AWSS3StorageProvider)

    def test_creation_defaults(self, cloud_storage: CloudStorage):
        cm = cloud_storage.create_mapping()

        assert len(cm) > 0  # As synced
        assert cm.read_blindly == False
        assert cm.read_blindly_error == False
        assert cm.read_blindly_default is None
        assert cm.serialisation == pickle()
        assert cm.key_prefix == None

    def test_creation_sync_options(self, cloud_storage: CloudStorage):
        cm = cloud_storage.create_mapping(sync_initially=False)
        assert len(cm) == 0  # As not synced

        cm = cloud_storage.create_mapping(sync_initially=True)
        assert len(cm) > 0  # As synced

    def test_creation_read_blindly_options(self, cloud_storage: CloudStorage):
        cm = cloud_storage.create_mapping(
            sync_initially=False,
            read_blindly=True,
            read_blindly_error=True,
            read_blindly_default=100,
        )
        assert cm.read_blindly == True
        assert cm.read_blindly_error == True
        assert cm.read_blindly_default == 100

        cm = cloud_storage.create_mapping(
            sync_initially=False,
            read_blindly=False,
            read_blindly_error=False,
            read_blindly_default="string",
        )
        assert cm.read_blindly == False
        assert cm.read_blindly_error == False
        assert cm.read_blindly_default == "string"

    def test_creation_key_prefix(self, cloud_storage: CloudStorage):
        cm = cloud_storage.create_mapping(sync_initially=False, key_prefix="a-prefix")
        assert cm.key_prefix == "a-prefix"

        cm = cloud_storage.create_mapping(sync_initially=False, key_prefix="prefix/2")
        assert cm.key_prefix == "prefix/2"

    def test_creation_serialisation_options(self, cloud_storage: CloudStorage):
        cm = cloud_storage.create_mapping(sync_initially=False, serialisation=pickle())
        assert cm.serialisation == pickle()

        cm = cloud_storage.create_mapping(sync_initially=False, serialisation=None)
        assert cm.serialisation == None
