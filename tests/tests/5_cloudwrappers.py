from azure.identity import DefaultAzureCredential

from cloudmappings import (
    AWSS3Storage,
    AzureBlobStorage,
    AzureTableStorage,
    GoogleCloudStorage,
)


class WrapperTests:
    def test_azure_blob_mapping(self, azure_blob_storage_account_url, test_container_name):
        storage = AzureBlobStorage(
            account_url=azure_blob_storage_account_url,
            container_name=test_container_name,
            credential=DefaultAzureCredential(),
        )
        cm = storage.create_mapping(
            sync_initially=False,
            read_blindly=True,
            read_blindly_error=False,
            read_blindly_default=None,
        )
        assert len(cm.etags) == 0
        assert cm.read_blindly == True

        cm = storage.create_mapping()
        assert len(cm.etags) > 0
        assert cm.read_blindly == False
        cm.read_blindly = True
        assert cm.read_blindly == True

    def test_azure_table_mapping(self, azure_table_storage_connection_string, test_container_name):
        storage = AzureTableStorage(
            connection_string=azure_table_storage_connection_string,
            table_name=test_container_name,
            credential=DefaultAzureCredential(),
        )
        cm = storage.create_mapping(
            sync_initially=False,
            read_blindly=True,
            read_blindly_error=False,
            read_blindly_default=None,
        )
        assert len(cm.etags) == 0
        assert cm.read_blindly == True

        cm = storage.create_mapping()
        assert len(cm.etags) > 0
        assert cm.read_blindly == False
        cm.read_blindly = True
        assert cm.read_blindly == True

    def test_gcp_storage_mapping(self, gcp_storage_project, test_container_name):
        storage = GoogleCloudStorage(
            project=gcp_storage_project,
            bucket_name=test_container_name,
        )
        cm = storage.create_mapping(
            sync_initially=False,
            read_blindly=True,
            read_blindly_error=False,
            read_blindly_default=None,
        )
        assert len(cm.etags) == 0
        assert cm.read_blindly == True

        cm = storage.create_mapping()
        assert len(cm.etags) > 0
        assert cm.read_blindly == False
        cm.read_blindly = True
        assert cm.read_blindly == True

    def test_aws_s3_mapping(self, test_container_name):
        storage = AWSS3Storage(
            bucket_name=test_container_name,
        )
        cm = storage.create_mapping(
            sync_initially=False,
            read_blindly=True,
            read_blindly_error=False,
            read_blindly_default=None,
        )
        assert len(cm.etags) == 0
        assert cm.read_blindly == True

        cm = storage.create_mapping()
        assert len(cm.etags) > 0
        assert cm.read_blindly == False
        cm.read_blindly = True
        assert cm.read_blindly == True
