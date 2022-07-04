from cloudmappings import (
    AWSS3Mapping,
    AzureBlobMapping,
    AzureTableMapping,
    GoogleCloudStorageMapping,
)


class WrapperTests:
    def test_azure_blob_mapping(self, azure_blob_storage_account_url, test_container_name):
        cm = AzureBlobMapping(
            account_url=azure_blob_storage_account_url,
            container_name=test_container_name,
            sync_initially=False,
            read_blindly=True,
        )
        assert len(cm.etags) == 0
        assert cm.read_blindly == True

        cm = AzureBlobMapping(
            account_url=azure_blob_storage_account_url,
            container_name=test_container_name,
        )
        assert len(cm.etags) > 0
        assert cm.read_blindly == False
        cm.read_blindly = True
        assert cm.read_blindly == True

    def test_azure_table_mapping(self, azure_table_storage_connection_string, test_container_name):
        cm = AzureTableMapping(
            connection_string=azure_table_storage_connection_string,
            table_name=test_container_name,
            sync_initially=False,
            read_blindly=True,
        )
        assert len(cm.etags) == 0
        assert cm.read_blindly == True

        cm = AzureTableMapping(
            connection_string=azure_table_storage_connection_string,
            table_name=test_container_name,
        )
        assert len(cm.etags) > 0
        assert cm.read_blindly == False
        cm.read_blindly = True
        assert cm.read_blindly == True

    def test_gcp_storage_mapping(self, gcp_storage_project, test_container_name):
        cm = GoogleCloudStorageMapping(
            project=gcp_storage_project,
            bucket_name=test_container_name,
            sync_initially=False,
            read_blindly=True,
        )
        assert len(cm.etags) == 0
        assert cm.read_blindly == True

        cm = GoogleCloudStorageMapping(
            project=gcp_storage_project,
            bucket_name=test_container_name,
        )
        assert len(cm.etags) > 0
        assert cm.read_blindly == False
        cm.read_blindly = True
        assert cm.read_blindly == True

    def test_aws_s3_mapping(self, test_container_name):
        cm = AWSS3Mapping(
            bucket_name=test_container_name,
            sync_initially=False,
            read_blindly=True,
        )
        assert len(cm.etags) == 0
        assert cm.read_blindly == True

        cm = AWSS3Mapping(
            bucket_name=test_container_name,
        )
        assert len(cm.etags) > 0
        assert cm.read_blindly == False
        cm.read_blindly = True
        assert cm.read_blindly == True
