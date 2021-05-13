import pytest

from cloudmappings.cloudstorage.azureblobstorage import AzureBlobStorage


class CloudStorageTests:
    def test_create_if_not_exists():
        # TODO:
        # Ensure initally storage does not exist:
        provider = AzureBlobStorage()
        assert provider.create_if_not_exists() == False
        assert provider.create_if_not_exists() == True

    def test_etags_are_enforced():
        provider = AzureBlobStorage()

        with pytest.raises(ValueError):
            provider.upload_data("key1", "etag-when-none-existing", b"data")

        provider.upload_data("key2", None, b"data")
        with pytest.raises(KeyError):
            provider.download_data("key2", "bad-etag", b"data")
        with pytest.raises(KeyError):
            provider.upload_data("key2", "bad-etag", b"data")
        with pytest.raises(KeyError):
            provider.delete_data("key2", "bad-etag", b"data")
