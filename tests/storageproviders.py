import pytest

from cloudmappings.storageproviders.storageprovider import KeySyncError


class StorageProviderTests:
    def test_create_if_not_exists(self, storage_provider):
        # TODO: Ensure initally storage does not exist:
        # assert storage_provider.create_if_not_exists() == False
        assert storage_provider.create_if_not_exists() == True

    def test_data_is_stored(self, storage_provider):
        etag = storage_provider.upload_data("key1", None, b"data")
        data = storage_provider.download_data("key1", etag)
        assert data == b"data"

    def test_keys_and_etags_are_listed(self, storage_provider):
        etag_3 = storage_provider.upload_data("key3", None, b"data")
        etag_4 = storage_provider.upload_data("key4", None, b"data")
        keys_and_etags = storage_provider.list_keys_and_etags(None)

        assert "key3" in keys_and_etags
        assert "key4" in keys_and_etags
        assert etag_3 == keys_and_etags["key3"]
        assert etag_4 == keys_and_etags["key4"]

    def test_keys_are_deleted(self, storage_provider):
        etag = storage_provider.upload_data("key5", None, b"data")
        storage_provider.delete_data("key5", etag)

        cloud_key_list = storage_provider.list_keys_and_etags("key5")
        assert "key5" not in cloud_key_list

    def test_etags_are_enforced(self, storage_provider):
        with pytest.raises(KeySyncError):
            storage_provider.upload_data("key6", "etag-when-none-existing", b"data")

        storage_provider.upload_data("key6", None, b"0")
        with pytest.raises(KeySyncError):
            storage_provider.download_data("key6", "bad-etag")
        with pytest.raises(KeySyncError):
            storage_provider.upload_data("key6", "bad-etag", b"data")
        with pytest.raises(KeySyncError):
            storage_provider.delete_data("key6", "bad-etag")
