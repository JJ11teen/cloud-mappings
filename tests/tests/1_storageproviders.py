import pytest

from cloudmappings.errors import KeySyncError
from cloudmappings.storageproviders.storageprovider import StorageProvider


class StorageProviderTests:
    def test_create_if_not_exists(self, storage_provider: StorageProvider):
        # The pytest arg "test_container_id",
        # combined with the this being the first test run for each provider,
        # ensures that initally storage does not exist.
        # We therefore expect False for the first call and True for the second:
        assert storage_provider.create_if_not_exists() == False
        assert storage_provider.create_if_not_exists() == True

    def test_data_is_stored(self, storage_provider: StorageProvider, test_id: str):
        key = test_id + "-data-store-test"

        etag = storage_provider.upload_data(key, None, b"data")
        data = storage_provider.download_data(key, etag)

        assert data == b"data"

    def test_keys_and_etags_are_listed(self, storage_provider: StorageProvider, test_id: str):
        key_1 = test_id + "-keys-and-etags-list-test-1"
        key_2 = test_id + "-keys-and-etags-list-test-2"

        etag_1 = storage_provider.upload_data(key_1, None, b"data")
        etag_2 = storage_provider.upload_data(key_2, None, b"data")
        keys_and_etags = storage_provider.list_keys_and_etags(None)

        assert key_1 in keys_and_etags
        assert key_2 in keys_and_etags
        assert etag_1 == keys_and_etags[key_1]
        assert etag_2 == keys_and_etags[key_2]

        for key, etag in keys_and_etags.items():
            assert etag is not None, (key, etag)

    def test_keys_are_deleted(self, storage_provider: StorageProvider, test_id: str):
        key = test_id + "-keys-deleted-test"

        etag = storage_provider.upload_data(key, None, b"data")
        storage_provider.delete_data(key, etag)

        cloud_key_list = storage_provider.list_keys_and_etags(key)
        assert key not in cloud_key_list

    def test_etags_are_enforced(self, storage_provider: StorageProvider, test_id: str):
        key = test_id + "etags-enforced-test"

        with pytest.raises(KeySyncError):
            storage_provider.upload_data(key, "etag-when-none-existing", b"data")

        good_etag = storage_provider.upload_data(key, None, b"0")
        assert good_etag is not None

        with pytest.raises(KeySyncError):
            storage_provider.download_data(key, "bad-etag")
        with pytest.raises(KeySyncError):
            storage_provider.upload_data(key, "bad-etag", b"data")
        with pytest.raises(KeySyncError):
            storage_provider.delete_data(key, "bad-etag")

    def test_etags_change_with_same_data(self, storage_provider: StorageProvider, test_id: str):
        key = test_id + "etags-unique-same-data-test"

        first_etag = storage_provider.upload_data(key, None, b"static-data")
        second_etag = storage_provider.upload_data(key, first_etag, b"static-data")

        assert first_etag != second_etag
