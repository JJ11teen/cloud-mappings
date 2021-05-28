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

    def test_keys_are_encoded_decoded(self, storage_provider: StorageProvider):
        alphanumeric = "simplekey0"
        forwardslash = "/here/are/forward/slashes"
        othercharacters = "/how.about_some ˆøœ¨åß∆∫ı˜unusual!@#$%^*characters"

        assert alphanumeric == storage_provider.decode_key(storage_provider.encode_key(alphanumeric))
        assert forwardslash == storage_provider.decode_key(storage_provider.encode_key(forwardslash))
        assert othercharacters == storage_provider.decode_key(storage_provider.encode_key(othercharacters))

    def test_data_is_stored(self, storage_provider: StorageProvider, test_id: str):
        key = test_id + "-data-store-test"
        encoded_key = storage_provider.encode_key(key)

        etag = storage_provider.upload_data(encoded_key, None, b"data")
        data = storage_provider.download_data(encoded_key, etag)

        assert data == b"data"

    def test_keys_and_etags_are_listed(self, storage_provider: StorageProvider, test_id: str):
        key_1 = test_id + "-keys-and-etags-list-test-1"
        key_2 = test_id + "-keys-and-etags-list-test-2"
        encoded_key_1 = storage_provider.encode_key(key_1)
        encoded_key_2 = storage_provider.encode_key(key_2)

        etag_1 = storage_provider.upload_data(encoded_key_1, None, b"data")
        etag_2 = storage_provider.upload_data(encoded_key_2, None, b"data")
        keys_and_etags = storage_provider.list_keys_and_etags(None)

        assert encoded_key_1 in keys_and_etags
        assert encoded_key_2 in keys_and_etags
        assert etag_1 == keys_and_etags[encoded_key_1]
        assert etag_2 == keys_and_etags[encoded_key_2]

        for encoded_key, etag in keys_and_etags.items():
            assert etag is not None, (encoded_key, etag)

    def test_keys_are_deleted(self, storage_provider: StorageProvider, test_id: str):
        key = test_id + "-keys-deleted-test"
        encoded_key = storage_provider.encode_key(key)

        etag = storage_provider.upload_data(encoded_key, None, b"data")
        storage_provider.delete_data(encoded_key, etag)

        cloud_key_list = storage_provider.list_keys_and_etags(encoded_key)
        assert encoded_key not in cloud_key_list

    def test_etags_are_enforced(self, storage_provider: StorageProvider, test_id: str):
        key = test_id + "etags-enforced-test"
        encoded_key = storage_provider.encode_key(key)

        with pytest.raises(KeySyncError):
            storage_provider.upload_data(encoded_key, "etag-when-none-existing", b"data")

        good_etag = storage_provider.upload_data(encoded_key, None, b"0")
        assert good_etag is not None

        with pytest.raises(KeySyncError):
            storage_provider.download_data(encoded_key, "bad-etag")
        with pytest.raises(KeySyncError):
            storage_provider.upload_data(encoded_key, "bad-etag", b"data")
        with pytest.raises(KeySyncError):
            # No etag, not expecting data to overwrite
            storage_provider.upload_data(encoded_key, None, b"data")
        with pytest.raises(KeySyncError):
            storage_provider.delete_data(encoded_key, "bad-etag")

    def test_etags_change_with_same_data(self, storage_provider: StorageProvider, test_id: str):
        key = test_id + "etags-unique-same-data-test"
        encoded_key = storage_provider.encode_key(key)

        first_etag = storage_provider.upload_data(encoded_key, None, b"static-data")
        second_etag = storage_provider.upload_data(encoded_key, first_etag, b"static-data")

        assert first_etag != second_etag
