import pytest

from cloudmappings.errors import KeySyncError
from cloudmappings.storageprovider import StorageProvider


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
        assert storage_provider.download_data(encoded_key, etag) == b"data"

    def test_non_byte_values_error(self, storage_provider: StorageProvider, test_id: str):
        key = test_id + "-non-bytes-error"
        encoded_key = storage_provider.encode_key(key)

        with pytest.raises(ValueError, match="must be bytes like"):
            storage_provider.upload_data(encoded_key, None, True)
        with pytest.raises(ValueError, match="must be bytes like"):
            storage_provider.upload_data(encoded_key, None, 10)
        with pytest.raises(ValueError, match="must be bytes like"):
            storage_provider.upload_data(encoded_key, None, "string-data")
        with pytest.raises(ValueError, match="must be bytes like"):
            storage_provider.upload_data(encoded_key, None, [0, 1, 0, 1])
        with pytest.raises(ValueError, match="must be bytes like"):
            storage_provider.upload_data(encoded_key, None, {"or": "something more", "elaborate": True})
        with pytest.raises(ValueError, match="must be bytes like"):
            storage_provider.upload_data(encoded_key, None, None)

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

    def test_keys_and_etags_are_listed_with_prefix(self, storage_provider: StorageProvider, test_id: str):
        root = test_id + "-keys-and-etags-list-prefix/"
        level_1 = root + "directory/"
        level_2 = level_1 + "subdirectory/"
        key = "key"

        root_etag = storage_provider.upload_data(root + key, None, b"data")
        level_1_etag = storage_provider.upload_data(level_1 + key, None, b"data")
        level_2_etag = storage_provider.upload_data(level_2 + key, None, b"data")

        # Check root lists all keys
        keys_and_etags = storage_provider.list_keys_and_etags(root)
        assert len(keys_and_etags) == 3
        assert keys_and_etags[root + key] == root_etag
        assert keys_and_etags[level_1 + key] == level_1_etag
        assert keys_and_etags[level_2 + key] == level_2_etag

        # Check level 1 lists level 1 and level 2, but not root
        keys_and_etags = storage_provider.list_keys_and_etags(level_1)
        assert len(keys_and_etags) == 2
        assert root + key not in keys_and_etags
        assert keys_and_etags[level_1 + key] == level_1_etag
        assert keys_and_etags[level_2 + key] == level_2_etag

        # Check level 2 only contains level 2 prefixed keys
        keys_and_etags = storage_provider.list_keys_and_etags(level_2)
        assert len(keys_and_etags) == 1
        assert root + key not in keys_and_etags
        assert level_1 + key not in keys_and_etags
        assert keys_and_etags[level_2 + key] == level_2_etag

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

    def test_download_with_no_etag(self, storage_provider: StorageProvider, test_id: str):
        key = test_id + "-download-with-no-etag"
        encoded_key = storage_provider.encode_key(key)

        # Assert no value returns None
        assert storage_provider.download_data(encoded_key, None) is None
        # Assert returns latest if value
        storage_provider.upload_data(encoded_key, None, b"data")
        assert storage_provider.download_data(encoded_key, None) == b"data"
