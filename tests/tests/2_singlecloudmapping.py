import pytest

from cloudmappings.cloudstoragemapping import CloudMapping
from cloudmappings.storageproviders.storageprovider import StorageProvider


class SingleCloudMappingTests:
    def test_initialising_without_sync(self, storage_provider: StorageProvider):
        CloudMapping(storage_provider=storage_provider, sync_initially=False)

    def test_initialising_with_sync(self, storage_provider: StorageProvider):
        CloudMapping(storage_provider=storage_provider, sync_initially=True)

    def test_repr(self, storage_provider: StorageProvider):
        cm = CloudMapping(storage_provider=storage_provider, sync_initially=False)

        _repr = str(cm)

        assert "CloudStorageProvider=" in _repr

        if "AzureBlob" in _repr:
            assert "StorageAccountName=" in _repr
            assert "ContainerName=" in _repr
        elif "AzureTable" in _repr:
            assert "StorageAccountName=" in _repr
            assert "TableName=" in _repr
        elif "GoogleCloudStorage" in _repr:
            assert "Project=" in _repr
            assert "BucketName=" in _repr
        elif "AWSS3" in _repr:
            assert "BucketName=" in _repr
        else:
            pytest.fail("Unknown provider repr")

    def test_no_key_errors(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping(storage_provider=storage_provider, sync_initially=False)
        key = test_id + "/no-key-errors-test"

        with pytest.raises(KeyError):
            cm[key]
        with pytest.raises(KeyError):
            del cm[key]
        assert key not in cm

    def test_basic_setting_and_getting(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping(storage_provider=storage_provider, sync_initially=False)

        cm[test_id + "-key-A"] = b"100"
        cm[test_id + "-key-a"] = b"uncapitalised"
        cm[test_id + "-key-3"] = b"three"

        assert cm[test_id + "-key-A"] == b"100"
        assert cm[test_id + "-key-a"] == b"uncapitalised"
        assert cm[test_id + "-key-3"] == b"three"

    def test_read_blindly_defaults_none(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping(storage_provider=storage_provider, sync_initially=False)
        key = test_id + "/read-blindly-test"

        # CloudMappings default to not getting blindly:
        assert not cm.read_blindly
        # If get_blindly, values default to None
        cm.read_blindly = True
        assert cm[key] is None

    def test_complex_keys(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping(storage_provider=storage_provider, sync_initially=False)
        key1 = test_id + "/here/are/some/sub/dirs"
        key2 = test_id + "/how.about_some ??????????????????????unusual!@#$%^*characters"

        cm[key1] = b"0"
        cm[key2] = b"1"

        assert cm[key1] == b"0"
        assert cm[key2] == b"1"

    def test_deleting_keys(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping(storage_provider=storage_provider, sync_initially=False)
        key = test_id + "/delete-test"

        cm[key] = b"0"
        del cm[key]
        with pytest.raises(KeyError):
            cm[key]

    def test_contains(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping(storage_provider=storage_provider, sync_initially=False)
        key = test_id + "/contains-test"

        assert key not in cm

        cm[key] = b"0"
        assert key in cm

    def test_length(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping(storage_provider=storage_provider, sync_initially=False)
        key_1 = test_id + "/length-test/1"
        key_2 = test_id + "/length-test/2"
        key_3 = test_id + "/length-test/3"

        assert len(cm) == 0

        cm[key_1] = b"a"
        cm[key_2] = b"b"
        assert len(cm) == 2

        cm[key_3] = b"c"
        assert len(cm) == 3
