import pytest

from cloudmappings.cloudstorage import CloudStorage


class SingleCloudMappingTests:
    def test_initialising_without_sync(self, cloud_storage: CloudStorage):
        cloud_storage.create_mapping(sync_initially=False)

    def test_initialising_with_sync(self, cloud_storage: CloudStorage):
        cloud_storage.create_mapping(sync_initially=True)

    def test_repr(self, cloud_storage: CloudStorage):
        cm = cloud_storage.create_mapping(sync_initially=False)

        _repr = str(cm)

        assert _repr.startswith("cloudmapping<CloudStorageProvider=")

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

    def test_no_key_errors(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False)
        key = test_id + "/no-key-errors-test"

        with pytest.raises(KeyError):
            cm[key]
        with pytest.raises(KeyError):
            del cm[key]
        assert key not in cm

    def test_basic_setting_and_getting(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False)

        cm[test_id + "-key-A"] = b"100"
        cm[test_id + "-key-a"] = b"uncapitalised"
        cm[test_id + "-key-3"] = b"three"

        assert cm[test_id + "-key-A"] == b"100"
        assert cm[test_id + "-key-a"] == b"uncapitalised"
        assert cm[test_id + "-key-3"] == b"three"

    def test_read_blindy(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False)
        key = test_id + "/read-blindly-test"

        assert not cm.read_blindly
        with pytest.raises(KeyError):
            cm[key]

        cm.read_blindly = True
        assert cm[key] is None

    def test_read_blindly_error(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False, read_blindly=True)
        key = test_id + "/read-blindly-error-test"

        assert not cm.read_blindly_error
        assert cm[key] is None

        cm.read_blindly_error = True
        with pytest.raises(KeyError):
            cm[key]

    def test_read_blindly_default(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False, read_blindly=True)
        key = test_id + "/read-blindly-default-test"

        assert cm.read_blindly_default is None
        assert cm[key] is None

        cm.read_blindly_default = 10
        assert cm[key] == 10

    def test_complex_keys(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False)
        key1 = test_id + "/here/are/some/sub/dirs"
        key2 = test_id + "/how.about_some ˆøœ¨åß∆∫ı˜unusual!@#$%^*characters"

        cm[key1] = b"0"
        cm[key2] = b"1"

        assert cm[key1] == b"0"
        assert cm[key2] == b"1"

    def test_deleting_keys(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False)
        key = test_id + "/delete-test"

        cm[key] = b"0"
        del cm[key]
        with pytest.raises(KeyError):
            cm[key]

    def test_contains(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False)
        key = test_id + "/contains-test"

        assert key not in cm

        cm[key] = b"0"
        assert key in cm

    def test_length(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False)
        key_1 = test_id + "/length-test/1"
        key_2 = test_id + "/length-test/2"
        key_3 = test_id + "/length-test/3"

        assert len(cm) == 0

        cm[key_1] = b"a"
        cm[key_2] = b"b"
        assert len(cm) == 2

        cm[key_3] = b"c"
        assert len(cm) == 3

    def test_subdir_sync(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False)
        prefix = test_id + "/subdir-sync-test"
        key = prefix + "/filename"

        assert prefix not in cm
        assert key not in cm
        assert len(cm) == 0

        cm.sync_with_cloud(prefix)
        assert prefix not in cm
        assert key not in cm
        assert len(cm) == 0

        cm[key] = b"data"
        assert prefix not in cm
        assert key in cm
        assert len(cm) == 1

        cm.sync_with_cloud(prefix)
        assert prefix not in cm
        assert key in cm
        assert len(cm) == 1

    def test_mapping_prefix_sync(self, cloud_storage: CloudStorage, test_id: str):
        prefix_1 = test_id + "/mapping-prefix-test-1/"
        prefix_2 = test_id + "/mapping-prefix-test-2/"

        # TODO: simplify all tests by using key_prefixes instead of not syncing initially:
        cm_1 = cloud_storage.create_mapping(key_prefix=prefix_1)
        cm_2 = cloud_storage.create_mapping(key_prefix=prefix_2)

        assert len(cm_1) == 0
        assert len(cm_2) == 0

        cm_1["k1"] = 1

        assert len(cm_1) == 1
        assert len(cm_2) == 0

        cm_1.sync_with_cloud()
        cm_2.sync_with_cloud()

        assert len(cm_1) == 1
        assert len(cm_2) == 0

        cm_2["k1"] = 2

        assert len(cm_1) == 1
        assert len(cm_2) == 1

        cm_1.sync_with_cloud()
        cm_2.sync_with_cloud()

        assert len(cm_1) == 1
        assert len(cm_2) == 1

        assert cm_1["k1"] == 1
        assert cm_2["k1"] == 2
