import pytest

from cloudmappings.cloudmapping import CloudMapping
from cloudmappings.cloudstorage import CloudStorage
from cloudmappings.errors import KeySyncError


class CloudMappingUtilsTests:
    def test_unknown_key_out_of_sync_errors(self, cloud_storage: CloudStorage, test_prefix: str):
        key = "unknown-out-of-sync"
        cm = cloud_storage.create_mapping(key_prefix=f"{test_prefix}/")

        # Upload some data the mapping doesnt know about to get out of sync
        cloud_storage.storage_provider.upload_data(
            cloud_storage.storage_provider.encode_key(f"{test_prefix}/{key}"), None, b"new-data!"
        )
        assert key not in cm

        # Cloudmapping doesn't know the key exists, so KeyError on get and delete, and KeySyncError on set:
        with pytest.raises(KeyError):
            cm[key]
        with pytest.raises(KeySyncError):
            cm[key] = b"session_2"
        with pytest.raises(KeyError):
            del cm[key]

    def test_known_key_out_of_sync_errors(self, cloud_storage: CloudStorage, test_prefix: str):
        key = "known-out-of-sync"
        cm = cloud_storage.create_mapping(key_prefix=f"{test_prefix}/")

        # The cloudmapping sets some initial state
        cm[key] = "initial"
        # Upload some data to get out of sync
        cloud_storage.storage_provider.upload_data(
            cloud_storage.storage_provider.encode_key(f"{test_prefix}/{key}"), cm.etags[key], b"new-data!"
        )

        assert key in cm
        # Cloudmapping now gets a KeySyncError on read, delete, and set:
        with pytest.raises(KeySyncError):
            cm[key]
        with pytest.raises(KeySyncError):
            cm[key] = b"session_1"
        with pytest.raises(KeySyncError):
            del cm[key]

    def test_sync_with_cloud(self, cloud_storage: CloudStorage, test_prefix: str):
        prefix = f"{test_prefix}/sync-with-cloud"
        cm = cloud_storage.create_mapping(sync_initially=False)

        assert len(cm) == 0

        # Upload some data to find with a sync
        cloud_storage.storage_provider.upload_data(
            cloud_storage.storage_provider.encode_key(f"{prefix}/two/one"), None, b"1"
        )
        cloud_storage.storage_provider.upload_data(
            cloud_storage.storage_provider.encode_key(f"{prefix}/two"), None, b"2"
        )

        cm.sync_with_cloud(f"{prefix}/two/")
        assert len(cm) == 1
        assert f"{prefix}/two/one" in cm

        cm.sync_with_cloud(prefix)
        assert len(cm) == 2
        assert f"{prefix}/two/one" in cm
        assert f"{prefix}/two" in cm

    def test_key_prefix_hierarchy(self, cloud_storage: CloudStorage, test_prefix: str):
        cm_root = cloud_storage.create_mapping(sync_initially=False, key_prefix=f"{test_prefix}/")
        cm_sub = cloud_storage.create_mapping(sync_initially=False, key_prefix=f"{test_prefix}/child_prefix/")

        key_from_sub = "hierarchy"
        key_from_root = "child_prefix/hierarchy"

        cm_root[key_from_sub] = 1
        # Set from subdirectory
        cm_sub[key_from_sub] = "sub"  # Won't raise error as different key

        # Get from root
        cm_root.sync_with_cloud(key_from_root)
        assert cm_root[key_from_root] == "sub"

        # Override from root and get from subdirectory
        cm_root[key_from_root] = "root"
        cm_sub.sync_with_cloud(key_from_sub)
        assert cm_sub[key_from_sub] == "root"

    def test_key_prefix_isolated(self, cloud_storage: CloudStorage, test_prefix: str):
        same_key = "key"
        cm_1 = cloud_storage.create_mapping(key_prefix=f"{test_prefix}/prefix-isolated-1/")
        cm_2 = cloud_storage.create_mapping(key_prefix=f"{test_prefix}/prefix-isolated-2/")

        assert len(cm_1) == 0
        assert len(cm_2) == 0

        cm_1[same_key] = 1

        assert len(cm_1) == 1
        assert len(cm_2) == 0

        cm_1.sync_with_cloud()
        cm_2.sync_with_cloud()

        assert len(cm_1) == 1
        assert len(cm_2) == 0

        cm_2[same_key] = 2

        assert len(cm_1) == 1
        assert len(cm_2) == 1

        cm_1.sync_with_cloud()
        cm_2.sync_with_cloud()

        assert len(cm_1) == 1
        assert len(cm_2) == 1

        assert cm_1[same_key] == 1
        assert cm_2[same_key] == 2

    def test_repr(self, cloud_mapping: CloudMapping):
        _repr = str(cloud_mapping)

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

    def test_read_blindy_get(self, cloud_storage: CloudStorage, test_prefix: str):
        key = f"{test_prefix}/read-blindly-get"
        # Upload some data to read blindly
        cloud_storage.storage_provider.upload_data(cloud_storage.storage_provider.encode_key(key), None, b"blind")

        cm = cloud_storage.create_mapping(sync_initially=False, serialisation=None)
        cm.read_blindly = True
        assert cm[key] == b"blind"

    def test_read_blindy_contains(self, cloud_storage: CloudStorage, test_prefix: str):
        key = f"{test_prefix}/read-blindly-contains"
        # Upload some data to read blindly
        cloud_storage.storage_provider.upload_data(cloud_storage.storage_provider.encode_key(key), None, b"blind")

        cm = cloud_storage.create_mapping(sync_initially=False)
        cm.read_blindly = True
        assert key in cm

    def test_read_blindly_error(self, cloud_mapping: CloudMapping):
        cloud_mapping.read_blindly = True
        cloud_mapping.read_blindly_error = True
        with pytest.raises(KeyError):
            cloud_mapping["key-not-real"]

    def test_read_blindly_default(self, cloud_mapping: CloudMapping):
        cloud_mapping.read_blindly = True
        cloud_mapping.read_blindly_error = False
        cloud_mapping.read_blindly_default = 100
        assert cloud_mapping["doesn't-exist"] == 100
