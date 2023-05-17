import pytest

from cloudmappings.cloudmapping import CloudMapping
from cloudmappings.cloudstorage import CloudStorage
from cloudmappings.errors import KeySyncError


class CloudMappingConcurrentTests:
    def test_no_ownership_error(self, cloud_mapping: CloudMapping, cloud_mapping_two: CloudMapping):
        key = "concurrent/no-ownership-test"

        # Session 1 takes ownership of key:
        cloud_mapping[key] = b"session_1"
        # Session 2 doesn't know the key exists, so KeyError on get and delete, and KeySyncError on set:
        with pytest.raises(KeyError):
            cloud_mapping_two[key]
        with pytest.raises(KeySyncError):
            cloud_mapping_two[key] = b"session_2"
        with pytest.raises(KeyError):
            del cloud_mapping_two[key]

        # Session 2 is synced
        cloud_mapping_two.sync_with_cloud()
        # Session 1 updates the key:
        cloud_mapping[key] = b"session_1"
        # Session 2 knows the key exists, but is out of sync so KeySyncError on get, set and delete:
        with pytest.raises(KeySyncError):
            cloud_mapping_two[key]
        with pytest.raises(KeySyncError):
            cloud_mapping_two[key] = b"session_2"
        with pytest.raises(KeySyncError):
            del cloud_mapping_two[key]

    def test_blind_read(self, cloud_mapping: CloudMapping, cloud_mapping_two: CloudMapping):
        key = "concurrent/blind-get-test"

        # Session 1 uploads data:
        cloud_mapping[key] = b"data"

        # Session 2 can read blindly
        cloud_mapping_two.read_blindly = True
        assert cloud_mapping_two[key] == b"data"

        # Session 2 can turn off read blindly
        cloud_mapping_two.read_blindly = False
        with pytest.raises(KeyError):
            cloud_mapping_two[key]

    def test_resync_pass_ownership(self, cloud_mapping: CloudMapping, cloud_mapping_two: CloudMapping):
        key = "concurrent/resync-pass-ownership-test"

        # Session 1 takes ownership of key
        cloud_mapping[key] = b"session_1"
        # Session 2 syncs and can now change blob value:
        cloud_mapping_two.sync_with_cloud(key)
        cloud_mapping_two[key] = b"session_2"
        # Session 1 has lost ownership
        with pytest.raises(KeySyncError):
            cloud_mapping[key] = b"session_1"

    def test_resync_all(self, cloud_mapping: CloudMapping, cloud_mapping_two: CloudMapping):
        key_1 = "concurrent/resync-all-1"
        key_2 = "concurrent/resync-all-2"

        # Session 1 takes ownership of keys
        cloud_mapping[key_1] = b"session_1"
        cloud_mapping[key_2] = b"session_1"
        # Session 1 only knows of these 2 keys:
        assert len(cloud_mapping) == 2
        assert len(cloud_mapping_two) == 0

        # Session 2 is synced
        cloud_mapping_two.sync_with_cloud()
        # Session 2 now knows of all the keys in the cloud:
        assert len(cloud_mapping_two) == 2
        # Session 2 can read the keys
        cloud_mapping_two[key_1]
        cloud_mapping_two[key_2]

        # Session 2 overwrites one key
        cloud_mapping_two[key_1] = "session_2"
        # Session 1 can no longer read this key
        with pytest.raises(KeySyncError):
            cloud_mapping[key_1]
        # However session 1 can still read the other keys
        cloud_mapping[key_2]

        # If Session 1 syncs all keys, it can now read them again:
        cloud_mapping.sync_with_cloud()
        cloud_mapping[key_1]
        cloud_mapping[key_2]

    def test_resync_specific(self, cloud_mapping: CloudMapping, cloud_mapping_two: CloudMapping):
        key_1 = "concurrent/resync-single-1"
        key_2 = "concurrent/resync-single-2"

        # Session 1 takes ownership of keys
        cloud_mapping[key_1] = b"session_1"
        cloud_mapping[key_2] = b"session_1"
        # Session 1 only knows of these 2 keys:
        assert len(cloud_mapping) == 2
        assert len(cloud_mapping_two) == 0

        # Session 2 is synced only with key 2
        cloud_mapping_two.sync_with_cloud(key_2)
        # Session 2 now knows only about key 2 in the cloud:
        assert len(cloud_mapping_two) == 1
        # Session 2 can read the keys
        with pytest.raises(KeyError):
            cloud_mapping_two[key_1]
        cloud_mapping_two[key_2]

        # Session 2 overwrites the key
        cloud_mapping_two[key_2] = "session_2"
        # Session 1 can no longer read this key
        with pytest.raises(KeySyncError):
            cloud_mapping[key_2]
        # However session 1 can still read the other keys
        cloud_mapping[key_1]

        # If Session 1 syncs the key, it can now read them again:
        cloud_mapping.sync_with_cloud(key_2)
        cloud_mapping[key_1]
        cloud_mapping[key_2]
