import pytest

from cloudmappings.cloudstorage import CloudStorage
from cloudmappings.errors import KeySyncError


class ConcurrentCloudMappingTests:
    def test_no_ownership_error(self, cloud_storage: CloudStorage, test_id: str):
        sess_1 = cloud_storage.create_mapping(sync_initially=False, serialisation=None)
        sess_2 = cloud_storage.create_mapping(sync_initially=False, serialisation=None)
        key = test_id + "/concurrent/no-ownership-test"

        # Session 1 takes ownership of key:
        sess_1[key] = b"session_1"
        # Session 2 doesn't know the key exists, so KeyError on get and delete, and KeySyncError on set:
        with pytest.raises(KeyError):
            sess_2[key]
        with pytest.raises(KeySyncError):
            sess_2[key] = b"session_2"
        with pytest.raises(KeyError):
            del sess_2[key]

        # Session 3 is created after the key is created
        sess_3 = cloud_storage.create_mapping()
        # Session 1 updates the key:
        sess_1[key] = b"session_1"
        # Session 3 knows the key exists, but is out of sync so KeySyncError on get, set and delete:
        with pytest.raises(KeySyncError):
            sess_3[key]
        with pytest.raises(KeySyncError):
            sess_3[key] = b"session_2"
        with pytest.raises(KeySyncError):
            del sess_3[key]

    def test_blind_read(self, cloud_storage: CloudStorage, test_id: str):
        sess_1 = cloud_storage.create_mapping(sync_initially=False, serialisation=None)
        sess_2 = cloud_storage.create_mapping(sync_initially=False, read_blindly=True, serialisation=None)
        sess_3 = cloud_storage.create_mapping(sync_initially=False, read_blindly=False, serialisation=None)
        key = test_id + "/concurrent/blind-get-test"

        # Session 1 uploads data:
        sess_1[key] = b"data"

        # Session 2 blindly gets by default, but can be turned off:
        assert sess_2[key] == b"data"
        sess_2.read_blindly = False
        with pytest.raises(KeyError):
            sess_2[key]

        # Session 3 dones't blindly get by default, but can be set to:
        with pytest.raises(KeyError):
            sess_3[key]
        sess_3.read_blindly = True
        assert sess_3[key] == b"data"

    def test_manual_change_error(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False, serialisation=None)
        key = test_id + "/concurrent/manual-change-test"
        encoded_key = cloud_storage.storage_provider.encode_key(key)

        # Session 1 takes ownership of key:
        cm[key] = b"session_1"
        # The blob is changed by some manual means:
        cloud_storage.storage_provider.upload_data(encoded_key, cm.etags[key], b"other")
        # Session 1 now gets a KeySyncError on read, delete, and set:
        with pytest.raises(KeySyncError):
            cm[key]
        with pytest.raises(KeySyncError):
            cm[key] = b"session_1"
        with pytest.raises(KeySyncError):
            del cm[key]

    def test_resync_all(self, cloud_storage: CloudStorage, test_id: str):
        sess_1 = cloud_storage.create_mapping(sync_initially=False, serialisation=None)
        key_1 = test_id + "/concurrent/resync-all-test-1"
        key_2 = test_id + "/concurrent/resync-all-test-2"
        key_3 = test_id + "/concurrent/resync-all-test-3"
        encoded_key_1 = cloud_storage.storage_provider.encode_key(key_1)

        # Session 1 takes ownership of keys
        sess_1[key_1] = b"session_1"
        sess_1[key_2] = b"session_1"
        sess_1[key_3] = b"session_1"
        # Session 1 only knows of these 3 keys:
        assert len(sess_1) == 3
        # Key 1 is changed by some manual means:
        cloud_storage.storage_provider.upload_data(encoded_key_1, sess_1.etags[key_1], b"other")
        # Session 2 is created after the data changed:
        sess_2 = cloud_storage.create_mapping(serialisation=None)
        # Session 2 knows of all the keys in the cloud:
        assert len(sess_2) >= 3
        # Session 2 can read the keys
        sess_2[key_1]
        sess_2[key_2]
        sess_2[key_3]
        # If Session 1 syncs all keys, it can now read them again:
        sess_1.sync_with_cloud()
        sess_1[key_1]
        sess_1[key_2]
        sess_1[key_3]

    def test_resync_specific(self, cloud_storage: CloudStorage, test_id: str):
        sess_1 = cloud_storage.create_mapping(sync_initially=False, serialisation=None)
        sess_2 = cloud_storage.create_mapping(sync_initially=False, serialisation=None)
        key_1 = test_id + "/concurrent/resync-specific-test-1"
        key_2 = test_id + "/concurrent/resync-specific-test-2"
        key_3 = test_id + "/concurrent/resync-specific-test-3"
        encoded_key_1 = cloud_storage.storage_provider.encode_key(key_1)

        # Session 1 takes ownership of keys
        sess_1[key_1] = b"session_1"
        sess_1[key_2] = b"session_1"
        sess_1[key_3] = b"session_1"
        # Only session 1  knows of these 3 keys:
        assert len(sess_1) == 3
        assert len(sess_2) == 0
        # Key 1 is changed by some manual means:
        cloud_storage.storage_provider.upload_data(encoded_key_1, sess_1.etags[key_1], b"other")
        # Session 2 syncs key 1 only:
        sess_2.sync_with_cloud(key_1)
        # Session 2 can now read key 1
        sess_2[key_1]
        # Session 2 only knows this one key
        assert len(sess_2) == 1

    def test_resync_pass_ownership(self, cloud_storage: CloudStorage, test_id: str):
        sess_1 = cloud_storage.create_mapping(serialisation=None)
        sess_2 = cloud_storage.create_mapping(serialisation=None)
        key = test_id + "/concurrent/resync-pass-ownership-test"

        # Session 1 takes ownership of key
        sess_1[key] = b"session_1"
        # Session 2 syncs and can now change blob value:
        sess_2.sync_with_cloud(key)
        sess_2[key] = b"session_2"
        # Session 1 has lost ownership
        with pytest.raises(KeySyncError):
            sess_1[key] = b"session_1"
