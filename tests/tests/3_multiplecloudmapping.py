import pytest

from cloudmappings.cloudstoragemapping import CloudMapping
from cloudmappings.errors import KeySyncError
from cloudmappings.storageproviders.storageprovider import StorageProvider


class ConcurrentCloudMappingTests:
    def test_no_ownership_error(self, storage_provider: StorageProvider, test_id: str):
        sess_1 = CloudMapping(storageprovider=storage_provider)
        sess_2 = CloudMapping(storageprovider=storage_provider)
        key = test_id + "/concurrent/no-ownership-test"

        # Session 1 takes ownership of key:
        sess_1[key] = b"session_1"
        # Session 2 is hasn't seen the key before, so KeySyncError when setting:
        with pytest.raises(KeySyncError):
            sess_2[key] = b"session_2"

    def test_manual_change_error(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping(storageprovider=storage_provider)
        key = test_id + "/concurrent/manual-change-test"

        # Session 1 takes ownership of key:
        cm[key] = b"session_1"
        # The blob is changed by some manual means:
        storage_provider.upload_data(key, cm.etags[key], b"other")
        # Session 1 now gets a KeySyncError on read, delete, and set:
        with pytest.raises(KeySyncError):
            cm[key]
        with pytest.raises(KeySyncError):
            cm[key] = b"session_1"
        with pytest.raises(KeySyncError):
            del cm[key]

    def test_resync_all(self, storage_provider: StorageProvider, test_id: str):
        sess_1 = CloudMapping(storageprovider=storage_provider)
        sess_2 = CloudMapping(storageprovider=storage_provider)
        key = test_id + "/concurrent/resync-all-test"

        # Session 1 takes ownership of key
        sess_1[key] = b"session_1"
        # The blob is changed by some manual means:
        storage_provider.upload_data(key, sess_1.etags[key], b"other")
        # If sessions syncs all keys, they can now both read:
        sess_1.sync_with_cloud()
        sess_2.sync_with_cloud()
        sess_1[key]
        sess_2[key]

    def test_resync_specific(self, storage_provider: StorageProvider, test_id: str):
        sess_1 = CloudMapping(storageprovider=storage_provider)
        sess_2 = CloudMapping(storageprovider=storage_provider)
        key = test_id + "/concurrent/resync-specific-test"

        # Session 1 takes ownership of key
        sess_1[key] = b"session_1"
        # The blob is changed by some manual means:
        storage_provider.upload_data(key, sess_1.etags[key], b"other")
        # If sessions syncs all keys, they can now both read:
        sess_1.sync_with_cloud(key)
        sess_2.sync_with_cloud(key)
        sess_1[key]
        sess_2[key]

    def test_resync_pass_ownership(self, storage_provider: StorageProvider, test_id: str):
        sess_1 = CloudMapping(storageprovider=storage_provider)
        sess_2 = CloudMapping(storageprovider=storage_provider)
        key = test_id + "/concurrent/resync-pass-ownership-test"

        # Session 1 takes ownership of key
        sess_1[key] = b"session_1"
        # Session 2 syncs and can now change blob value:
        sess_2.sync_with_cloud(key)
        sess_2[key] = b"session_2"
        # Session 1 has lost ownership
        with pytest.raises(KeySyncError):
            sess_1[key] = b"session_1"
