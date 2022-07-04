import pickle
import zlib

import pytest

from cloudmappings.cloudstoragemapping import CloudMapping
from cloudmappings.storageproviders.storageprovider import StorageProvider


class CloudMappingUtilityTests:
    def test_with_pickle(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping.with_pickle(storage_provider=storage_provider, sync_initially=False)

        key = test_id + "with-pickle"
        data = {"picklable": True, "number": 10.01}

        cm[key] = data
        assert cm[key] == data
        # Manual download and deserialisation:
        assert pickle.loads(storage_provider.download_data(key, cm.etags[key])) == data

        # Test default value
        cm.read_blindly = True
        assert cm[test_id + "empty-key"] is None

    def test_with_json(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping.with_json(storage_provider=storage_provider, sync_initially=False)

        key = test_id + "with-json"
        data = [10, "json-encodable"]
        json = b'[10, "json-encodable"]'

        cm[key] = data
        assert cm[key] == data
        # Manual download:
        assert storage_provider.download_data(key, cm.etags[key]) == json

        # Test default value
        cm.read_blindly = True
        assert cm[test_id + "empty-key"] is None

    def test_with_compressed_json(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping.with_json_zlib(storage_provider=storage_provider, sync_initially=False)

        key = test_id + "with-compressed-json"
        data = {"a": True, "b": {}, "c": 3}
        json = b'{"a": true, "b": {}, "c": 3}'

        cm[key] = data
        assert cm[key] == data
        # Manual download:
        raw_bytes = storage_provider.download_data(key, cm.etags[key])
        assert zlib.decompress(raw_bytes) == json

        # Test default value
        cm.read_blindly = True
        assert cm[test_id + "empty-key"] is None

    def test_changing_read_blindly_defaults(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping.with_pickle(storage_provider=storage_provider, sync_initially=False, read_blindly=True)

        key = test_id + "empty-key"

        assert cm[key] is None

        cm.read_blindly_default = False
        assert cm[key] == False

        cm.read_blindly_default = 0
        assert cm[key] == 0
