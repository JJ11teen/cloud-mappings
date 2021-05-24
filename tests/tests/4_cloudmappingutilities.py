import pytest
import pickle
import zlib

from cloudmappings.cloudstoragemapping import CloudMapping
from cloudmappings.errors import KeySyncError
from cloudmappings.storageproviders.storageprovider import StorageProvider


class CloudMappingUtilityTests:
    def test_with_buffers_fails_with_uneven_buffers(self, storage_provider: StorageProvider, test_id: str):
        with pytest.raises(ValueError, match="equal number of input buffers as output buffers"):
            CloudMapping.with_buffers(
                [lambda i: i, lambda i: i],
                [lambda i: i],
                storageprovider=storage_provider,
                sync_initially=False,
            )

    def test_with_pickle(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping.with_pickle(storageprovider=storage_provider, sync_initially=False)

        key = test_id + "with-pickle"
        data = {"picklable": True, "number": 10.01}

        cm[key] = data
        assert cm[key] == data
        # Manual download and deserialisation:
        assert pickle.loads(storage_provider.download_data(key, cm.etags[key])) == data

    def test_with_json(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping.with_json(storageprovider=storage_provider, sync_initially=False)

        key = test_id + "with-json"
        data = [10, "json-encodable"]
        json = b'[10, "json-encodable"]'

        cm[key] = data
        assert cm[key] == data
        # Manual download:
        assert storage_provider.download_data(key, cm.etags[key]) == json

    def test_with_compressed_json(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping.with_json_zlib(storageprovider=storage_provider, sync_initially=False)

        key = test_id + "with-compressed-json"
        data = {"a": True, "b": {}, "c": 3}
        json = b'{"a": true, "b": {}, "c": 3}'

        cm[key] = data
        assert cm[key] == data
        # Manual download:
        raw_bytes = storage_provider.download_data(key, cm.etags[key])
        assert zlib.decompress(raw_bytes) == json
