import pickle
import zlib

import pytest

from cloudmappings.cloudstoragemapping import CloudMapping
from cloudmappings.storageproviders.storageprovider import StorageProvider


class CloudMappingUtilityTests:
    def test_with_serialisers_includes_extras(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping.with_serialisers(
            [lambda i: i],
            [lambda i: i],
            storage_provider=storage_provider,
            sync_initially=False,
        )

        key = test_id + "includes-extras"

        # etags are inherited
        assert cm.etags is not None
        cm[key] = b"0"
        assert key in cm.etags

        # get_blindy is inherited
        assert cm.get_read_blindly() == False
        cm.set_read_blindly(True)
        assert cm.get_read_blindly() == True
        assert cm.get_read_blindly() == cm.d.get_read_blindly()

    def test_with_serialisers_fails_with_uneven_buffers(self, storage_provider: StorageProvider):
        with pytest.raises(ValueError, match="equal number of dumps functions as loads functions"):
            CloudMapping.with_serialisers(
                [lambda i: i, lambda i: i],
                [lambda i: i],
                storage_provider=storage_provider,
                sync_initially=False,
            )

    def test_with_pickle(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping.with_pickle(storage_provider=storage_provider, sync_initially=False)

        key = test_id + "with-pickle"
        data = {"picklable": True, "number": 10.01}

        cm[key] = data
        assert cm[key] == data
        # Manual download and deserialisation:
        assert pickle.loads(storage_provider.download_data(key, cm.etags[key])) == data

    def test_with_json(self, storage_provider: StorageProvider, test_id: str):
        cm = CloudMapping.with_json(storage_provider=storage_provider, sync_initially=False)

        key = test_id + "with-json"
        data = [10, "json-encodable"]
        json = b'[10, "json-encodable"]'

        cm[key] = data
        assert cm[key] == data
        # Manual download:
        assert storage_provider.download_data(key, cm.etags[key]) == json

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
