import json
import pickle
import zlib

import pytest

from cloudmappings.cloudstorage import CloudStorage
from cloudmappings.serialisation import Serialisers


class CloudMappingUtilityTests:
    def test_pickle_is_default(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False)

        assert cm.serialisation == Serialisers.pickle()

    def test_no_serialisation(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False, serialisation=None)

        load_key = test_id + "with-no-serialisation-loads"
        dump_key = test_id + "with-no-serialisation-dumps"
        data = b"binarystring"

        assert cm.serialisation == None
        assert not cm.serialisation

        # Manual upload and serialisation:
        cloud_storage.storage_provider.upload_data(dump_key, None, data)
        cm.sync_with_cloud(dump_key)
        assert cm[dump_key] == data

        # Manual download and deserialisation:
        cm[load_key] = data
        manual_download = cloud_storage.storage_provider.download_data(load_key, cm.etags[load_key])
        assert manual_download == data

        # Test default value
        cm.read_blindly = True
        assert cm[test_id + "empty-key"] is None

    def test_with_pickle(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False, serialisation=Serialisers.pickle())

        load_key = test_id + "with-pickle-loads"
        dump_key = test_id + "with-pickle-dumps"
        data = {"picklable": True, "number": 10.01}

        # Manual upload and serialisation:
        manual_upload = pickle.dumps(data)
        cloud_storage.storage_provider.upload_data(dump_key, None, manual_upload)
        cm.sync_with_cloud(dump_key)
        assert cm[dump_key] == data

        # Manual download and deserialisation:
        cm[load_key] = data
        manual_download = cloud_storage.storage_provider.download_data(load_key, cm.etags[load_key])
        manual_download = pickle.loads(manual_download)
        assert manual_download == data

        # Test default value
        cm.read_blindly = True
        assert cm[test_id + "empty-key"] is None

    def test_with_raw_string(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False, serialisation=Serialisers.raw_string())

        load_key = test_id + "with-raw-string-loads"
        dump_key = test_id + "with-raw-string-dumps"
        data = "a simple string"

        # Manual upload and serialisation:
        manual_upload = bytes(data, encoding="utf-8")
        cloud_storage.storage_provider.upload_data(dump_key, None, manual_upload)
        cm.sync_with_cloud(dump_key)
        assert cm[dump_key] == data

        # Manual download and deserialisation:
        cm[load_key] = data
        manual_download = cloud_storage.storage_provider.download_data(load_key, cm.etags[load_key])
        manual_download = str(manual_download, encoding="utf-8")
        assert manual_download == data

        # Test default value
        cm.read_blindly = True
        assert cm[test_id + "empty-key"] is None

    def test_with_json(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False, serialisation=Serialisers.json())

        load_key = test_id + "with-json-loads"
        dump_key = test_id + "with-json-dumps"
        data = [10, "json-encodable"]

        # Manual upload and serialisation:
        manual_upload = bytes(json.dumps(data, sort_keys=True), encoding="utf-8")
        cloud_storage.storage_provider.upload_data(dump_key, None, manual_upload)
        cm.sync_with_cloud(dump_key)
        assert cm[dump_key] == data

        # Manual download and deserialisation:
        cm[load_key] = data
        manual_download = cloud_storage.storage_provider.download_data(load_key, cm.etags[load_key])
        manual_download = json.loads(str(manual_download, encoding="utf-8"))
        assert manual_download == data

        # Test default value
        cm.read_blindly = True
        assert cm[test_id + "empty-key"] is None

    def test_with_json_zlib(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False, serialisation=Serialisers.json_zlib())

        load_key = test_id + "with-json-zlib-loads"
        dump_key = test_id + "with-json-zlib-dumps"
        data = {"a": True, "b": {}, "c": 3}

        # Manual upload and serialisation:
        manual_upload = zlib.compress(bytes(json.dumps(data, sort_keys=True), encoding="utf-8"))
        cloud_storage.storage_provider.upload_data(dump_key, None, manual_upload)
        cm.sync_with_cloud(dump_key)
        assert cm[dump_key] == data

        # Manual download and deserialisation:
        cm[load_key] = data
        manual_download = cloud_storage.storage_provider.download_data(load_key, cm.etags[load_key])
        manual_download = json.loads(str(zlib.decompress(manual_download), encoding="utf-8"))
        assert manual_download == data

        # Test default value
        cm.read_blindly = True
        assert cm[test_id + "empty-key"] is None

    def test_changing_read_blindly_defaults(self, cloud_storage: CloudStorage, test_id: str):
        cm = cloud_storage.create_mapping(sync_initially=False, read_blindly=True)

        key = test_id + "empty-key"

        assert cm[key] is None

        cm.read_blindly_default = False
        assert cm[key] == False

        cm.read_blindly_default = 0
        assert cm[key] == 0

    def test_key_prefix(self, cloud_storage: CloudStorage, test_id: str):
        key_prefix = "keyprefix/"
        cm_root = cloud_storage.create_mapping(sync_initially=False)
        cm_sub = cloud_storage.create_mapping(sync_initially=False, key_prefix=key_prefix)

        key = test_id + "key-prefix-key"
        key_with_prefix = key_prefix + key

        cm_root[key] = 1
        cm_sub[key] = "sub"  # Won't raise error as no clash

        cm_root.sync_with_cloud(key_with_prefix)
        assert cm_root[key_with_prefix] == "sub"

        cm_root[key_with_prefix] = "root"
        cm_sub.sync_with_cloud(key)
        assert cm_sub[key] == "root"
