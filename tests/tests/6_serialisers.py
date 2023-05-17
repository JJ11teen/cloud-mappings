import json
import pickle
import zlib

import pytest
from pytest_mock import MockFixture

import cloudmappings.serialisers.core as DefaultSerialisers
from cloudmappings.cloudstorage import CloudStorage
from cloudmappings.serialisers.serialisation import CloudMappingSerialisation


class CloudMappingUtilityTests:
    def test_serialiser_is_called(self, mocker: MockFixture, cloud_storage: CloudStorage, test_prefix: str):
        class Dummy:
            @staticmethod
            def dumps(value: str) -> bytes:
                return b"byte-data"

            @staticmethod
            def loads(value: bytes) -> str:
                return "string-data"

        dummy = Dummy()
        dumps_spy = mocker.spy(dummy, "dumps")
        loads_spy = mocker.spy(dummy, "loads")

        cm = cloud_storage.create_mapping(
            serialisation=CloudMappingSerialisation(dumps=dummy.dumps, loads=dummy.loads), key_prefix=f"{test_prefix}/"
        )

        # Write something, should call dumps
        cm["key"] = "some data"
        dumps_spy.assert_called_once_with("some data")

        # Read something, should call str
        cm["key"]
        loads_spy.assert_called_once_with(b"byte-data")

    def test_no_serialisation(self, cloud_storage: CloudStorage, test_prefix: str):
        cm = cloud_storage.create_mapping(sync_initially=False, serialisation=None)

        load_key = f"{test_prefix}/with-no-serialisation-loads"
        dump_key = f"{test_prefix}/with-no-serialisation-dumps"
        data = b"binarystring"

        assert cm.serialisation == None
        assert not cm.serialisation

        # Manual upload and serialisation:
        cloud_storage.storage_provider.upload_data(cloud_storage.storage_provider.encode_key(dump_key), None, data)
        cm.sync_with_cloud(dump_key)
        assert cm[dump_key] == data

        # Manual download and deserialisation:
        cm[load_key] = data
        manual_download = cloud_storage.storage_provider.download_data(
            cloud_storage.storage_provider.encode_key(load_key), cm.etags[load_key]
        )
        assert manual_download == data

        # Test default value
        cm.read_blindly = True
        assert cm[f"{test_prefix}/empty-key"] is None

        # Test no-bytes value raises
        with pytest.raises(ValueError, match="Data must be bytes like"):
            cm[dump_key] = "a string!"

    def test_serialiser_chaining(self, mocker: MockFixture):
        class Dummy:
            @staticmethod
            def dumps_first(value) -> str:
                return "dumpsfirst"

            @staticmethod
            def dumps_second(value) -> str:
                return "dumpssecond"

            @staticmethod
            def loads_first(value) -> str:
                return "loadsfirst"

            @staticmethod
            def loads_second(value) -> str:
                return "loadssecond"

        dummy = Dummy()
        dumps_1_spy = mocker.spy(dummy, "dumps_first")
        dumps_2_spy = mocker.spy(dummy, "dumps_second")
        loads_1_spy = mocker.spy(dummy, "loads_first")
        loads_2_spy = mocker.spy(dummy, "loads_second")

        serialiser = CloudMappingSerialisation.from_chain(
            ordered_dumps_funcs=[dummy.dumps_first, dummy.dumps_second],
            ordered_loads_funcs=[dummy.loads_first, dummy.loads_second],
        )

        assert serialiser.dumps("test") == "dumpsecond"
        dumps_1_spy.assert_called_once_with("test")
        dumps_2_spy.assert_called_once_with("dumpsfirst")

        assert serialiser.loads("test") == "loadssecond"
        loads_1_spy.assert_called_once_with("test")
        loads_2_spy.assert_called_once_with("loadsfirst")

    def test_with_pickle(self):
        serialiser = DefaultSerialisers.pickle()

        data = {"picklable": True, "number": 10.01}
        pickled = pickle.dumps(data)

        assert serialiser.dumps(data) == pickled
        assert serialiser.loads(pickled) == data

    def test_with_raw_string(self):
        serialiser = DefaultSerialisers.raw_string()

        data = "a simple string"
        as_bytes = bytes(data, encoding="utf-8")

        assert serialiser.dumps(data) == as_bytes
        assert serialiser.loads(as_bytes) == data

    def test_with_json(self):
        serialiser = DefaultSerialisers.json()

        data = [10, "json-encodable"]
        as_json = bytes(json.dumps(data, sort_keys=True), encoding="utf-8")

        assert serialiser.dumps(data) == as_json
        assert serialiser.loads(as_json) == data

    def test_with_json_zlib(self):
        serialiser = DefaultSerialisers.json_zlib()

        data = {"a": True, "b": {}, "c": 3}
        as_json_zlib = zlib.compress(bytes(json.dumps(data, sort_keys=True), encoding="utf-8"))

        assert serialiser.dumps(data) == as_json_zlib
        assert serialiser.loads(as_json_zlib) == data
