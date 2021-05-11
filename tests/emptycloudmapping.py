import pytest

from .mock.memorystorage import MemoryStorage
from cloudmappings.cloudstoragemapping import CloudStorageMapping


class EmptyCloudMappingTests:
    def test_basic_setting_and_getting(self):
        store = CloudStorageMapping(MemoryStorage(None), None)

        store["key-A"] = b"100"
        store["key-a"] = b"uncapitalised"
        store["key-3"] = b"three"

        assert store["key-A"] == b"100"
        assert store["key-a"] == b"uncapitalised"
        assert store["key-3"] == b"three"

    def test_complex_keys(self):
        store = CloudStorageMapping(MemoryStorage(None), None)

        store["here/are/some/sub/dirs"] = b"0"
        store["howaboutsome ˆøœ¨åß∆∫ı˜ unusual !@#$%^* characters"] = b"1"

        assert store["here/are/some/sub/dirs"] == b"0"
        assert store["howaboutsome ˆøœ¨åß∆∫ı˜ unusual !@#$%^* characters"] == b"1"

    def test_deleting_keys(self):
        store = CloudStorageMapping(MemoryStorage(None), None)

        store["1"] = 1
        assert store["1"] == 1

        del store["1"]
        with pytest.raises(KeyError):
            store["1"]

    def test_contains(self):
        store = CloudStorageMapping(MemoryStorage(None), None)
        assert "1" not in store

        store["1"] = 1
        assert "1" in store

    def test_length(self):
        store = CloudStorageMapping(MemoryStorage(None), None)
        assert len(store) == 0

        store["a"] = b"100"
        store["b"] = b"uncapitalised"
        assert len(store) == 2

        store["c"] = b"three"
        assert len(store) == 3
