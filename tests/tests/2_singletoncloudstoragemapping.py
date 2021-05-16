import pytest

from cloudmappings.cloudstoragemapping import CloudMapping


class SingletonCloudMappingTests:
    def test_initialising_mapping(self, storage_provider):
        cm = CloudMapping(storageprovider=storage_provider)

    def test_basic_setting_and_getting(self, storage_provider):
        cm = CloudMapping(storageprovider=storage_provider)

        cm["key-A"] = b"100"
        cm["key-a"] = b"uncapitalised"
        cm["key-3"] = b"three"

        assert cm["key-A"] == b"100"
        assert cm["key-a"] == b"uncapitalised"
        assert cm["key-3"] == b"three"

    def test_complex_keys(self, storage_provider):
        cm = CloudMapping(storageprovider=storage_provider)

        cm["here/are/some/sub/dirs"] = b"0"
        cm["howaboutsome ˆøœ¨åß∆∫ı˜ unusual !@#$%^* characters"] = b"1"

        assert cm["here/are/some/sub/dirs"] == b"0"
        assert cm["howaboutsome ˆøœ¨åß∆∫ı˜ unusual !@#$%^* characters"] == b"1"

    def test_deleting_keys(self, storage_provider):
        cm = CloudMapping(storageprovider=storage_provider)

        cm["1"] = b"0"
        del cm["1"]
        with pytest.raises(KeyError):
            cm["1"]

    def test_contains(self, storage_provider):
        cm = CloudMapping(storageprovider=storage_provider)

        assert "1" not in cm

        cm["1"] = b"1"
        assert "1" in cm

    def test_length(self, storage_provider):
        cm = CloudMapping(storageprovider=storage_provider)

        assert len(cm) == 0

        cm["a"] = b"100"
        cm["b"] = b"uncapitalised"
        assert len(cm) == 2

        cm["c"] = b"three"
        assert len(cm) == 3

    def test_repr(self, storage_provider):
        cm = CloudMapping(storageprovider=storage_provider)

        _repr = str(cm)

        assert "CloudStorageProvider=" in _repr

        if "Azure" in _repr:
            assert "StorageAccountName=" in _repr
            assert "ContainerName=" in _repr
        elif "Google" in _repr:
            assert "Project=" in _repr
            assert "BucketName=" in _repr
        elif "AWS" in _repr:
            assert "BucketName=" in _repr
