import pytest


class SingletonCloudMappingTests:
    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(self, cloud_mapping):
        for k in list(cloud_mapping.keys()):
            del cloud_mapping[k]

    def test_basic_setting_and_getting(self, cloud_mapping):
        cloud_mapping["key-A"] = b"100"
        cloud_mapping["key-a"] = b"uncapitalised"
        cloud_mapping["key-3"] = b"three"

        assert cloud_mapping["key-A"] == b"100"
        assert cloud_mapping["key-a"] == b"uncapitalised"
        assert cloud_mapping["key-3"] == b"three"

    def test_complex_keys(self, cloud_mapping):
        cloud_mapping["here/are/some/sub/dirs"] = b"0"
        cloud_mapping["howaboutsome ˆøœ¨åß∆∫ı˜ unusual !@#$%^* characters"] = b"1"

        assert cloud_mapping["here/are/some/sub/dirs"] == b"0"
        assert cloud_mapping["howaboutsome ˆøœ¨åß∆∫ı˜ unusual !@#$%^* characters"] == b"1"

    def test_deleting_keys(self, cloud_mapping):
        cloud_mapping["1"] = b"0"
        del cloud_mapping["1"]
        with pytest.raises(KeyError):
            cloud_mapping["1"]

    def test_contains(self, cloud_mapping):
        assert "1" not in cloud_mapping

        cloud_mapping["1"] = b"1"
        assert "1" in cloud_mapping

    def test_length(self, cloud_mapping):
        assert len(cloud_mapping) == 0

        cloud_mapping["a"] = b"100"
        cloud_mapping["b"] = b"uncapitalised"
        assert len(cloud_mapping) == 2

        cloud_mapping["c"] = b"three"
        assert len(cloud_mapping) == 3

    def test_repr(self, cloud_mapping):
        _repr = str(cloud_mapping)

        assert "CloudStorageProvider=" in _repr

        if "Azure" in _repr:
            assert "StorageAccountName=" in _repr
            assert "ContainerName=" in _repr
        elif "Google" in _repr:
            assert "Project=" in _repr
            assert "BucketName=" in _repr
        elif "AWS" in _repr:
            assert "BucketName=" in _repr
