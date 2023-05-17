import pytest

from cloudmappings import CloudMapping


class CloudMappingDictTests:
    def test_basic_setting_and_getting(self, cloud_mapping: CloudMapping):
        cloud_mapping["key-A"] = b"100"
        cloud_mapping["key-a"] = b"uncapitalised"
        cloud_mapping["key-3"] = b"three"

        assert cloud_mapping["key-A"] == b"100"
        assert cloud_mapping["key-a"] == b"uncapitalised"
        assert cloud_mapping["key-3"] == b"three"

    def test_no_key_raises_error(self, cloud_mapping: CloudMapping):
        key = "no-key-errors"

        with pytest.raises(KeyError):
            cloud_mapping[key]
        with pytest.raises(KeyError):
            del cloud_mapping[key]
        assert key not in cloud_mapping

    def test_deleting_keys(self, cloud_mapping: CloudMapping):
        key = "delete"

        cloud_mapping[key] = 0
        del cloud_mapping[key]
        with pytest.raises(KeyError):
            cloud_mapping[key]

    def test_contains(self, cloud_mapping: CloudMapping):
        key = "contains"

        assert key not in cloud_mapping

        cloud_mapping[key] = None
        assert key in cloud_mapping

    def test_length(self, cloud_mapping: CloudMapping):
        key_1 = "length/1"
        key_2 = "length/2"
        key_3 = "length/3"

        assert len(cloud_mapping) == 0

        cloud_mapping[key_1] = b"a"
        cloud_mapping[key_2] = b"b"
        assert len(cloud_mapping) == 2

        cloud_mapping[key_3] = b"c"
        assert len(cloud_mapping) == 3

    def test_complex_string_keys(self, cloud_mapping: CloudMapping):
        key1 = "here/are/some/sub/dirs"
        key2 = "how.about_some ˆøœ¨åß∆∫ı˜unusual!@#$%^*characters"

        cloud_mapping[key1] = b"0"
        cloud_mapping[key2] = b"1"

        assert cloud_mapping[key1] == b"0"
        assert cloud_mapping[key2] == b"1"
