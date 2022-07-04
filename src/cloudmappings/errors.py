class KeySyncError(ValueError):
    storage_provider_name: str
    key: str
    expected_etag: str

    def __init__(self, storage_provider_name: str, key: str, etag: str) -> None:
        self.storage_provider_name = storage_provider_name
        self.key = key
        self.expected_etag = etag
        super().__init__(
            f"Mapping is out of sync with cloud data.\n"
            f"Cloud storage: '{storage_provider_name}'\n"
            f"Key: '{key}', etag: '{etag}'"
        )


class ValueSizeError(ValueError):
    storage_provider_name: str
    key: str

    def __init__(self, storage_provider_name: str, key: str) -> None:
        self.storage_provider_name = storage_provider_name
        self.key = key
        super().__init__(
            f"Value is too big to fit in cloud.\n" f"Cloud storage: '{storage_provider_name}'\n" f"Key: '{key}'"
        )
