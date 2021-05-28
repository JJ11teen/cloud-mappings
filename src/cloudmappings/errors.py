class KeySyncError(ValueError):
    def __init__(self, storage_provider_name: str, key: str, etag: str) -> None:
        super().__init__(
            f"Mapping is out of sync with cloud data.\n"
            f"Cloud storage: '{storage_provider_name}'\n"
            f"Key: '{key}', etag: '{etag}'"
        )


class ValueSizeError(ValueError):
    def __init__(self, storage_provider_name: str, key: str) -> None:
        super().__init__(
            f"Value is too big to fit in cloud.\n" f"Cloud storage: '{storage_provider_name}'\n" f"Key: '{key}'"
        )
