class KeySyncError(ValueError):
    def __init__(self, storageprovider_safe_name: str, key: str, etag: str) -> None:
        super().__init__(
            f"Mapping is out of sync with cloud data.\n"
            f"Cloud storage: '{storageprovider_safe_name}'\n"
            f"Key: '{key}', etag: '{etag}'"
        )


class ValueSizeError(ValueError):
    def __init__(self, storageprovider_safe_name: str, key: str, size: int) -> None:
        super().__init__(
            f"Value is too big to fit in cloud."
            f"Cloud storage: '{storageprovider_safe_name}'\n"
            f"Key: '{key}', size: '{size}'"
        )
