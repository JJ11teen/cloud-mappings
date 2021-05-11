from typing import Dict

import boto3

from .cloudstorage import CloudStorage


class AWSS3(CloudStorage):
    def __init__(
        self,
        bucket_name: str,
    ) -> None:
        self._bucket_name = bucket_name

    def create_if_not_exists(self, metadata: Dict[str, str]):
        bucket = boto3.resource("s3").Bucket(self._bucket_name)
        try:
            bucket.create()
        except bucket.meta.client.exceptions.BucketAlreadyExists:
            return True
        return False

    def download_data(self, key: str, etag: str) -> bytes:
        obj = boto3.resource("s3").Object(self._bucket_name, key)
        return obj.get(IfMatch=etag)["Body"].read()

    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        obj = boto3.resource("s3").Object(self._bucket_name, key)
        return obj.put(
            Body=data,
            # TODO: check that etag is MD5 hash at least most of the time?
            # TODO: figure out something else the rest of the time?
            ContentMD5=etag,
        )["Etag"]

    def delete_data(self, key: str, etag: str) -> None:
        obj = boto3.resource("s3").Object(self._bucket_name, key)
        obj.delete(
            # TODO: somewhere to check etag here?
        )

    def list_keys_and_ids(self, key_prefix: str) -> Dict[str, str]:
        bucket = boto3.resource("s3").Bucket(self._bucket_name)
        return {
            o.key: o.e_tag
            for o in bucket.objects.filter(
                Prefix=key_prefix,
            )
        }
