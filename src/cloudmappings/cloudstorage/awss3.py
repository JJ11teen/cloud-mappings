from typing import Dict
from uuid import uuid4

import boto3

from .cloudstorage import CloudStorage, KeyCloudSyncError


_metadata_etag_key = "cloud-mappings-etag"


class AWSS3(CloudStorage[str]):
    def __init__(
        self,
        bucket_name: str,
    ) -> None:
        self._client = boto3.client("s3")
        self._bucket_name = bucket_name

    def create_if_not_exists(self, metadata: Dict[str, str]):
        bucket = boto3.resource("s3").Bucket(self._bucket_name)
        try:
            bucket.create()
        except bucket.meta.client.exceptions.BucketAlreadyExists:
            return True
        return False

    def _get_body_etag_version_id_if_exists(self, key: str) -> Dict:
        try:
            response = self._client.get_object(
                Bucket=self._bucket_name,
                Key=key,
            )
            return response["Body"], response["Metadata"][_metadata_etag_key], response["VersionId"]
        except self._client.meta.client.exceptions.NoSuchKey:
            return (
                None,
                None,
            )

    def download_data(self, key: str, etag: str) -> bytes:
        body, existing_etag, _ = self._get_body_etag_version_id_if_exists(key)
        if etag is not None and (body is None or etag != existing_etag):
            raise KeyCloudSyncError(key=key, etag=etag)
        return body.read()

    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        body, existing_etag, _ = self._get_body_etag_version_id_if_exists(key)
        if body is not None and (etag is None or etag != existing_etag):
            raise KeyCloudSyncError(key=key, etag=etag)

        new_etag = uuid4()
        self._client.put_object(
            Bucket=self._bucket_name,
            Key=key,
            Body=data,
            Metadata={
                _metadata_etag_key: new_etag,
            },
        )
        return new_etag

    def delete_data(self, key: str, etag: str) -> None:
        body, existing_etag, version_id = self._get_body_etag_version_id_if_exists(key)
        if body is None or etag != existing_etag:
            raise KeyCloudSyncError(key=key, etag=etag)
        self._client.delete_object(
            Bucket=self._bucket_name,
            Key=key,
            VersionId=version_id,
        )

    def list_keys_and_ids(self, key_prefix: str) -> Dict[str, str]:
        bucket = boto3.resource("s3").Bucket(self._bucket_name)
        return {
            o.key: o.Object.metadata[_metadata_etag_key]
            for o in bucket.objects.filter(
                Prefix=key_prefix,
            )
        }
