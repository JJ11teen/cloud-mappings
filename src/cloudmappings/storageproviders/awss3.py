import logging
from typing import Dict
from uuid import uuid4

import boto3

from .storageprovider import StorageProvider


_metadata_etag_key = "cloud-mappings-etag"


class AWSS3Provider(StorageProvider):
    def __init__(
        self,
        bucket_name: str,
        silence_warning: bool = False,
    ) -> None:
        self._client = boto3.client("s3")
        self._bucket_name = bucket_name
        if not silence_warning:
            logging.warning(
                msg=(
                    "AWS S3 does not support server-side atomic requests, it is not recommended for concurrent use.\n",
                    "Consider using another provider such as Azure or GCP if you need concurrent access.\n",
                    "You may silence this warning with silence_warning=True.",
                ),
            )

    def logical_name(self) -> str:
        return "CloudStorageProvider=AWSS3," f"BucketName={self._bucket_name}"

    def create_if_not_exists(self):
        already_exists = False
        bucket = boto3.resource("s3").Bucket(self._bucket_name)

        # Note: There is a race condition here:
        # If bucket is created after the creation_date is fetched but before the create call succeeds.
        # Currently, bucket.creation_date appears to be the only way to check if a bucket exists in s3,
        # and the create*() series of methods silently return success even if the bucket already exists.
        # TODO: Monitor S3 API to see if a parameter to support atomic creations is added.
        if bucket.creation_date is not None:
            already_exists = True
            existing_versioning = self._client.get_bucket_versioning(Bucket=self._bucket_name)
            if existing_versioning["Status"] != "Enabled":
                raise ValueError(
                    "Found existing bucket with Versioning disabled. Enable versioning or specify a non-existing bucket name."
                )
        else:
            bucket.create()
            bucket.Versioning().put(
                VersioningConfiguration={"Status": "Enabled"},
            )
        return already_exists

    def _get_body_etag_version_id_if_exists(self, key: str) -> Dict:
        try:
            response = self._client.get_object(
                Bucket=self._bucket_name,
                Key=key,
            )
            return (
                response["Body"],
                response["Metadata"][_metadata_etag_key],
                response["VersionId"],
            )
        except self._client.exceptions.NoSuchKey:
            return (None, None, None)

    def download_data(self, key: str, etag: str) -> bytes:
        body, existing_etag, _ = self._get_body_etag_version_id_if_exists(key)
        if etag is not None and (body is None or etag != existing_etag):
            self.raise_key_sync_error(key=key, etag=etag)
        if body is None:
            return None
        return body.read()

    def upload_data(self, key: str, etag: str, data: bytes) -> str:
        if not isinstance(data, bytes):
            raise ValueError("Data must be bytes like")
        _, existing_etag, _ = self._get_body_etag_version_id_if_exists(key)
        if etag != existing_etag:
            self.raise_key_sync_error(key=key, etag=etag)
        # Note: There is a race condition here:
        # If blob is changed after the etag is fetched but before the put_object call succeeds.
        # Currently, the S3 API does not appear to support any parameters that would enable server-side
        # conflict checking.
        # TODO: Monitor S3 API to see if a parameter to support atomic requests is added.
        new_etag = str(uuid4())
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
            self.raise_key_sync_error(key=key, etag=etag)
        # Note: There is a race condition here:
        # If blob is changed after the etag is fetched but before the delete_object call succeeds.
        # Currently, the S3 API does not appear to support any parameters that would enable server-side
        # conflict checking.
        # TODO: Monitor S3 API to see if a parameter to support atomic requests is added.
        self._client.delete_object(
            Bucket=self._bucket_name,
            Key=key,
            VersionId=version_id,
        )

    def list_keys_and_etags(self, key_prefix: str) -> Dict[str, str]:
        bucket = boto3.resource("s3").Bucket(self._bucket_name)
        kwargs = {}
        if key_prefix is not None:
            kwargs["Prefix"] = key_prefix
        return {o.key: o.Object().metadata[_metadata_etag_key] for o in bucket.objects.filter(**kwargs)}
