# This is a helper utility to remove cloud resources created by running tests.
# Azure containers are not cleaned up as they can (and should) be set to auto-delete after a week.

if __name__ == "__main__":
    import boto3
    from google.cloud import storage

    client = boto3.client("s3")
    s3 = boto3.resource("s3")

    buckets = client.list_buckets()

    for bucket in buckets["Buckets"]:
        if bucket["Name"].startswith("pytest"):
            s3_bucket = s3.Bucket(bucket["Name"])
            s3_bucket.objects.all().delete()
            s3_bucket.object_versions.delete()
            s3_bucket.delete()
            print(f"Deleted s3 bucket {bucket['Name']}")

    storage_client = storage.Client()
    buckets = storage_client.list_buckets()
    for bucket in buckets:
        if bucket.name.startswith("pytest"):
            bucket.delete(force=True)
            print(f"Deleted gcp bucket {bucket.name}")

    print(f"Not deleting Azure containers")
