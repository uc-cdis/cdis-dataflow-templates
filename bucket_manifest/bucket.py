
from google.cloud import storage


def get_bucket_manifest(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    result = []
    for blob in blobs:
        result.append("{}\t{}\t{}".format(bucket_name, blob.name, blob.size))
    return result


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} downloaded to {}.".format(
            source_blob_name, destination_file_name
        )
    )


def compute_md5(bucket, blob):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(blob)
    return blob.md5_hash
