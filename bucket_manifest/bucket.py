
from google.cloud import storage


def get_bucket_manifest(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    result = []
    for blob in blobs:
        result.append("{}\t{}\t{}".format(bucket_name, blob.name, blob.size))
    return result
