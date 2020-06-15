from google.cloud import storage
import logging


def get_bucket_manifest(bucket_name):
    """
    Get list of the bucket objects
    """
    result = []
    storage_client = storage.Client()
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs()
        for blob in blobs:
            result.append("{}\t{}\t{}".format(bucket_name, blob.name, blob.size))
    except Exception as e:
        logging.error("Can not get object list of {}. Detail {}".format(bucket_name, e))
    
    return result



def download_blob(bucket_name, source_blob_name, destination_file_name):
    """
    Downloads a blob from the bucket.
    
    Args:
        bucket_name(str): the bucket-name
        source_blob_name(str): the storage object name
        destination_file_name(str): local/path/to/file
    """

    storage_client = storage.Client()
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
    except Exception as e:
        logging.error("Can not download {}. Detail {}".format(source_blob_name, e))


def compute_md5(bucket, blob):
    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket)
        blob = bucket.get_blob(blob)
        return blob.md5_hash
    except Exception as e:
        logging.error("Can not compute md5 of {}. Detail {}".format(blob, e))
        return None

