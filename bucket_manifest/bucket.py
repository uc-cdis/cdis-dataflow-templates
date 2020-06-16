import base64
import hashlib
import logging

from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession

CHUNK_SIZE = 1024 * 1024 * 64
MAX_RETRIES = 5


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


def get_object_md5_from_metadata(sess, bucket_name, blob_name):
    """
    Get object metadata

    Args:
        sess(session): google client session
        bucket_name(str): google bucket name
        blob_name(str): object key
    
    Returns:
        str: google object checksum
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, blob_name
    )
    tries = 0
    while tries < MAX_RETRIES:
        try:
            res = sess.request(method="GET", url=url)
            if res.status_code == 200:
                if "md5Hash" in res.json() and res.json()["md5Hash"] != "":
                    return base64.b64decode(res.json()["md5Hash"]).hex()
            else:
                logging.error(
                    f"Can not get object metadata of {blob_name}. Status code {res.status_code}"
                )
                break
        except Exception as e:
            logging.error(f"Can not get object metadata of {blob_name}. Detail {e}")
        tries += 1
    return None


def get_object_size(sess, bucket_name, blob_name):
    """
    get object metadata

    Args:
        sess(session): google client session
        bucket_name(str): google bucket name
        blob_name(str): object key
    
    Returns:
        size(int): bucket object size
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, blob_name
    )
    tries = 0
    while tries < MAX_RETRIES:
        try:
            res = sess.request(method="GET", url=url)
            if res.status_code == 200:
                return int(res.json()["size"])
        except Exception as e:
            logging.error(f"Can not get object size of {blob_name}. Detail {e}")
        tries += 1

    return None


def get_object_chunk_data(sess, bucket_name, blob_name, start, end):
    """
    get object chunk data

    Args:
        sess(session): google client session
        bucket_name(str): google bucket name
        blob_name(str): object key
        start(int): start position
        end(int): end position
    
    Returns:
        list(byte): chunk data
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}?alt=media".format(
        bucket_name, blob_name
    )
    tries = 0
    while tries < MAX_RETRIES:
        try:
            res = sess.request(
                method="GET",
                url=url,
                headers={"Range": "bytes={}-{}".format(start, end)},
            )
            if res.status_code in [200, 206]:
                return res.content
        except Exception as e:
            logging.error(f"Can not get object chunk of {blob_name}. Detail {e}")
        tries += 1

    return None


def compute_md5(bucket_name, blob_name):
    """
    Compute md5 of a bucket object
    """
    client = storage.Client()
    sess = AuthorizedSession(client._credentials)
    md5 = get_object_md5_from_metadata(sess, bucket_name, blob_name)
    if md5:
        return md5
    size = get_object_size(sess, bucket_name, blob_name)
    sig = hashlib.md5()
    if size:
        start = 0
        while start < size:
            end = min(start + CHUNK_SIZE, size - 1)
            chunk_data = get_object_chunk_data(sess, bucket_name, blob_name, start, end)
            sig.update(chunk_data)
            start = end + 1
        return sig.hexdigest()
    return None
