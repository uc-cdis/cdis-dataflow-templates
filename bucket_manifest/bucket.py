import base64
import hashlib
import logging

from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession

# Chunk size for streaming data from a bucket.
CHUNK_SIZE = 1024 * 1024 * 64
MAX_RETRIES = 5

logging.basicConfig(level=logging.INFO)


def get_bucket_manifest(bucket_name):
    """
    Get list of the bucket objects
    """
    result = []
    # Initialize a storage client.
    storage_client = storage.Client()
    try:
        # Initialize a bucket client.
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs()
        for blob in blobs:
            # Keep track all the object infos
            result.append("{}\t{}\t{}".format(bucket_name, blob.name, blob.size))
    except Exception as e:
        logging.error("Can not get object list of {}. Detail {}".format(bucket_name, e))

    return result


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
            # Make a request to get object metadata
            res = sess.request(method="GET", url=url)
            if res.status_code == 200:
                # If md5 hash in the object metadata, return it
                if res.json().get("md5Hash"):
                    return base64.b64decode(res.json()["md5Hash"]).hex()
            else:
                logging.error(
                    f"Can not get object metadata of {blob_name}. Status code {res.status_code}"
                )
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
            # Make a request to get object metadata
            res = sess.request(method="GET", url=url)
            # Success
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
    # Retry logic
    while tries < MAX_RETRIES:
        try:
            # Make a request to download a chunk data starting at {start} and ending at {end}
            res = sess.request(
                method="GET",
                url=url,
                headers={"Range": "bytes={}-{}".format(start, end)},
            )
            # Success
            if res.status_code in [200, 206]:
                return res.content
            # access denied
            elif res.status_code in [401, 403]:
                logging.error(f"Access denied!!! {bucket_name/blob_name}")
                return None

        except Exception as e:
            logging.error(f"Can not get object chunk of {blob_name}. Detail {e}")
        tries += 1

    return None


def compute_md5(bucket_name, blob_name):
    """
    Compute md5 of a bucket object
    """
    # Initialize a storage client
    client = storage.Client()
    # Get client authorized session
    sess = AuthorizedSession(client._credentials)
    # If md5 is in the object metadata. Return it and exit
    md5 = get_object_md5_from_metadata(sess, bucket_name, blob_name)
    if md5:
        return md5
    # Start process to compute md5 by streaming data from the bucket.
    # Need to know the size of the object.
    size = get_object_size(sess, bucket_name, blob_name)
    # Initialize hash object.
    sig = hashlib.md5()
    if size:
        # Initialize a start location
        start = 0
        while start < size:
            end = min(start + CHUNK_SIZE, size - 1)
            # Download a chunk data
            chunk_data = get_object_chunk_data(sess, bucket_name, blob_name, start, end)
            # Update md5
            sig.update(chunk_data)
            start = end + 1
            # Log the progess. It is usefull for computing big file
            logging.info(f"Progress: {start*1.0/size*100}%")
        return sig.hexdigest()
    return None
