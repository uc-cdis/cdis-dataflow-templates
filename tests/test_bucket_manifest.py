
import hashlib
from unittest.mock import MagicMock
from unittest.mock import patch

import google

from bucket_manifest.bucket import compute_md5, get_object_chunk


class MockedResponse():
    def __init__(self, status_code, content):
        self.status_code = status_code
        self.content = content



def test_get_object_chunk():
    sess = MagicMock()
    sess.request.return_value = MockedResponse(206, "example")
    assert get_object_chunk(sess, "bucket_name", "blob_name", 0, 10) == "example"

def test_fail_get_object_chunk():
    sess = MagicMock()
    sess.request.return_value = MockedResponse(404, "example")
    assert get_object_chunk(sess, "bucket_name", "blob_name", 0, 10) == None


@patch("bucket_manifest.bucket.get_object_chunk")
@patch("bucket_manifest.bucket.get_object_size")
@patch("bucket_manifest.bucket.get_object_md5_from_metadata")
def test_get_md5_from_metadata(mock_get_object_md5_from_metadata, mock_get_object_size, mock_get_object_chunk):
    """
    Test compute md5 of streaming data
    """
    mock_get_object_md5_from_metadata.return_value = "md5_example"
    google.cloud.storage.Client = MagicMock()
    compute_md5("test", "test")
    assert mock_get_object_chunk.called == False

@patch("bucket_manifest.bucket.get_object_chunk")
@patch("bucket_manifest.bucket.get_object_size")
@patch("bucket_manifest.bucket.get_object_md5_from_metadata")
def test_compute_md5_streaming(mock_get_object_md5_from_metadata, mock_get_object_size, mock_get_object_chunk):
    """
    Test compute md5 of streaming data
    """
    mock_get_object_md5_from_metadata.return_value = None
    google.cloud.storage.Client = MagicMock()
    hashlib.md5 = MagicMock()
    mock_get_object_size.return_value = 1
    compute_md5("test", "test")
    assert mock_get_object_chunk.called == True
