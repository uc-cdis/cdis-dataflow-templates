import csv
import logging


def write_tsv(filename, files, fieldnames=None):
    """
    write to tsv file
    Args:
        filename(str): file name
        files(list(dict)): list of file info
        [
            {
                "GUID": "guid_example",
                "filename": "example",
                "size": 100,
                "acl": "['open']",
                "md5": "md5_hash",
            },
        ]
        fieldnames(list(str)): list of column names
    Returns:
        filename(str): file name
    """

    if not files:
        return None
    fieldnames = fieldnames or files[0].keys()
    with open(filename, mode="w") as outfile:
        writer = csv.DictWriter(outfile, delimiter="\t", fieldnames=fieldnames)
        writer.writeheader()

        for f in files:
            for field in fieldnames:
                if field not in f:
                    f[field] = None
            writer.writerow(f)

    return filename


def upload_file(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket
    
    Args:
        file_name: File to upload
        bucket: Bucket to upload to
        object_name: S3 object name. If not specified then file_name is used
    Returns:
        Bool: True if file was uploaded, else False
    """
    return True