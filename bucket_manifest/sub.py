#!/usr/bin/env python
import argparse
import logging
import csv
import json
import time
from datetime import datetime

from urllib.parse import urlparse
from google.cloud import pubsub_v1

import utils

logging.basicConfig(level=logging.INFO)


def sub(project_id, subscription_id, n_expected_messages=1):
    """
        Receive {n_expected_messages} messages from a Pub/Sub subscription.
        
        Args:
            project_id(str): google project id
            subscription_id(str): subscription id
            n_expected_messages(int): number of expected messages
        
        Returns:
            list(dict): List of dictionaries
                {
                    "url": "gs://bucket/object1",
                    "md5": "test_md5",
                    "size": 1
                }
    """
    subscriber_client = pubsub_v1.SubscriberClient()
    # `projects/{project_id}/topics/{topic_id}`
    subscription_path = subscriber_client.subscription_path(project_id, subscription_id)

    NUM_MESSAGES = 10
    n_messages = 0

    results = []

    with subscriber_client:
        while n_messages < n_expected_messages:
            # The subscriber pulls a specific number of messages.
            response = subscriber_client.pull(
                subscription_path, max_messages=NUM_MESSAGES
            )
            n_messages = n_messages + len(response.received_messages)

            ack_ids = []
            # Pull messages from the subscription
            for received_message in response.received_messages:
                data = received_message.message.data
                logging.info("Received: {}".format(data))
                try:
                    json_data = json.loads(data)
                    results.append(
                        {
                            "url": "gs://{}/{}".format(
                                json_data["bucket"], json_data["key"]
                            ),
                            "size": json_data["size"],
                            "md5": json_data["md5"],
                        }
                    )
                except Exception as e:
                    logging.error(
                        "Fail to handle message {}. Detail {}".format(data, e)
                    )
                ack_ids.append(received_message.ack_id)

            logging.info(
                "Received and acknowledged {} messages.".format(
                    len(response.received_messages)
                )
            )
            if ack_ids:
                # Acknowledges the received messages so they will not be sent again.
                subscriber_client.acknowledge(subscription_path, ack_ids)
            else:
                # No message, take a sleep
                time.sleep(5)

    return results


def write_messages_to_tsv(files, bucket_name, metadata_file=None):
    """
    Consume the subscription and write results to tsv manifest
    Args:
        files(dict): a dictionary of object files
            {
                "url": "test_url",
                "md5": "test_md5",
                "size": 1
            }
        bucket_name(str): bucket for uploading the manifest to
        metadata_file(str): metadata file for merging
    """
    metadata_info = {}
    # Default filenames without merging
    fields = ["url", "size", "md5"]

    # merge extra metadata info from file
    if metadata_file:
        with open(metadata_file, "rt") as csvfile:
            csvReader = csv.DictReader(csvfile, delimiter="\t")
            # Build a map with url as the key
            for row in csvReader:
                if "url" in row:
                    metadata_info[row["url"]] = {
                        k: v for k, v in row.items() if k != "url"
                    }

        # do merging if possible, and update fields
        need_merge = False
        first_row_need_merge = None
        for row_num, fi in enumerate(files):
            if fi["url"] in metadata_info:
                need_merge = True
                first_row_need_merge = first_row_need_merge or row_num
                for k, v in metadata_info[fi["url"]].items():
                    fi[k] = v
        if files and need_merge:
            # add new fields
            [
                fields.append(k)
                for k in files[first_row_need_merge].keys()
                if k not in ["url", "size", "md5"]
            ]

    if len(files) > 0:
        # part the url
        parts = urlparse(files[0]["url"])
        
        # generate unique manifest output
        now = datetime.now()
        current_time = now.strftime("%m_%d_%y_%H:%M:%S")
        filename = "manifest_{}_{}.tsv".format(parts.netloc, current_time)

        # write list of object metadata to a file
        utils.write_tsv(filename, files, fields)
        # Upload the file to google bucket
        utils.upload_file(bucket_name, filename, filename)

    logging.info("DONE!!!")


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    bucket_manifest_cmd = subparsers.add_parser("create_manifest")
    bucket_manifest_cmd.add_argument("--project_id", help="Google Cloud project ID")
    bucket_manifest_cmd.add_argument(
        "--subscription_id", help="Pub/Sub subscription ID"
    )
    bucket_manifest_cmd.add_argument(
        "--n_expected_messages", type=int, help="Number of expected messages"
    )
    bucket_manifest_cmd.add_argument("--bucket_name", help="Output bucket name")
    bucket_manifest_cmd.add_argument(
        "--metadata_file", required=False, help="Metadata file for merging"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    if args.action == "create_manifest":
        files = sub(args.project_id, args.subscription_id, args.n_expected_messages)
        write_messages_to_tsv(files, args.bucket_name, args.metadata_file)
