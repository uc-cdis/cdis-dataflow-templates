#!/usr/bin/env python
import argparse
import logging
import csv
import datetime

from urllib.parse import urlparse
from google.cloud import pubsub_v1

import utils


def sub(project_id, subscription_id, n_expected_messages=3, timeout=10000):
    """Receives messages from a Pub/Sub subscription."""
    subscriber_client = pubsub_v1.SubscriberClient()
    subscription_path = subscriber_client.subscription_path(project_id, subscription_id)

    NUM_MESSAGES = 10
    n_messages = 0

    with subscriber_client:
        while n_messages < n_expected_messages:
            # The subscriber pulls a specific number of messages.
            response = subscriber_client.pull(subscription_path, max_messages=NUM_MESSAGES)
            n_messages = n_messages + len(response.received_messages)

            ack_ids = []
            for received_message in response.received_messages:
                print("Received: {}".format(received_message.message.data))
                logging.info("Received: {}".format(received_message.message.data))
                ack_ids.append(received_message.ack_id)

            # Acknowledges the received messages so they will not be sent again.
            subscriber_client.acknowledge(subscription_path, ack_ids)

            print(
                "Received and acknowledged {} messages. Done.".format(
                    len(response.received_messages)
                )
            )


def write_messages_to_tsv(files, n_total_messages, bucket_name, authz_file=None):
    """
    Consume the sqs and write results to tsv manifest
    Args:
        queue_url(str): SQS url
        n_total_messages(int): The expected number of messages being received
        bucket_name(str): bucket for uploading the manifest to
        authz_file(str): authz data file
    """
    authz_objects = {}
    # Default filenames without merging
    fields = ["url", "size", "md5"]

    # merge authz info from file
    if authz_file:
        with open(authz_file, "rt") as csvfile:
            csvReader = csv.DictReader(csvfile, delimiter="\t")
            # Build a map with url as the key
            for row in csvReader:
                if "url" in row:
                    authz_objects[row["url"]] = {
                        k: v for k, v in row.items() if k != "url"
                    }

        # do merging if possible, and update fields
        need_merge = False
        first_row_need_merge = None
        for row_num, fi in enumerate(files):
            if fi["url"] in authz_objects:
                need_merge = True
                first_row_need_merge = first_row_need_merge or row_num
                for k, v in authz_objects[fi["url"]].items():
                    fi[k] = v
        if files and need_merge:
            # add new fields
            [
                fields.append(k)
                for k in files[first_row_need_merge].keys()
                if k not in ["url", "size", "md5"]
            ]

    if len(files) > 0:
        parts = urlparse(files[0]["url"])
        now = datetime.now()
        current_time = now.strftime("%m_%d_%y_%H:%M:%S")

        filename = "manifest_{}_{}.tsv".format(parts.netloc, current_time)
        utils.write_tsv(filename, files, fields)

        logging.info(
            "Output manifest is stored at gs://{}/{}".format(bucket_name, filename)
        )

    logging.info("DONE!!!")


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    bucket_manifest_cmd = subparsers.add_parser("create_manifest")
    bucket_manifest_cmd.add_argument("--project_id", help="Google Cloud project ID")
    bucket_manifest_cmd.add_argument("--subscription_id", help="Pub/Sub subscription ID")
    bucket_manifest_cmd.add_argument("--n_expected_messages", type=int, help="Number of expected messages")
    bucket_manifest_cmd.add_argument("--bucket_name", help="Output bucket name")
    bucket_manifest_cmd.add_argument("--authz_file", required=False, help="Authz data file")

    return  parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    if args.action == "create_manifest":
        files = sub(args.project_id, args.subscription_id, args.n_expected_messages)

        #write_messages_to_tsv(files, args.bucket_name, args.authz_file)
