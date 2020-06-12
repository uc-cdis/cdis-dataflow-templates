#!/usr/bin/env python
import argparse
import logging
import queue
import time

from google.cloud import pubsub_v1


def sub(project_id, subscription_id, n_expected_messages, timeout=10000):
    """Receives messages from a Pub/Sub subscription."""
    # Initialize a Subscriber client
    subscriber_client = pubsub_v1.SubscriberClient()
    # Create a fully qualified identifier in the form of
    # `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber_client.subscription_path(project_id, subscription_id)

    n_messages = 0

    while n_messages < n_expected_messages:
        # The subscriber pulls a specific number of messages.
        response = subscriber_client.pull(subscription_path, max_messages=10)

        ack_ids = []
        n_messages += len(response.received_messages)

        for received_message in response.received_messages:
            logging.info("Received: {}".format(received_message.message.data))
            ack_ids.append(received_message.ack_id)

        # Acknowledges the received messages so they will not be sent again.
        subscriber_client.acknowledge(subscription_path, ack_ids)

        logging.info(
            "Received and acknowledged {} messages. Done.".format(
                len(response.received_messages)
            )
        )
        
    subscriber_client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("project_id", help="Google Cloud project ID")
    parser.add_argument("subscription_id", help="Pub/Sub subscription ID")

    args = parser.parse_args()

    sub(args.project_id, args.subscription_id)