#!/usr/bin/env python
import time
import logging

from google.cloud import pubsub_v1


def get_callback(api_future, data, ref):
    """Wrap message data in the context of the callback function."""

    def callback(api_future):
        try:
            logging.info(
                "Published message {} now has message ID {}".format(
                    data, api_future.result()
                )
            )
            ref["num_messages"] += 1
        except Exception:
            logging.error(
                "A problem occurred when publishing {}: {}\n".format(
                    data, api_future.exception()
                )
            )

    return callback


def pub(project_id, topic_id, data):
    """
    Publishes a message to a Pub/Sub topic.
    Data sent to Cloud Pub/Sub must be a bytestring.
    """
    # Initialize a Publisher client.
    client = pubsub_v1.PublisherClient()
    # Create a fully qualified identifier in the form of
    # `projects/{project_id}/topics/{topic_id}`
    topic_path = client.topic_path(project_id, topic_id)

    # Keep track of the number of published messages.
    ref = dict({"num_messages": 0})

    # When you publish a message, the client returns a future.
    api_future = client.publish(topic_path, data=data)
    api_future.add_done_callback(get_callback(api_future, data, ref))

    # Keep the main thread from exiting while the message future
    # gets resolved in the background.
    while api_future.running():
        time.sleep(0.5)
        logging.debug("Published {} message(s).".format(ref["num_messages"]))
