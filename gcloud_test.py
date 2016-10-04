#!/usr/bin/python

import argparse
from gcloud import pubsub


def receive_message(topic_name, subscription_name):
    """Receives a message from a pull subscription."""
    pubsub_client = pubsub.Client('safe-browsing-notification-api')
    topic = pubsub_client.topic(topic_name)
    subscription = topic.subscription(subscription_name)
    # Change return_immediately=False to block until messages are
    # received.
    results = subscription.pull(return_immediately=True)
    print('Received {} messages.'.format(len(results)))

    for ack_id, message in results:
        print('* {}: {}, {}'.format(
            message.message_id, message.data, message.attributes))

    # Acknowledge received messages. If you do not acknowledge, Pub/Sub will
    # redeliver the message.
    if results:
        subscription.acknowledge([ack_id for ack_id, message in results])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest='command')
    receive_parser = subparsers.add_parser(
        'receive', help=receive_message.__doc__)
    receive_parser.add_argument('topic_name')
    receive_parser.add_argument('subscription_name')

    args = parser.parse_args()

    if args.command == 'receive':
        receive_message(args.topic_name, args.subscription_name)
