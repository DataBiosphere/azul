import sys

import argparse
import json
import more_itertools
from azul import config
import logging

from urllib.parse import urlparse

from azul.deployment import aws

logger = logging.getLogger(__name__)


def main(argv):
    logging.basicConfig(format='%(asctime)s %(levelname)-7s %(threadName)-7s: %(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser(description='Extract and or remove messages from SQS queues')

    subparsers = parser.add_subparsers(help='sub-command help', dest='command')

    parser_dump = subparsers.add_parser('dump', help='Dump contents of queue into designated file')
    parser_dump.add_argument('queue_name', help='Name of the queue to obtain the messages from')
    parser_dump.add_argument('file_name', help='Name of file to write output to')
    parser_dump.add_argument('--delete', '-D', action='store_true',
                             help='Purge messages from the queue after writing them to the specified file. '
                                  'By default the messages will be returned to the queue')
    parser_dump.add_argument('--json-body', '-J', action='store_true', help='Serialize json for queue message body')
    subparsers.add_parser('list', help='List SQS queues in current deployment')

    args = parser.parse_args(argv)
    if args.command == 'list':
        list_queues()
    elif args.command == 'dump':
        get_messages_from_queue(options=args)
    else:
        parser.parse_args(['--help'])


def list_queues():
    sqs = aws.sqs_resource
    logging.info('Listing queues')
    all_queues = sqs.queues.all()
    print('\n{:<35s}{:^20s}{:^20s}{:^18s}\n'.format('Queue Name',
                                                    'Messages Available',
                                                    'Messages in Flight',
                                                    'Messages Delayed'))
    for queue in all_queues:
        _, _, queue_name = urlparse(queue.url).path.rpartition('/')
        if is_azul_queue(queue_name):
            print('{:<35s}{:^20s}{:^20s}{:^18s}'.format(queue_name,
                                                        queue.attributes['ApproximateNumberOfMessages'],
                                                        queue.attributes['ApproximateNumberOfMessagesNotVisible'],
                                                        queue.attributes['ApproximateNumberOfMessagesDelayed']))


def get_messages_from_queue(options):
    sqs = aws.sqs_resource
    queue = sqs.get_queue_by_name(QueueName=options.queue_name)
    logging.info('Writing messages from queue "%s" to file "%s"', queue.url, options.file_name)

    visibility_timeout = 60 * 5
    logging.info('Default visibility timeout "%i"', visibility_timeout)
    messages = []
    while True:
        message_batch = queue.receive_messages(AttributeNames=['All'],
                                               MaxNumberOfMessages=10,
                                               VisibilityTimeout=visibility_timeout)
        if not message_batch:  # Nothing left in queue
            break
        else:
            messages.extend(message_batch)
    with open(options.file_name, 'w') as file:
        json.dump({'queue': queue.url,
                   'messages': extract_fields(messages, options.json_body)},
                  fp=file,
                  indent=4)
    logging.info('Wrote %i messages', len(messages))
    message_batches = list(more_itertools.chunked(messages, 10))
    if options.delete:
        logging.info('Removing messages from queue "%s"', queue.url)
        delete_messages(message_batches, queue)
    else:
        logger.info('Returning messages to queue %s', queue.url)
        return_messages(message_batches, queue)
    logging.info(f'Finished writing {options.file_name!r}')


def return_messages(message_batches, queue):
    for message_batch in message_batches:
        batch = [dict(Id=message.message_id,
                      ReceiptHandle=message.receipt_handle,
                      VisibilityTimeout=0) for message in message_batch]
        response = queue.change_message_visibility_batch(Entries=batch)
        if len(response['Successful']) != len(batch):
            raise RuntimeError(f'Failed to revert visibility: {response!r}')


def delete_messages(message_batches, queue_resource):
    for message_batch in message_batches:
        response = queue_resource.delete_messages(
            Entries=[dict(Id=message.message_id,
                          ReceiptHandle=message.receipt_handle) for message in message_batch])
        if len(response['Successful']) != len(message_batch):
            raise RuntimeError(f'Failed to delete messages: {response!r}')


def extract_fields(messages, json_body=False):
    return [
        {
            "MessageId": message.message_id,
            "ReceiptHandle": message.receipt_handle,
            "MD5OfBody": message.md5_of_body,
            "Body": json.loads(message.body) if json_body else message.body,
            "Attributes": message.attributes
        } for message in messages
    ]


def is_azul_queue(queue_name) -> bool:
    return any(config.unqualified_resource_name_or_none(queue_name, suffix)[1] == config.deployment_stage
               for suffix in ('', '.fifo'))


if __name__ == '__main__':
    main(sys.argv[1:])
