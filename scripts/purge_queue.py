from itertools import chain
import os
import sys

import argparse
import boto3
import json
import more_itertools
from azul import config
import logging

from urllib.parse import urlparse

from azul.files import write_file_atomically

logger = logging.getLogger(__name__)


class Main:

    @classmethod
    def main(cls, argv):
        logging.basicConfig(format='%(asctime)s %(levelname)-7s %(threadName)-7s: %(message)s', level=logging.INFO)
        parser = argparse.ArgumentParser(description='Extract and or remove messages from SQS queues')

        subparsers = parser.add_subparsers(help='sub-command help', dest='command')

        parser_dump = subparsers.add_parser('dump',
                                            help='Dump contents of queue into designated file')
        parser_dump.add_argument('queue', metavar='QUEUE_NAME',
                                 help='Name of the queue to obtain messages from')
        parser_dump.add_argument('path', metavar='FILE_PATH',
                                 help='Path of file to write messages to')
        parser_dump.add_argument('--delete', '-D', action='store_true',
                                 help='Purge messages from the queue after writing them to the specified file. '
                                      'By default the messages will be returned to the queue')
        parser_dump.add_argument('--no-json-body', '-J', dest='json_body', action='store_false',
                                 help='Do not deserialize JSON in queue message body.')

        parser_feed = subparsers.add_parser('feed', help='Feed messages from file back into queue')
        parser_feed.add_argument('path', metavar='FILE_PATH',
                                 help='Path of file to read messages from')
        parser_feed.add_argument('queue', metavar='QUEUE_NAME',
                                 help='Name of the queue to feed messages into')
        parser_feed.add_argument('--force', '-F', action='store_true',
                                 help='Force feeding messages to a queue they did not originate from.')
        parser_feed.add_argument('--delete', '-D', action='store_true',
                                 help='Remove messages from the file after submitting them to the specified queue. '
                                      'By default the messages will remain in the file')

        subparsers.add_parser('list', help='List SQS queues in current deployment')

        args = parser.parse_args(argv)

        main = Main(args)
        if args.command:
            getattr(main, args.command)()
        else:
            parser.print_usage()

    def __init__(self, args):
        self.args = args

    def list(self):
        sqs = boto3.resource('sqs')
        logging.info('Listing queues')
        all_queues = sqs.queues.all()
        print('\n{:<35s}{:^20s}{:^20s}{:^18s}\n'.format('Queue Name',
                                                        'Messages Available',
                                                        'Messages in Flight',
                                                        'Messages Delayed'))
        for queue in all_queues:
            _, _, queue_name = urlparse(queue.url).path.rpartition('/')
            if self._is_azul_queue(queue_name):
                print('{:<35s}{:^20s}{:^20s}{:^18s}'.format(queue_name,
                                                            queue.attributes['ApproximateNumberOfMessages'],
                                                            queue.attributes['ApproximateNumberOfMessagesNotVisible'],
                                                            queue.attributes['ApproximateNumberOfMessagesDelayed']))

    def dump(self):
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=self.args.queue)
        logging.info('Writing messages from queue "%s" to file "%s"', queue.url, self.args.path)

        messages = []
        while True:
            message_batch = queue.receive_messages(AttributeNames=['All'],
                                                   MaxNumberOfMessages=10,
                                                   VisibilityTimeout=300)
            if not message_batch:  # Nothing left in queue
                break
            else:
                messages.extend(message_batch)
        self._dump_messages(messages, queue.url)
        message_batches = list(more_itertools.chunked(messages, 10))
        if self.args.delete:
            logging.info('Removing messages from queue "%s"', queue.url)
            self._delete_messages(message_batches, queue)
        else:
            logger.info('Returning messages to queue "%s"', queue.url)
            self._return_messages(message_batches, queue)
        logging.info(f'Finished writing {self.args.path!r}')

    def _dump_messages(self, messages, queue_url):
        messages = [self._condense(message) for message in messages]
        with write_file_atomically(self.args.path) as file:
            content = {
                'queue': queue_url,
                'messages': messages
            }
            json.dump(content, file, indent=4)
        logging.info('Wrote %i messages', len(messages))

    def _return_messages(self, message_batches, queue):
        for message_batch in message_batches:
            batch = [dict(Id=message.message_id,
                          ReceiptHandle=message.receipt_handle,
                          VisibilityTimeout=0) for message in message_batch]
            response = queue.change_message_visibility_batch(Entries=batch)
            if len(response['Successful']) != len(batch):
                raise RuntimeError(f'Failed to return message: {response!r}')

    def _delete_messages(self, message_batches, queue):
        for message_batch in message_batches:
            response = queue.delete_messages(
                Entries=[dict(Id=message.message_id,
                              ReceiptHandle=message.receipt_handle) for message in message_batch])
            if len(response['Successful']) != len(message_batch):
                raise RuntimeError(f'Failed to delete messages: {response!r}')

    def _condense(self, message):
        """
        Prepare a message for writing to a local file.
        """
        return {
            'MessageId': message.message_id,
            'ReceiptHandle': message.receipt_handle,
            'MD5OfBody': message.md5_of_body,
            'Body': json.loads(message.body) if self.args.json_body else message.body,
            'Attributes': message.attributes,
        }

    def _reconstitute(self, message):
        """
        Prepare a message from a local file for submission to a queue.

        The inverse of _condense().
        """
        body = message['Body']
        if not isinstance(body, str):
            body = json.dumps(body)
        attributes = message['Attributes']
        result = {
            'Id': message['MessageId'],
            'MessageBody': body,
        }
        for key in ('MessageGroupId', 'MessageDeduplicationId'):
            try:
                result[key] = attributes[key]
            except KeyError:
                pass
        return result

    def _is_azul_queue(self, queue_name) -> bool:
        return any(config.unqualified_resource_name_or_none(queue_name, suffix)[1] == config.deployment_stage
                   for suffix in ('', '.fifo'))

    def feed(self):
        with open(self.args.path) as file:
            content = json.load(file)
            orig_queue = content['queue']
            messages = content['messages']
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=self.args.queue)
        logging.info('Writing messages from file "%s" to queue "%s"', self.args.path, queue.url)
        if orig_queue != queue.url:
            if self.args.force:
                logger.warning('Messages originating from queue "%s" are being fed into queue "%s"',
                               orig_queue, queue.url)
            else:
                raise RuntimeError(f'Cannot feed messages originating from "{orig_queue}" to "{queue.url}". '
                                   f'Use --force to override')
        message_batches = list(more_itertools.chunked(messages, 10))

        def _cleanup():
            if self.args.delete:
                remaining_messages = list(chain.from_iterable(message_batches))
                if len(remaining_messages) < len(messages):
                    self._dump_messages(messages, orig_queue)
                else:
                    assert len(remaining_messages) == len(messages)
                    logger.info('No messages were submitted, not touching local file "%s"', self.args.path)

        while message_batches:
            message_batch = message_batches[0]
            entries = [self._reconstitute(message) for message in message_batch]
            try:
                queue.send_messages(Entries=entries)
            except:
                assert message_batches
                _cleanup()
                raise
            message_batches.pop(0)

        if self.args.delete:
            if message_batches:
                _cleanup()
            else:
                logger.info('All messages were submitted, removing local file "%s"', self.args.path)
                os.unlink(self.args.path)


if __name__ == '__main__':
    Main.main(sys.argv[1:])
