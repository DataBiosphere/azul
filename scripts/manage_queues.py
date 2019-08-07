from itertools import chain
import os
import sys

import argparse
import time

import boto3
import json
import more_itertools
from more_itertools import one

from azul import config
import logging

from urllib.parse import urlparse

from azul.files import write_file_atomically
from azul.logging import configure_script_logging

logger = logging.getLogger(__name__)


class Main:

    @classmethod
    def main(cls, argv):
        configure_script_logging(logger)
        p = argparse.ArgumentParser(description='Manage the SQS queues in the current deployment')
        sps = p.add_subparsers(help='sub-command help', dest='command')

        sps.add_parser('list',
                       help='List SQS queues in current deployment')

        sp = sps.add_parser('dump',
                            help='Dump contents of queue into designated file')
        sp.add_argument('queue', metavar='QUEUE_NAME',
                        help='Name of the queue to obtain messages from')
        sp.add_argument('path', metavar='FILE_PATH',
                        help='Path of file to write messages to')
        sp.add_argument('--delete', '-D', action='store_true',
                        help='Remove messages from the queue after writing them to the specified file. By default the '
                             'messages will be returned to the queue')
        sp.add_argument('--no-json-body', '-J', dest='json_body', action='store_false',
                        help='Do not deserialize JSON in queue message body.')

        sp = sps.add_parser('feed', help='Feed messages from file back into queue')
        sp.add_argument('path', metavar='FILE_PATH',
                        help='Path of file to read messages from')
        sp.add_argument('queue', metavar='QUEUE_NAME',
                        help='Name of the queue to feed messages into')
        sp.add_argument('--force', '-F', action='store_true',
                        help='Force feeding messages to a queue they did not originate from.')
        sp.add_argument('--delete', '-D', action='store_true',
                        help='Remove messages from the file after submitting them to the specified queue. By default '
                             'the messages will remain in the file')

        sp = sps.add_parser('purge',
                            help='Purge all messages in a queue')
        sp.add_argument('queue', metavar='QUEUE_NAME',
                        help='Name of the queue to purge.')

        sps.add_parser('purge_all',
                       help='Purge all messages in all queues in the current deployment. Use with caution. The '
                            'messages will be lost forever.')

        sp = sps.add_parser('dump_all',
                            help='Dump all messages in all queues in the current deployment. Each queue will be '
                                 'dumped into a separate JSON file. The name of the JSON file is the name of '
                                 'the queue followed by ".json"')
        sp.add_argument('--delete', '-D', action='store_true',
                        help='Remove messages from each queue after writing them to the its file. By default the '
                             'messages will be returned to the queue')
        sp.add_argument('--no-json-body', '-J', dest='json_body', action='store_false',
                        help='Do not deserialize JSON in queue message body.')

        args = p.parse_args(argv)

        main = Main(args)
        if args.command:
            getattr(main, args.command)()
        else:
            p.print_usage()

    def __init__(self, args):
        self.args = args

    def list(self):
        logger.info('Listing queues')
        print('\n{:<35s}{:^20s}{:^20s}{:^18s}\n'.format('Queue Name',
                                                        'Messages Available',
                                                        'Messages in Flight',
                                                        'Messages Delayed'))
        queues = self._azul_queues()
        for queue_name, queue in queues:
            print('{:<35s}{:^20s}{:^20s}{:^18s}'.format(queue_name,
                                                        queue.attributes['ApproximateNumberOfMessages'],
                                                        queue.attributes['ApproximateNumberOfMessagesNotVisible'],
                                                        queue.attributes['ApproximateNumberOfMessagesDelayed']))

    def dump(self):
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=self.args.queue)
        self._dump(queue, self.args.path)

    def dump_all(self):
        for queue_name, queue in self._azul_queues():
            self._dump(queue, queue_name + '.json')

    def _dump(self, queue, path):
        logger.info('Writing messages from queue "%s" to file "%s"', queue.url, path)
        messages = []
        while True:
            message_batch = queue.receive_messages(AttributeNames=['All'],
                                                   MaxNumberOfMessages=10,
                                                   VisibilityTimeout=300)
            if not message_batch:  # Nothing left in queue
                break
            else:
                messages.extend(message_batch)
        self._dump_messages(messages, queue.url, path)
        message_batches = list(more_itertools.chunked(messages, 10))
        if self.args.delete:
            logger.info('Removing messages from queue "%s"', queue.url)
            self._delete_messages(message_batches, queue)
        else:
            logger.info('Returning messages to queue "%s"', queue.url)
            self._return_messages(message_batches, queue)
        logger.info(f'Finished writing {path !r}')

    def _dump_messages(self, messages, queue_url, path):
        messages = [self._condense(message) for message in messages]
        with write_file_atomically(path) as file:
            content = {
                'queue': queue_url,
                'messages': messages
            }
            json.dump(content, file, indent=4)
        logger.info('Wrote %i messages', len(messages))

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

    def _azul_queues(self):
        sqs = boto3.resource('sqs')
        all_queues = sqs.queues.all()
        for queue in all_queues:
            _, _, queue_name = urlparse(queue.url).path.rpartition('/')
            if self._is_azul_queue(queue_name):
                yield queue_name, queue

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
        logger.info('Writing messages from file "%s" to queue "%s"', self.args.path, queue.url)
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
                    self._dump_messages(messages, orig_queue, self.args.path)
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

    def purge(self):
        sqs = boto3.resource('sqs')
        queue_name = self.args.queue
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        self._purge(queue_name, queue)

    def purge_all(self):
        for queue_name, queue in self._azul_queues():
            self._purge(queue_name, queue)

    def _purge(self, queue_name, queue):
        # The `write` lambda recirculates messages into the same queue it is
        # reading from so we need to disable SQS push before purging the queue
        if queue_name == config.token_queue_name:
            self._manage_sqs_push(queue, enable=False)
            self._wait_for_queue_idle(queue)
        logger.info('Purging queue "%s"', queue.url)
        queue.purge()
        if queue_name == config.token_queue_name:
            self._wait_for_queue_empty(queue)
            self._manage_sqs_push(queue, enable=True)

    def _wait_for_queue_idle(self, queue):
        while True:
            num_inflight_messages = int(queue.attributes['ApproximateNumberOfMessagesNotVisible'])
            if num_inflight_messages == 0:
                break
            logger.info('Queue "%s" has %i in-flight messages', queue.url, num_inflight_messages)
            time.sleep(3)
            queue.reload()

    def _wait_for_queue_empty(self, queue):
        while True:
            num_messages = (
                int(queue.attributes['ApproximateNumberOfMessagesNotVisible']) +
                int(queue.attributes['ApproximateNumberOfMessagesDelayed']) +
                int(queue.attributes['ApproximateNumberOfMessages'])
            )
            if num_messages == 0:
                break
            logger.info('Queue "%s" still has %i messages', queue.url, num_messages)
            time.sleep(3)
            queue.reload()

    def _manage_sqs_push(self, queue, enable):
        lambda_ = boto3.client('lambda')
        function_name = config.indexer_name + '-write'
        response = lambda_.list_event_source_mappings(FunctionName=function_name,
                                                      EventSourceArn=queue.attributes['QueueArn'])
        mapping = one(response['EventSourceMappings'])

        def update_():
            logger.info('%s push from "%s" to lambda function "%s"',
                        'Enabling' if enable else 'Disabling', queue.url, function_name)
            lambda_.update_event_source_mapping(UUID=mapping['UUID'],
                                                Enabled=enable)

        while True:
            state = mapping['State']
            logger.info('Push from "%s" to lambda function "%s" is in state "%s".',
                        queue.url, function_name, state)
            if state in ('Disabling', 'Enabling'):
                pass
            elif state == 'Enabled':
                if enable:
                    break
                else:
                    update_()
            elif state == 'Disabled':
                if enable:
                    update_()
                else:
                    break
            else:
                raise NotImplementedError(state)
            time.sleep(3)
            mapping = lambda_.get_event_source_mapping(UUID=mapping['UUID'])


if __name__ == '__main__':
    Main.main(sys.argv[1:])
