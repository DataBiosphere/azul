from collections import deque
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
from itertools import chain
import json
import logging
import os
import time
from typing import (
    Any,
    List,
    Mapping,
)

from boltons.cacheutils import cachedproperty
import boto3
import more_itertools
from more_itertools import one

from azul import config
from azul.files import write_file_atomically
from azul.lambdas import Lambdas

logger = logging.getLogger(__name__)

Queue = Any  # place-holder for boto3's SQS queue resource


class Queues:

    def __init__(self, delete=False, json_body=True):
        self._delete = delete
        self._json_body = json_body

    def list(self):
        logger.info('Listing queues')
        print('\n{:<35s}{:^20s}{:^20s}{:^18s}\n'.format('Queue Name',
                                                        'Messages Available',
                                                        'Messages in Flight',
                                                        'Messages Delayed'))
        queues = self.azul_queues()
        for queue_name, queue in queues.items():
            print('{:<35s}{:^20s}{:^20s}{:^18s}'.format(queue_name,
                                                        queue.attributes['ApproximateNumberOfMessages'],
                                                        queue.attributes['ApproximateNumberOfMessagesNotVisible'],
                                                        queue.attributes['ApproximateNumberOfMessagesDelayed']))

    def dump(self, queue_name, path):
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        self._dump(queue, path)

    def dump_all(self):
        for queue_name, queue in self.azul_queues().items():
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
        if self._delete:
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
            'Body': json.loads(message.body) if self._json_body else message.body,
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

    def azul_queues(self):
        return self.get_queues(config.all_queue_names)

    @classmethod
    def get_queues(cls, queue_names: List[str]) -> Mapping[str, Any]:
        sqs = boto3.resource('sqs')
        return {queue_name: sqs.get_queue_by_name(QueueName=queue_name) for queue_name in queue_names}

    @classmethod
    def count_messages(cls, queues: Mapping[str, Any]) -> int:
        """
        Count the number of messages in the given queues

        :param queues: A collection of mapped queue_names to queues
        :return: Total count of available, in flight, and delayed messages
        """
        attribute_names = ['ApproximateNumberOfMessages' + suffix for suffix in ('', 'NotVisible', 'Delayed')]
        total_message_count = 0
        for queue_name, queue in queues.items():
            queue.reload()
            message_counts = [int(queue.attributes[attribute_name]) for attribute_name in attribute_names]
            queue_length = sum(message_counts)
            logger.debug('Queue %s has %i message(s) (%i available, %i in flight and %i delayed).',
                         queue_name, queue_length, *message_counts)
            total_message_count += queue_length
        logger.info('Counting %i message(s) in %i queue(s).', total_message_count, len(queues))
        return total_message_count

    @classmethod
    def wait_for_queue_level(cls, queue_names, empty: bool = True, num_bundles: int = None):
        """
        Wait until the total count of queued messages reaches the desired level

        :param queue_names: The names of the queues to check messages counts in
        :param empty: True to wait until the queues are empty, False to wait until not empty.
        :param num_bundles: Number of bundles being indexed (None = many bundles)
        """
        sleep_time = 5
        deque_size = 10 if empty else 1
        queues = cls.get_queues(queue_names)
        queue_size_history = deque(maxlen=deque_size)
        wait_start_time = time.time()

        if not empty:
            timeout = 2 * 60
        elif num_bundles is None:
            timeout = 60 * 60
        else:
            # calculate timeout for queues to empty with a given number of bundles
            time_to_clear_deque = deque_size * sleep_time
            time_processing_bundles = num_bundles * 5  # small time per bundle
            timeout = max(time_processing_bundles + time_to_clear_deque, 60)  # at least 1 min
            timeout = min(timeout, 60 * 60)  # at most 60 min

        logger.info('Waiting up to %s seconds for %s queues to %s ...',
                    timeout, len(queues), 'empty' if empty else 'not be empty')
        while True:
            total_message_count = cls.count_messages(queues)
            queue_wait_time_elapsed = (time.time() - wait_start_time)
            queue_size_history.append(total_message_count)
            cumulative_queue_size = sum(queue_size_history)
            if queue_wait_time_elapsed > timeout:
                logger.error('The queue(s) are NOT at the desired level.')
                return
            elif (cumulative_queue_size == 0) == empty and len(queue_size_history) == deque_size:
                logger.info('The queue(s) are at the desired level.')
                break
            else:
                logger.info('The most recently sampled queue sizes are %r.', queue_size_history)
            time.sleep(5)

    def feed(self, path, queue_name, force=False):
        with open(path) as file:
            content = json.load(file)
            orig_queue = content['queue']
            messages = content['messages']
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        logger.info('Writing messages from file "%s" to queue "%s"', path, queue.url)
        if orig_queue != queue.url:
            if force:
                logger.warning('Messages originating from queue "%s" are being fed into queue "%s"',
                               orig_queue, queue.url)
            else:
                raise RuntimeError(f'Cannot feed messages originating from "{orig_queue}" to "{queue.url}". '
                                   f'Use --force to override')
        message_batches = list(more_itertools.chunked(messages, 10))

        def _cleanup():
            if self._delete:
                remaining_messages = list(chain.from_iterable(message_batches))
                if len(remaining_messages) < len(messages):
                    self._dump_messages(messages, orig_queue, path)
                else:
                    assert len(remaining_messages) == len(messages)
                    logger.info('No messages were submitted, not touching local file "%s"', path)

        while message_batches:
            message_batch = message_batches[0]
            entries = [self._reconstitute(message) for message in message_batch]
            try:
                queue.send_messages(Entries=entries)
            except BaseException:
                assert message_batches
                _cleanup()
                raise
            message_batches.pop(0)

        if self._delete:
            if message_batches:
                _cleanup()
            else:
                logger.info('All messages were submitted, removing local file "%s"', path)
                os.unlink(path)

    def purge(self, queue_name):
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        self.purge_queues_safely({queue_name: queue})

    def purge_all(self):
        self.purge_queues_safely(self.azul_queues())

    def purge_queues_safely(self, queues: Mapping[str, Queue]):
        self.manage_lambdas(queues, enable=False)
        self.purge_queues_unsafely(queues)
        self.manage_lambdas(queues, enable=True)

    def purge_queues_unsafely(self, queues: Mapping[str, Queue]):
        with ThreadPoolExecutor(max_workers=len(queues)) as tpe:
            futures = [tpe.submit(self._purge_queue, queue) for queue in queues.values()]
            self._handle_futures(futures)

    def _purge_queue(self, queue: Queue):
        logger.info('Purging queue "%s"', queue.url)
        queue.purge()
        self._wait_for_queue_empty(queue)

    def _wait_for_queue_idle(self, queue: Queue):
        while True:
            num_inflight_messages = int(queue.attributes['ApproximateNumberOfMessagesNotVisible'])
            if num_inflight_messages == 0:
                break
            logger.info('Queue "%s" has %i in-flight messages', queue.url, num_inflight_messages)
            time.sleep(3)
            queue.reload()

    def _wait_for_queue_empty(self, queue: Queue):
        # Gotta have some fun some of the time
        attribute_names = tuple(map('ApproximateNumberOfMessages'.__add__, ('', 'Delayed', 'NotVisible')))
        while True:
            num_messages = sum(map(int, map(queue.attributes.get, attribute_names)))
            if num_messages == 0:
                break
            logger.info('Queue "%s" still has %i messages', queue.url, num_messages)
            time.sleep(3)
            queue.reload()

    def _manage_sqs_push(self, function_name, queue, enable: bool):
        lambda_ = boto3.client('lambda')
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

    def manage_lambdas(self, queues: Mapping[str, Queue], enable: bool):
        """
        Enable or disable the readers and writers of the given queues
        """
        with ThreadPoolExecutor(max_workers=len(queues)) as tpe:
            futures = []
            for queue_name, queue in queues.items():
                if queue_name == config.notify_queue_name:
                    futures.append(tpe.submit(self._manage_lambda, config.indexer_name, enable))
                    futures.append(tpe.submit(self._manage_sqs_push, config.indexer_name + '-index', queue, enable))
                elif queue_name == config.token_queue_name:
                    futures.append(tpe.submit(self._manage_sqs_push, config.indexer_name + '-write', queue, enable))
            self._handle_futures(futures)
            futures = [tpe.submit(self._wait_for_queue_idle, queue) for queue in queues.values()]
            self._handle_futures(futures)

    def _manage_lambda(self, function_name, enable: bool):
        self._lambdas.manage_lambda(function_name, enable)

    @cachedproperty
    def _lambdas(self):
        return Lambdas()

    def _handle_futures(self, futures):
        errors = []
        for future in as_completed(futures):
            e = future.exception()
            if e:
                errors.append(e)
                logger.error('Exception in worker thread', exc_info=e)
        if errors:
            raise RuntimeError(errors)
