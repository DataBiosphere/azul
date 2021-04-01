from collections import (
    deque,
)
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
from datetime import (
    datetime,
)
from itertools import (
    chain,
)
import json
import logging
from math import (
    ceil,
)
import os
import time
from typing import (
    Any,
    Iterable,
    Mapping,
    Tuple,
)

import more_itertools
from more_itertools import (
    one,
)

from azul import (
    cached_property,
    config,
)
from azul.deployment import (
    aws,
)
from azul.files import (
    write_file_atomically,
)
from azul.lambdas import (
    Lambdas,
)
from azul.modules import (
    load_app_module,
)

logger = logging.getLogger(__name__)

Queue = Any  # place-holder for boto3's SQS queue resource


class Queues:

    def __init__(self, delete=False, json_body=True):
        self._delete = delete
        self._json_body = json_body

    def list(self):
        logger.info('Listing queues')
        print(f'\n{"Queue Name":<35s}'
              f'{"Messages Available":^20s}'
              f'{"Messages In Flight":^20s}'
              f'{"Messages Delayed":^18s}\n')
        queues = self.azul_queues()
        for queue_name, queue in queues.items():
            print(f'{queue_name:<35s}'
                  f'{queue.attributes["ApproximateNumberOfMessages"]:^20s}'
                  f'{queue.attributes["ApproximateNumberOfMessagesNotVisible"]:^20s}'
                  f'{queue.attributes["ApproximateNumberOfMessagesDelayed"]:^18s}')

    def dump(self, queue_name, path):
        queue = self.sqs.get_queue_by_name(QueueName=queue_name)
        self._dump(queue, path)

    @property
    def sqs(self):
        return aws.resource('sqs')

    def dump_all(self):
        for queue_name, queue in self.azul_queues().items():
            self._dump(queue, queue_name + '.json')

    def _dump(self, queue, path):
        logger.info('Writing messages from queue %r to file %r', queue.url, path)
        messages = self._get_messages(queue)
        self._dump_messages(messages, queue.url, path)
        logger.info(f'Finished writing {path!r}')
        self._cleanup_messages(queue, messages)

    def _get_messages(self, queue):
        messages = []
        while True:
            message_batch = queue.receive_messages(AttributeNames=['All'],
                                                   MaxNumberOfMessages=10,
                                                   VisibilityTimeout=300)
            if not message_batch:  # Nothing left in queue
                return messages
            else:
                messages.extend(message_batch)

    def read_messages(self, queue):
        messages = self._get_messages(queue)
        self._cleanup_messages(queue, messages)
        return messages

    def _cleanup_messages(self, queue, messages):
        message_batches = list(more_itertools.chunked(messages, 10))
        if self._delete:
            logger.info('Removing messages from queue %r', queue.url)
            self._delete_messages(message_batches, queue)
        else:
            logger.info('Returning messages to queue %r', queue.url)
            self._return_messages(message_batches, queue)

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
            '_Attributes': {
                k: datetime.fromtimestamp(int(message.attributes[k]) / 1000).astimezone().isoformat()
                for k in ('SentTimestamp', 'ApproximateFirstReceiveTimestamp')
            }
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

    def get_queues(self, queue_names: Iterable[str]) -> Mapping[str, Queue]:
        return {
            queue_name: self.sqs.get_queue_by_name(QueueName=queue_name)
            for queue_name in queue_names
        }

    def _get_queue_lengths(self, queues: Mapping[str, Queue]) -> Tuple[int, Mapping[str, int]]:
        """
        Count the number of messages in the given queues.

        :param queues: A dictionary of Boto3 queue resources by name.

        :return: A tuple of the total number of messages in all queues and a
                 dictionary mapping each queue's name to the number of messages
                 in that queue.
        """
        attributes = [
            'ApproximateNumberOfMessages' + suffix
            for suffix in ('', 'NotVisible', 'Delayed')
        ]
        total, lengths = 0, {}
        for queue_name, queue in queues.items():
            queue.reload()
            message_counts = [int(queue.attributes[attribute]) for attribute in attributes]
            length = sum(message_counts)
            logger.debug('Queue %s has %i message(s) (%i available, %i in flight and %i delayed).',
                         queue_name, length, *message_counts)
            total += length
            lengths[queue_name] = length
        return total, lengths

    def wait_to_stabilize(self) -> int:
        """
        Wait for the work queues to reach a steady state.
        """
        sleep_time = 10
        queues = self.get_queues(config.work_queue_names)
        # A generous 10 minutes to accommodate transient stalls
        total_lengths = deque(maxlen=ceil(10 * 60 / sleep_time))
        # Two minutes to safely accommodate SQS eventual consistency window of
        # one minute. For more info, read WARNING section on
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.get_queue_attributes
        assert total_lengths.maxlen * sleep_time >= 2 * 60

        while True:
            # Determine queue lengths
            total_length, queue_lengths = self._get_queue_lengths(queues)
            total_lengths.append(total_length)
            logger.info('Counting %i messages in %i queues.',
                        total_length, len(queue_lengths))
            logger.info('Message count history (most recent first) is %r.',
                        list(reversed(total_lengths)))

            if len(total_lengths) == total_lengths.maxlen:
                cummdiff = sum(
                    abs(first - second)
                    for first, second in more_itertools.pairwise(total_lengths)
                )
                if cummdiff == 0:
                    final_length = total_lengths[-1]
                    logger.info('The queues have stabilized.')
                    break

            logger.info('Waiting for %s queue(s) to stabilize ...', len(queues))
            time.sleep(sleep_time)

        if final_length != 0:
            raise Exception('The queues have stalled', final_length)
        return final_length

    def feed(self, path, queue_name, force=False):
        with open(path) as file:
            content = json.load(file)
            orig_queue = content['queue']
            messages = content['messages']
        queue = self.sqs.get_queue_by_name(QueueName=queue_name)
        logger.info('Writing messages from file %r to queue %r', path, queue.url)
        if orig_queue != queue.url:
            if force:
                logger.warning('Messages originating from queue %r are being fed into queue %r',
                               orig_queue, queue.url)
            else:
                raise RuntimeError(f'Cannot feed messages originating from {orig_queue!r} to {queue.url!r}. '
                                   f'Use --force to override.')
        message_batches = list(more_itertools.chunked(messages, 10))

        def _cleanup():
            if self._delete:
                remaining_messages = list(chain.from_iterable(message_batches))
                if len(remaining_messages) < len(messages):
                    self._dump_messages(messages, orig_queue, path)
                else:
                    assert len(remaining_messages) == len(messages)
                    logger.info('No messages were submitted, not touching local file %r', path)

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
                logger.info('All messages were submitted, removing local file %r', path)
                os.unlink(path)

    def purge(self, queue_name):
        queue = self.sqs.get_queue_by_name(QueueName=queue_name)
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
        logger.info('Purging queue %r', queue.url)
        queue.purge()
        self._wait_for_queue_empty(queue)

    def _wait_for_queue_idle(self, queue: Queue):
        while True:
            num_inflight_messages = int(queue.attributes['ApproximateNumberOfMessagesNotVisible'])
            if num_inflight_messages == 0:
                break
            logger.info('Queue %r has %i in-flight messages', queue.url, num_inflight_messages)
            time.sleep(3)
            queue.reload()

    def _wait_for_queue_empty(self, queue: Queue):
        # Gotta have some fun some of the time
        attribute_names = tuple(map('ApproximateNumberOfMessages'.__add__, ('', 'Delayed', 'NotVisible')))
        while True:
            num_messages = sum(map(int, map(queue.attributes.get, attribute_names)))
            if num_messages == 0:
                break
            logger.info('Queue %r still has %i messages', queue.url, num_messages)
            time.sleep(3)
            queue.reload()

    def _manage_sqs_push(self, function_name, queue, enable: bool):
        lambda_ = aws.client('lambda')
        response = lambda_.list_event_source_mappings(FunctionName=function_name,
                                                      EventSourceArn=queue.attributes['QueueArn'])
        mapping = one(response['EventSourceMappings'])

        def update_():
            logger.info('%s push from %r to lambda function %r',
                        'Enabling' if enable else 'Disabling', queue.url, function_name)
            lambda_.update_event_source_mapping(UUID=mapping['UUID'],
                                                Enabled=enable)

        while True:
            state = mapping['State']
            logger.info('Push from %r to lambda function %r is in state %r.',
                        queue.url, function_name, state)
            if state in ('Disabling', 'Enabling', 'Updating'):
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
        indexer = load_app_module('indexer')
        functions_by_queue = {
            handler.queue: [config.indexer_function_name(handler.name)]
            for handler in indexer.app.handler_map.values()
            if hasattr(handler, 'queue')
        }
        # Since the indexer lambda writes to the notifications queue, we must
        # deactivate it also.
        notifications_queue = config.notifications_queue_name()
        functions_by_queue[notifications_queue].append(config.indexer_name)

        with ThreadPoolExecutor(max_workers=len(queues)) as tpe:
            futures = []

            def submit(f, *args, **kwargs):
                futures.append(tpe.submit(f, *args, **kwargs))

            for queue_name, queue in queues.items():
                try:
                    functions = functions_by_queue[queue_name]
                except KeyError:
                    assert queue_name in config.fail_queue_names
                else:
                    for function in functions:
                        submit(self._manage_lambda, function, enable)
            self._handle_futures(futures)
            futures = [tpe.submit(self._wait_for_queue_idle, queue) for queue in queues.values()]
            self._handle_futures(futures)

    def _manage_lambda(self, function_name, enable: bool):
        self._lambdas.manage_lambda(function_name, enable)

    @cached_property
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
