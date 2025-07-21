import builtins
from collections import (
    deque,
)
from collections.abc import (
    Iterable,
    Mapping,
)
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
    as_completed,
)
from datetime import (
    datetime,
)
from enum import (
    Enum,
)
from itertools import (
    chain,
    islice,
)
import json
import logging
from math import (
    ceil,
)
import os
import time
from typing import (
    Self,
    TYPE_CHECKING,
    cast,
)
import uuid

import attrs
from chalice.app import (
    SQSRecord,
)
import more_itertools
from more_itertools import (
    chunked,
    one,
)

from azul import (
    R,
    cached_property,
    config,
)
from azul.deployment import (
    aws,
)
from azul.files import (
    write_file_atomically,
)
from azul.json import (
    Serializable,
)
from azul.lambdas import (
    Lambdas,
)
from azul.modules import (
    load_app_module,
)
from azul.types import (
    AnyJSON,
    JSON,
    json_mapping,
    json_str,
)

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import (
        ChangeMessageVisibilityBatchRequestEntryTypeDef,
        SendMessageBatchRequestEntryTypeDef,
        SendMessageRequestQueueSendMessageTypeDef,
    )
    from mypy_boto3_sqs.service_resource import (
        Message,
        Queue,
    )


@attrs.frozen(kw_only=True)
class SQSMessage:
    body: JSON

    #: Approximate number of times this message has been received, or None if
    #: this message was not received from a queue
    #:
    attempts: int | None = None

    #: The ID of this message in the queue, or None if this message was not
    #: received from a queue.
    #:
    id: str | None = None

    def to_entry(self) -> 'SendMessageRequestQueueSendMessageTypeDef':
        return {'MessageBody': json.dumps(self.body)}

    def to_batch_entry(self, id: int) -> 'SendMessageBatchRequestEntryTypeDef':
        return {**self.to_entry(), 'Id': str(id)}

    @classmethod
    def from_record(cls, record: SQSRecord) -> Self:
        attributes = json_mapping(record.to_dict()['attributes'])
        return cls(id=json_str(record.to_dict()['messageId']),
                   body=json.loads(record.body),
                   attempts=int(json_str(attributes['ApproximateReceiveCount'])))


@attrs.frozen(kw_only=True)
class SQSFifoMessage(SQSMessage):
    group_id: str
    dedup_id: str = attrs.field(factory=lambda: str(uuid.uuid4()))

    def to_entry(self) -> 'SendMessageRequestQueueSendMessageTypeDef':
        return {
            **super().to_entry(),
            'MessageGroupId': self.group_id,
            'MessageDeduplicationId': self.dedup_id
        }

    @classmethod
    def from_record(cls, record: SQSRecord) -> Self:
        attributes = json_mapping(record.to_dict()['attributes'])
        return cls(id=json_str(record.to_dict()['messageId']),
                   body=json.loads(record.body),
                   attempts=int(json_str(attributes['ApproximateReceiveCount'])),
                   group_id=json_str(attributes['MessageGroupId']),
                   dedup_id=json_str(attributes['MessageDeduplicationId']))


class Queues:
    #: The number of messages to be queued in a single SQS SendMessageBatch
    #: action. Theoretically, larger batches are better but SQS currently limits
    #: the SendMessageBatch size to 10. This is also used to configure the
    #: number of SQS messages that Lambda delivers to a function bound to a
    #: queue. Lambda can deliver at most 10 FIFO messages or 10,000 standard
    #: messages.
    #:
    batch_size = 10

    def __init__(self, delete: bool = False, json_body: bool = True):
        self._delete = delete
        self._json_body = json_body

    def list(self):
        log.info('Listing queues')
        print(f'\n{"Queue Name":<35s}'
              f'{"Messages Available":^20s}'
              f'{"Messages In Flight":^20s}'
              f'{"Messages Delayed":^18s}\n')
        queues = self.all_queues()
        for queue_name, queue in queues.items():
            print(f'{queue_name:<35s}'
                  f'{queue.attributes["ApproximateNumberOfMessages"]:^20s}'
                  f'{queue.attributes["ApproximateNumberOfMessagesNotVisible"]:^20s}'
                  f'{queue.attributes["ApproximateNumberOfMessagesDelayed"]:^18s}')

    def dump(self, queue_name: str, path: str):
        queue = aws.sqs_queue(queue_name)
        self._dump(queue, path)

    def dump_all(self):
        for queue_name, queue in self.all_queues().items():
            self._dump(queue, queue_name + '.json')

    def _dump(self, queue: 'Queue', path: str):
        log.info('Writing messages from queue %r to file %r', queue.url, path)
        messages = self._get_messages(queue)
        self._dump_messages(messages, queue.url, path)
        log.info(f'Finished writing {path!r}')
        self._cleanup_messages(queue, messages)

    def _get_messages(self, queue: 'Queue') -> builtins.list['Message']:
        messages: list['Message'] = []
        while True:
            message_batch = queue.receive_messages(AttributeNames=['All'],
                                                   MaxNumberOfMessages=10,
                                                   VisibilityTimeout=300)
            if not message_batch:  # Nothing left in queue
                return messages
            else:
                messages.extend(message_batch)

    def read_messages(self, queue: 'Queue') -> builtins.list['Message']:
        messages = self._get_messages(queue)
        self._cleanup_messages(queue, messages)
        return messages

    def send_messages(self, queue: 'Queue', messages: Iterable[SQSMessage]) -> int:
        num_messages = 0
        for batch in chunked(messages, self.batch_size):
            entries = [message.to_batch_entry(i) for i, message in enumerate(batch)]
            queue.send_messages(Entries=entries)
            num_messages += len(batch)
        return num_messages

    def send_message(self, queue: 'Queue', message: SQSMessage):
        queue.send_message(**message.to_entry())

    def _cleanup_messages(self, queue: 'Queue', messages: Iterable['Message']):
        message_batches = list(more_itertools.chunked(messages, self.batch_size))
        if self._delete:
            log.info('Removing messages from queue %r', queue.url)
            self._delete_messages(message_batches, queue)
        else:
            log.info('Returning messages to queue %r', queue.url)
            self._return_messages(message_batches, queue)

    def _dump_messages(self,
                       messages: Iterable['Message'],
                       queue_url: str,
                       path: str):
        messages = [self._condense(message) for message in messages]
        with write_file_atomically(path) as file:
            content = {
                'queue': queue_url,
                'messages': messages
            }
            json.dump(content, file, indent=4)
        log.info('Wrote %i messages', len(messages))

    def _return_messages(self,
                         message_batches: Iterable[Iterable['Message']],
                         queue: 'Queue'):
        for message_batch in message_batches:
            batch: list['ChangeMessageVisibilityBatchRequestEntryTypeDef'] = [
                dict(Id=message.message_id,
                     ReceiptHandle=message.receipt_handle,
                     VisibilityTimeout=0)
                for message in message_batch
            ]
            response = queue.change_message_visibility_batch(Entries=batch)
            if len(response['Successful']) != len(batch):
                raise RuntimeError(f'Failed to return message: {response!r}')

    def _delete_messages(self,
                         message_batches: Iterable[builtins.list['Message']],
                         queue: 'Queue'):
        for message_batch in message_batches:
            response = queue.delete_messages(
                Entries=[dict(Id=message.message_id,
                              ReceiptHandle=message.receipt_handle) for message in message_batch])
            if len(response['Successful']) != len(message_batch):
                raise RuntimeError(f'Failed to delete messages: {response!r}')

    def _condense(self, message: 'Message') -> JSON:
        """
        Prepare a message for writing to a local file.
        """
        # The cast is needed because the type stub for `Message` misuses `typing.Literal`
        attributes = cast(dict[str, str], message.attributes)
        return {
            'MessageId': message.message_id,
            'ReceiptHandle': message.receipt_handle,
            'MD5OfBody': message.md5_of_body,
            'Body': json.loads(message.body) if self._json_body else message.body,
            'Attributes': json_mapping(attributes),
            '_Attributes': {
                k: datetime.fromtimestamp(int(json_str(attributes[k])) / 1000).astimezone().isoformat()
                for k in ('SentTimestamp', 'ApproximateFirstReceiveTimestamp')
            }
        }

    def _reconstitute(self, message: JSON) -> 'SendMessageBatchRequestEntryTypeDef':
        """
        Prepare a message from a local file for submission to a queue.

        The inverse of _condense().
        """
        body = message['Body']
        if not isinstance(body, str):
            body = json.dumps(body)
        attributes = json_mapping(message['Attributes'])
        result: 'SendMessageBatchRequestEntryTypeDef' = {
            'Id': json_str(message['MessageId']),
            'MessageBody': body,
        }
        for key in ('MessageGroupId', 'MessageDeduplicationId'):
            try:
                result[key] = json_str(attributes[key])
            except KeyError:
                pass
        return result

    def all_queues(self) -> dict[str, 'Queue']:
        return self.get_queues(config.all_queue_names)

    def get_queues(self, queue_names: Iterable[str]) -> dict[str, 'Queue']:
        return {
            queue_name: aws.sqs_queue(queue_name)
            for queue_name in queue_names
        }

    def get_queue_lengths(self,
                          queues: Mapping[str, 'Queue']
                          ) -> tuple[int, dict[str, int]]:
        """
        Count the number of messages in the given queues.

        :param queues: A dictionary of Boto3 queue resources by name.

        :return: A tuple of the total number of messages in all queues and a
                 dictionary mapping each queue's name to the number of messages
                 in that queue.
        """
        total, lengths = 0, {}
        for queue_name, queue in queues.items():
            queue.reload()
            message_counts = [
                int(queue.attributes['ApproximateNumberOfMessages']),
                int(queue.attributes['ApproximateNumberOfMessagesNotVisible']),
                int(queue.attributes['ApproximateNumberOfMessagesDelayed']),
            ]
            length = sum(message_counts)
            log.debug('Queue %s has %i message(s) (%i available, %i in flight and %i delayed).',
                      queue_name, length, *message_counts)
            total += length
            lengths[queue_name] = length
        return total, lengths

    def wait_to_stabilize(self,
                          queue_names: Iterable[str],
                          timeout: int,
                          *,
                          detect_stall: bool
                          ) -> int:
        """
        Wait for queues to reach a steady state.

        :param queue_names: Which queues to wait for.

        :param timeout: The highest timeout among lambda functions receiving
                        messages from the queues.

        :param detect_stall: If True, the method will raise an exception if
                             there is no observable progress while the queues
                             are nonempty. Otherwise, the method will wait
                             indefinitely for the queues to empty.

        :return: The total final length of the stabilized queues. The only
                 observable is zero; otherwise, an exception is raised.
        """
        sleep_time = 10
        queues = self.get_queues(queue_names)
        maxlen = ceil(timeout / sleep_time)
        total_lengths: deque[int] = deque(maxlen=maxlen)
        # Two minutes to safely accommodate SQS eventual consistency window of
        # one minute. For more info, read WARNING section on
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.get_queue_attributes
        assert maxlen * sleep_time >= 2 * 60

        while True:
            # Determine queue lengths
            total_length, queue_lengths = self.get_queue_lengths(queues)
            total_lengths.append(total_length)
            log.info('Counting %i messages in %i queues.',
                     total_length, len(queue_lengths))
            log.info('Message count history (most recent first) is %r.',
                     list(reversed(total_lengths)))

            min_num_zeros = 60 // sleep_time
            assert min_num_zeros <= maxlen, min_num_zeros
            num_total_lengths = len(total_lengths)
            if num_total_lengths >= min_num_zeros:
                if not any(islice(reversed(total_lengths), min_num_zeros)):
                    final_length = total_lengths[-1]
                    log.info('The queues have emptied.')
                    break
                elif detect_stall and num_total_lengths == total_lengths.maxlen:
                    cummdiff = sum(
                        abs(first - second)
                        for first, second in more_itertools.pairwise(total_lengths)
                    )
                    if cummdiff == 0:
                        final_length = total_lengths[-1]
                        log.info('The queues have stabilized.')
                        break

            log.info('Waiting for %s queue(s) to stabilize ...', len(queues))
            time.sleep(sleep_time)

        if final_length != 0:
            raise Exception('The queues have stalled', final_length)
        return final_length

    def feed(self, path: str, queue_name: str, force: bool = False):
        with open(path) as file:
            content = json.load(file)
            orig_queue = content['queue']
            messages = content['messages']
        queue = aws.sqs_queue(queue_name)
        log.info('Writing messages from file %r to queue %r', path, queue.url)
        if orig_queue != queue.url:
            if force:
                log.warning('Messages originating from queue %r are being fed into queue %r',
                            orig_queue, queue.url)
            else:
                raise RuntimeError(f'Cannot feed messages originating from {orig_queue!r} to {queue.url!r}. '
                                   f'Use --force to override.')
        message_batches = list(more_itertools.chunked(messages, self.batch_size))

        def _cleanup():
            if self._delete:
                remaining_messages = list(chain.from_iterable(message_batches))
                if len(remaining_messages) < len(messages):
                    self._dump_messages(messages, orig_queue, path)
                else:
                    assert len(remaining_messages) == len(messages)
                    log.info('No messages were submitted, not touching local file %r', path)

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
                log.info('All messages were submitted, removing local file %r', path)
                os.unlink(path)

    def purge(self, queue_name: str):
        queues = self.get_queues([queue_name])
        self.purge_queues_safely(queues)

    def purge_all(self):
        self.purge_queues_safely(self.all_queues())

    def purge_indexer(self):
        queues = self.get_queues(config.indexer_work_queue_names)
        self.purge_queues_safely(queues)

    def purge_mirror(self):
        queues = self.get_queues(config.mirror_work_queue_names)
        self.purge_queues_safely(queues)

    def purge_queues_safely(self, queues: Mapping[str, 'Queue']):
        self.manage_lambdas(queues, enable=False)
        self.purge_queues_unsafely(queues)
        self.manage_lambdas(queues, enable=True)

    def purge_queues_unsafely(self, queues: Mapping[str, 'Queue']):
        with ThreadPoolExecutor(max_workers=len(queues)) as tpe:
            futures = [tpe.submit(self._purge_queue, queue) for queue in queues.values()]
            self._handle_futures(futures)

    def _purge_queue(self, queue: 'Queue'):
        log.info('Purging queue %r', queue.url)
        queue.purge()
        self._wait_for_queue_empty(queue)

    def _wait_for_queue_idle(self, queue: 'Queue'):
        while True:
            num_inflight_messages = int(queue.attributes['ApproximateNumberOfMessagesNotVisible'])
            if num_inflight_messages == 0:
                break
            log.info('Queue %r has %i in-flight messages', queue.url, num_inflight_messages)
            time.sleep(3)
            queue.reload()

    def _wait_for_queue_empty(self, queue: 'Queue'):
        while True:
            num_messages = (
                int(queue.attributes['ApproximateNumberOfMessages']) +
                int(queue.attributes['ApproximateNumberOfMessagesDelayed']) +
                int(queue.attributes['ApproximateNumberOfMessages'])
            )
            if num_messages == 0:
                break
            log.info('Queue %r still has %i messages', queue.url, num_messages)
            time.sleep(3)
            queue.reload()

    def _manage_sqs_push(self, function_name: str, queue: 'Queue', enable: bool):
        lambda_ = aws.lambda_
        response = lambda_.list_event_source_mappings(FunctionName=function_name,
                                                      EventSourceArn=queue.attributes['QueueArn'])
        mapping_uuid = one(response['EventSourceMappings'])['UUID']

        def update_():
            log.info('%s push from %r to lambda function %r',
                     'Enabling' if enable else 'Disabling', queue.url, function_name)
            lambda_.update_event_source_mapping(UUID=mapping_uuid, Enabled=enable)

        state = one(response['EventSourceMappings'])['State']
        while True:
            log.info('Push from %r to lambda function %r is in state %r.',
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
            state = lambda_.get_event_source_mapping(UUID=mapping_uuid)['State']

    def functions_by_queue(self) -> dict[str, str]:
        """
        Returns a dictionary that maps queues to the Lambda function triggered
        by the queue. The keys and values are fully qualified resource names.
        """
        indexer = load_app_module('indexer')
        functions_by_queue = {
            handler.queue: config.indexer_function_name(handler.name)
            for handler in indexer.app.handler_map.values()
            if hasattr(handler, 'queue')
        }
        invalid_queues = functions_by_queue.keys() - set(config.all_queue_names)
        assert not invalid_queues, invalid_queues
        return functions_by_queue

    def manage_lambdas(self, queues: Mapping[str, 'Queue'], enable: bool):
        """
        Enable or disable the readers and writers of the given queues.
        """
        functions_by_queue = self.functions_by_queue()

        with ThreadPoolExecutor(max_workers=len(queues)) as tpe:
            futures = []

            def submit(f, *args, **kwargs):
                futures.append(tpe.submit(f, *args, **kwargs))

            for queue_name, queue in queues.items():
                try:
                    function = functions_by_queue[queue_name]
                except KeyError:
                    assert queue_name in config.fail_queue_names
                else:
                    if queue_name == config.notifications_queue.name:
                        # Prevent new notifications from being added
                        submit(self._manage_lambda, config.indexer_name, enable)
                    submit(self._manage_sqs_push, function, queue, enable)
            self._handle_futures(futures)
            futures = [tpe.submit(self._wait_for_queue_idle, queue) for queue in queues.values()]
            self._handle_futures(futures)

    def _manage_lambda(self, function_name: str, enable: bool):
        self._lambdas.manage_lambda(function_name, enable)

    @cached_property
    def _lambdas(self) -> Lambdas:
        return Lambdas()

    def _handle_futures(self, futures: Iterable[Future]):
        errors = []
        for future in as_completed(futures):
            e = future.exception()
            if e:
                errors.append(e)
                log.error('Exception in worker thread', exc_info=e)
        if errors:
            raise RuntimeError(errors)


class Action(Serializable, Enum):

    @classmethod
    def from_json(cls, action: AnyJSON) -> Self:
        assert isinstance(action, str), R('Action is not a string', type(action))
        try:
            return cls[action]
        except KeyError:
            assert False, R('Invalid action', action)

    def to_json(self) -> str:
        return self.name
