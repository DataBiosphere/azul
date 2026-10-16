import builtins
from collections import (
    deque,
)
from collections.abc import (
    Iterable,
    Iterator,
    Mapping,
)
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
    as_completed,
)
from contextlib import (
    contextmanager,
)
from datetime import (
    datetime,
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
    ClassVar,
    Self,
    TYPE_CHECKING,
    TextIO,
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
    only,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.lambdas import (
    LambdaFunctions,
)
from azul.lib import (
    cached_property,
)
from azul.lib.attrs import (
    DiscriminatingPolymorphicSerializableAttrs,
    SerializableAttrs,
)
from azul.lib.files import (
    write_file_atomically,
)
from azul.lib.json import (
    StaticRegisteredPolymorphicSerializable,
)
from azul.lib.types import (
    JSON,
    json_mapping,
    json_str,
)
from azul.modules import (
    load_app_module,
)

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from mypy_boto3_sqs.literals import (
        QueueAttributeNameType,
    )
    from mypy_boto3_sqs.service_resource import (
        Message,
        Queue,
    )
    from mypy_boto3_sqs.type_defs import (
        ChangeMessageVisibilityBatchRequestEntryTypeDef,
        ListMessageMoveTasksResultEntryTypeDef,
        SendMessageBatchRequestEntryTypeDef,
        SendMessageRequestQueueSendMessageTypeDef,
    )


@attrs.frozen(kw_only=True, repr=False)
class SQSMessage(SerializableAttrs):
    body: JSON

    #: Approximate number of times this message has been received, or None if
    #: this message was not received from a queue
    #:
    attempts: int | None = None

    #: The ID of this message in the queue, or None if this message was not
    #: received from a queue.
    #:
    id: str | None = None

    def to_entry(self) -> SendMessageRequestQueueSendMessageTypeDef:
        return {'MessageBody': json.dumps(self.body)}

    def to_batch_entry(self, id: int) -> SendMessageBatchRequestEntryTypeDef:
        return {**self.to_entry(), 'Id': str(id)}

    @classmethod
    def from_record(cls, record: SQSRecord) -> Self:
        attributes = json_mapping(record.to_dict()['attributes'])
        return cls(id=json_str(record.to_dict()['messageId']),
                   body=json.loads(record.body),
                   attempts=int(json_str(attributes['ApproximateReceiveCount'])))

    def __repr__(self) -> str:
        # We profusely log instances of this class, so we would like their
        # logged form to benefit from CloudWatch's automatic parsing of JSON
        return json.dumps(self.to_json())


@attrs.frozen(kw_only=True)
class SQSFifoMessage(SQSMessage):
    group_id: str
    dedup_id: str = attrs.field(factory=lambda: str(uuid.uuid4()))

    def to_entry(self) -> SendMessageRequestQueueSendMessageTypeDef:
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


@attrs.frozen(kw_only=True, slots=False)
class Queues:
    #: Whether to remove the messages from the queue they are dumped from, and
    #: from the file they are fed from
    #:
    _delete: bool = False

    #: If True, disable the queue worker lambdas for the duration of a dump.
    #: If False, leave them enabled even if the queues hold messages. If None,
    #: force a decision unless the work queues are empty.
    #:
    _safe_dump: bool | None = None

    #: The number of messages to be queued in a single SQS SendMessageBatch
    #: action. Theoretically, larger batches are better but SQS currently limits
    #: the SendMessageBatch size to 10. This is also used to configure the
    #: number of SQS messages that Lambda delivers to a function bound to a
    #: queue. Lambda can deliver at most 10 FIFO messages or 10,000 standard
    #: messages.
    #:
    batch_size = 10

    #: The number of seconds to wait between consecutive polls of the progress
    #: of a message move task. Such a task may run for up to 36 hours.
    #:
    redrive_poll_interval = 10

    #: The number of seconds to wait between consecutive polls of the queue
    #: lengths in :meth:`wait_to_stabilize`. That method derives the length of
    #: its observation window from this interval, and asserts that the window
    #: covers the SQS consistency delay, so this value can't be raised freely.
    #:
    stabilization_poll_interval = 10

    #: The number of seconds a receive waits for messages to arrive. Any value
    #: above zero selects long polling, which queries all SQS hosts instead of
    #: a sample of them and therefore eliminates the empty responses a short
    #: poll can return while messages remain. The documented maximum is used
    #: because AWS notes that lower values make such responses more likely.
    #:
    receive_wait_time = 20

    #: The number of consecutive empty receives after which a queue is
    #: considered drained even though it still reports messages. Each of those
    #: receives long-polls every SQS host, but the reported counts are
    #: approximate, and a queue whose producers are still running may never
    #: report zero. AWS prescribes observing the counts instead of trusting a
    #: single empty receive, but their procedure also assumes that the
    #: producers of the queue have been stopped.
    #:
    #: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/confirm-queue-is-empty.html
    #:
    max_empty_receives = 3

    #: The names of the queue attributes counting the messages that are
    #: available, in flight and delayed, respectively. All three are
    #: approximate.
    #:
    message_count_attributes: ClassVar[tuple[QueueAttributeNameType, ...]] = (
        'ApproximateNumberOfMessages',
        'ApproximateNumberOfMessagesNotVisible',
        'ApproximateNumberOfMessagesDelayed'
    )

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
        with self._managed_lambdas():
            self._dump(queue, path)

    def dump_all(self):
        with self._managed_lambdas():
            for queue_name, queue in self.all_queues().items():
                self._dump(queue, queue_name + '.jsonl')

    @contextmanager
    def _managed_lambdas(self) -> Iterator[None]:
        queues = self.get_queues(config.work_queue_names)
        if self._safe_dump is True:
            self.manage_lambdas(queues, enable=False)
            try:
                yield
            finally:
                self.manage_lambdas(queues, enable=True)
        else:
            num_messages, _ = self.get_queue_lengths(queues)
            if num_messages == 0:
                yield
                log.warning(
                    'The work queues were empty before the dump, but messages arriving '
                    'afterwards could still have been snatched up by the lambdas'
                )
            elif self._safe_dump is None:
                raise RuntimeError(
                    'The work queues hold messages. Pass --safe to disable the lambdas '
                    'during the dump, or --unsafe to dump anyway.',
                    num_messages
                )
            elif self._delete:
                raise RuntimeError(
                    'The work queues hold messages, making --delete unsafe. '
                    'Pass --safe instead of --unsafe.',
                    num_messages
                )
            else:
                yield
                log.warning(
                    'The work queues held %i message(s) before the dump, and more have '
                    'likely arrived during the dump, so the dump is probably incomplete',
                    num_messages
                )

    def _dump(self, queue: Queue, path: str):
        log.info('Writing messages from queue %r to file %r', queue.url, path)
        if self._is_fifo(queue) and not self._delete:
            log.warning('Without --delete, only the messages at the head of '
                        'each group will be dumped from FIFO queue %r', queue.url)
        # Messages that are not deleted must be held in flight until the dump
        # ends, or receiving them again would make the dump run forever
        messages: list[Message] = []
        num_messages, num_empty_receives = 0, 0
        with open(path, 'w') as file:
            self._write_line(file, {'queue': queue.url})
            while True:
                message_batch = self._receive_messages(queue)
                if message_batch:
                    num_empty_receives = 0
                    for message in message_batch:
                        self._write_line(file, self._condense(message))
                    file.flush()
                    os.fsync(file.fileno())
                    num_messages += len(message_batch)
                    if self._delete:
                        self._delete_messages([message_batch], queue)
                    else:
                        messages.extend(message_batch)
                else:
                    num_empty_receives += 1
                    num_remaining = self._num_remaining_messages(queue, len(messages))
                    if num_remaining == 0:
                        break
                    elif num_empty_receives < self.max_empty_receives:
                        log.info('Queue %r reports %i message(s) left after an '
                                 'empty receive, polling it again',
                                 queue.url, num_remaining)
                    else:
                        log.warning('Queue %r still reports %i message(s) after '
                                    '%i empty receives, dumping it no further',
                                    queue.url, num_remaining, num_empty_receives)
                        break
        log.info('Wrote %i messages', num_messages)
        log.info(f'Finished writing {path!r}')
        if not self._delete:
            self._cleanup_messages(queue, messages)

    def _num_remaining_messages(self, queue: Queue, num_in_flight: int) -> int:
        """
        The number of messages the given queue reports, other than the given
        number of messages this dump itself holds in flight. The counts are
        approximate, so the result is, too.
        """
        queue.reload()
        return max(0, sum(self._message_counts(queue)) - num_in_flight)

    def _message_counts(self, queue: Queue) -> builtins.list[int]:
        return [
            int(queue.attributes[name])
            for name in self.message_count_attributes
        ]

    def _is_fifo(self, queue: Queue) -> bool:
        return queue.attributes.get('FifoQueue') == 'true'

    def _receive_messages(self, queue: Queue) -> builtins.list[Message]:
        return queue.receive_messages(AttributeNames=['All'],
                                      MaxNumberOfMessages=self.batch_size,
                                      VisibilityTimeout=300,
                                      WaitTimeSeconds=self.receive_wait_time)

    def _get_messages(self, queue: Queue) -> builtins.list[Message]:
        messages: list[Message] = []
        while True:
            message_batch = self._receive_messages(queue)
            if not message_batch:  # Nothing left in queue
                return messages
            else:
                messages.extend(message_batch)

    def read_messages(self, queue: Queue) -> builtins.list[Message]:
        messages = self._get_messages(queue)
        self._cleanup_messages(queue, messages)
        return messages

    def send_messages(self,
                      queue: Queue,
                      messages: Iterable[SQSMessage],
                      rate_limit: float | None = None,
                      log_level: int = logging.DEBUG
                      ) -> int:
        num_messages = 0
        for batch in chunked(messages, self.batch_size):
            entries = [message.to_batch_entry(i) for i, message in enumerate(batch)]
            start = time.time()
            queue.send_messages(Entries=entries)
            for message in batch:
                self._log_queued_message(message, log_level)
            if rate_limit is not None:
                period = 1 / rate_limit
                time_spent = time.time() - start
                time_to_sleep = period - time_spent
                if time_to_sleep > 0:
                    log.debug('Sleeping %.3fs to avoid SQS rate limit', time_to_sleep)
                    time.sleep(time_to_sleep)
            num_messages += len(batch)
        return num_messages

    def send_message(self,
                     queue: Queue,
                     message: SQSMessage,
                     log_level: int = logging.DEBUG):
        queue.send_message(**message.to_entry())
        self._log_queued_message(message, log_level)

    def _log_queued_message(self, message: SQSMessage, log_level: int):
        log.log(log_level, 'Queued message %r', message)

    def _cleanup_messages(self, queue: Queue, messages: Iterable[Message]):
        message_batches = list(more_itertools.chunked(messages, self.batch_size))
        if self._delete:
            log.info('Removing messages from queue %r', queue.url)
            self._delete_messages(message_batches, queue)
        else:
            log.info('Returning messages to queue %r', queue.url)
            self._return_messages(message_batches, queue)

    def _dump_messages(self,
                       messages: Iterable[JSON],
                       queue_url: str,
                       path: str):
        messages = list(messages)
        with write_file_atomically(path) as file:
            for content in chain([{'queue': queue_url}], messages):
                self._write_line(file, content)
        log.info('Wrote %i messages', len(messages))

    def _write_line(self, file: TextIO, content: JSON):
        json.dump(content, file)
        file.write('\n')

    def _return_messages(self,
                         message_batches: Iterable[Iterable[Message]],
                         queue: Queue):
        for message_batch in message_batches:
            batch: list[ChangeMessageVisibilityBatchRequestEntryTypeDef] = [
                dict(Id=message.message_id,
                     ReceiptHandle=message.receipt_handle,
                     VisibilityTimeout=0)
                for message in message_batch
            ]
            response = queue.change_message_visibility_batch(Entries=batch)
            if len(response['Successful']) != len(batch):
                raise RuntimeError(f'Failed to return message: {response!r}')

    def _delete_messages(self,
                         message_batches: Iterable[builtins.list[Message]],
                         queue: Queue):
        for message_batch in message_batches:
            response = queue.delete_messages(
                Entries=[
                    dict(Id=message.message_id,
                         ReceiptHandle=message.receipt_handle)
                    for message in message_batch
                ]
            )
            if len(response['Successful']) != len(message_batch):
                raise RuntimeError(f'Failed to delete messages: {response!r}')

    def _condense(self, message: Message) -> JSON:
        """
        Prepare a message for writing to a local file.
        """
        # The cast is needed because the type stub for `Message` misuses `typing.Literal`
        attributes = cast(dict[str, str], message.attributes)
        return {
            'MessageId': message.message_id,
            'ReceiptHandle': message.receipt_handle,
            'MD5OfBody': message.md5_of_body,
            'Body': json.loads(message.body),
            'Attributes': json_mapping(attributes),
            '_Attributes': {
                k: datetime.fromtimestamp(int(json_str(attributes[k])) / 1000).astimezone().isoformat()
                for k in ('SentTimestamp', 'ApproximateFirstReceiveTimestamp')
            }
        }

    def _reconstitute(self, message: JSON) -> SendMessageBatchRequestEntryTypeDef:
        """
        Prepare a message from a local file for submission to a queue.

        The inverse of _condense().
        """
        attributes = json_mapping(message['Attributes'])
        result: SendMessageBatchRequestEntryTypeDef = {
            'Id': json_str(message['MessageId']),
            'MessageBody': json.dumps(message['Body']),
        }
        for key in ('MessageGroupId', 'MessageDeduplicationId'):
            try:
                result[key] = json_str(attributes[key])
            except KeyError:
                pass
        return result

    def all_queues(self) -> dict[str, Queue]:
        return self.get_queues(config.all_queue_names)

    def get_queues(self, queue_names: Iterable[str]) -> dict[str, Queue]:
        return {
            queue_name: aws.sqs_queue(queue_name)
            for queue_name in queue_names
        }

    def get_queue_lengths(self,
                          queues: Mapping[str, Queue]
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
            message_counts = self._message_counts(queue)
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
        queues = self.get_queues(queue_names)
        maxlen = ceil(timeout / self.stabilization_poll_interval)
        total_lengths: deque[int] = deque(maxlen=maxlen)
        # Two minutes to safely accommodate SQS eventual consistency window of
        # one minute. For more info, read WARNING section on
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.get_queue_attributes
        assert maxlen * self.stabilization_poll_interval >= 2 * 60

        while True:
            # Determine queue lengths
            total_length, queue_lengths = self.get_queue_lengths(queues)
            total_lengths.append(total_length)
            log.info('Counting %i messages in %i queues.',
                     total_length, len(queue_lengths))
            log.info('Message count history (most recent first) is %r.',
                     list(reversed(total_lengths)))

            min_num_zeros = 60 // self.stabilization_poll_interval
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
            time.sleep(self.stabilization_poll_interval)

        if final_length != 0:
            raise Exception('The queues have stalled', final_length)
        return final_length

    def feed(self, path: str, queue_name: str):
        with open(path) as file:
            envelope, *messages = (json.loads(line) for line in file)
        orig_queue = envelope['queue']
        queue = aws.sqs_queue(queue_name)
        log.info('Writing messages from file %r to queue %r', path, queue.url)
        message_batches = list(more_itertools.chunked(messages, self.batch_size))

        def _cleanup():
            if self._delete:
                remaining_messages = list(chain.from_iterable(message_batches))
                if len(remaining_messages) < len(messages):
                    self._dump_messages(remaining_messages, orig_queue, path)
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

    def redrive(self, source_queue_name: str, destination_queue_name: str):
        """
        Move all messages from one queue to another, using the SQS redrive
        facility, and wait for the move to complete. Unlike :meth:`dump` and
        :meth:`feed`, this does not receive the messages, so it is not subject
        to the limitations of receiving from a FIFO queue.

        SQS only moves messages out of a queue that is configured as the
        dead-letter queue of another queue, and only into a queue of the same
        type, i.e., either both queues are FIFO or neither is.
        """
        source = aws.sqs_queue(source_queue_name)
        destination = aws.sqs_queue(destination_queue_name)
        source_arn = source.attributes['QueueArn']
        # SQS only reports the handle of a task while that task is running, so
        # the task started below can only be told apart from the tasks that
        # preceded it by when it was started.
        task = self._latest_message_move_task(source_arn)
        previously_started = None if task is None else task['StartedTimestamp']
        log.info('Moving messages from queue %r to queue %r',
                 source.url, destination.url)
        aws.sqs.start_message_move_task(SourceArn=source_arn,
                                        DestinationArn=destination.attributes['QueueArn'])
        while True:
            task = self._latest_message_move_task(source_arn)
            if task is None or task['StartedTimestamp'] == previously_started:
                log.info('Waiting for the move to start ...')
            else:
                num_moved = task.get('ApproximateNumberOfMessagesMoved', 0)
                status = task['Status']
                if status == 'RUNNING':
                    log.info('Moved %i of %i messages',
                             num_moved, task.get('ApproximateNumberOfMessagesToMove', 0))
                elif status == 'COMPLETED':
                    log.info('Moved %i messages', num_moved)
                    break
                else:
                    raise RuntimeError('Failed to move messages',
                                       status, task.get('FailureReason'), num_moved)
            time.sleep(self.redrive_poll_interval)

    def _latest_message_move_task(self,
                                  source_arn: str
                                  ) -> ListMessageMoveTasksResultEntryTypeDef | None:
        response = aws.sqs.list_message_move_tasks(SourceArn=source_arn, MaxResults=1)
        # SQS omits the key entirely if no task was ever started on the queue,
        # even though the Boto3 type stub declares it to be present
        return only(response.get('Results', []))

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

    def purge_queues_safely(self, queues: Mapping[str, Queue]):
        self.manage_lambdas(queues, enable=False)
        self.purge_queues_unsafely(queues)
        self.manage_lambdas(queues, enable=True)

    def purge_queues_unsafely(self, queues: Mapping[str, Queue]):
        with ThreadPoolExecutor(max_workers=len(queues)) as tpe:
            futures = [tpe.submit(self._purge_queue, queue) for queue in queues.values()]
            self._handle_futures(futures)

    def _purge_queue(self, queue: Queue):
        log.info('Purging queue %r', queue.url)
        queue.purge()
        self._wait_for_queue_empty(queue)

    def _wait_for_queue_idle(self, queue: Queue):
        while True:
            num_inflight_messages = int(queue.attributes['ApproximateNumberOfMessagesNotVisible'])
            if num_inflight_messages == 0:
                break
            log.info('Queue %r has %i in-flight messages', queue.url, num_inflight_messages)
            time.sleep(3)
            queue.reload()

    def _wait_for_queue_empty(self, queue: Queue):
        while True:
            num_messages = sum(self._message_counts(queue))
            if num_messages == 0:
                break
            log.info('Queue %r still has %i messages', queue.url, num_messages)
            time.sleep(3)
            queue.reload()

    def _manage_sqs_push(self,
                         *,
                         function: str,
                         alias: str,
                         queue: Queue,
                         enable: bool):
        lambda_ = aws.lambda_
        partial_arn = f'{function}:{alias}'
        response = lambda_.list_event_source_mappings(FunctionName=partial_arn,
                                                      EventSourceArn=queue.attributes['QueueArn'])
        mapping_uuid = one(response['EventSourceMappings'])['UUID']

        def update_():
            log.info('%s push from %r to lambda function %r',
                     'Enabling' if enable else 'Disabling', queue.url, function)
            lambda_.update_event_source_mapping(UUID=mapping_uuid, Enabled=enable)

        state = one(response['EventSourceMappings'])['State']
        while True:
            log.info('Push from %r to lambda function %r is in state %r.',
                     queue.url, function, state)
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

    def manage_lambdas(self, queues: Mapping[str, Queue], enable: bool):
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
                    submit(self._manage_sqs_push,
                           function=function,
                           alias=config.active_function_alias_name,
                           queue=queue,
                           enable=enable)
            self._handle_futures(futures)
            futures = [tpe.submit(self._wait_for_queue_idle, queue) for queue in queues.values()]
            self._handle_futures(futures)

    def _manage_lambda(self, function_name: str, enable: bool):
        self._functions.manage_function(function_name, enable)

    @cached_property
    def _functions(self) -> LambdaFunctions:
        return LambdaFunctions()

    def _handle_futures(self, futures: Iterable[Future]):
        errors = []
        for future in as_completed(futures):
            e = future.exception()
            if e:
                errors.append(e)
                log.error('Exception in worker thread', exc_info=e)
        if errors:
            raise RuntimeError(errors)


class Action(DiscriminatingPolymorphicSerializableAttrs,
             StaticRegisteredPolymorphicSerializable):
    """
    Represents a unit of pending work. Actions are persisted as the bodies of
    queue messages. We may even use the terms *message* and *action*
    interchangeably.
    """

    @classmethod
    def discriminator(cls) -> str:
        return 'action'
