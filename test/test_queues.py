import json
import os
import tempfile
import time
from unittest.mock import (
    patch,
)

from more_itertools import (
    one,
)
from moto import (
    mock_aws,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.queues import (
    Queues,
    SQSFifoMessage,
)
from sqs_test_case import (
    SqsTestCase,
)


@mock_aws
class TestQueues(SqsTestCase):

    def _create_queue(self):
        queue_name = config.tallies_queue.to_fail.name
        self._create_mock_queues([queue_name])
        return queue_name, aws.sqs_queue(queue_name)

    def _send_messages(self, queue, num_messages: int, num_groups: int | None = None):
        # By default, each message is put in a group of its own so that none of
        # them is withheld while another one of the same group is in flight
        messages = [
            SQSFifoMessage(body={'i': i},
                           group_id=str(i if num_groups is None else i % num_groups))
            for i in range(num_messages)
        ]
        Queues().send_messages(queue, messages)
        return messages

    def _read_dump(self, path: str):
        with open(path) as file:
            envelope, *messages = (json.loads(line) for line in file)
        return envelope, messages

    def test_round_trip(self):
        queue_name, queue = self._create_queue()
        messages = self._send_messages(queue, 3)
        with tempfile.TemporaryDirectory() as dir_path:
            path = os.path.join(dir_path, 'dump.jsonl')
            Queues(delete=True).dump(queue_name, path)
            queue.reload()
            self.assertEqual('0', queue.attributes['ApproximateNumberOfMessages'])
            envelope, dumped_messages = self._read_dump(path)
            self.assertEqual(queue.url, envelope['queue'])
            self.assertEqual([message.body for message in messages],
                             [message['Body'] for message in dumped_messages])
            Queues().feed(path, queue_name)
        fed_messages = Queues(delete=True).read_messages(queue)
        self.assertEqual([message.body for message in messages],
                         sorted((json.loads(message.body) for message in fed_messages),
                                key=lambda body: body['i']))

    def test_fifo_dump_needs_delete(self):
        queue_name, queue = self._create_queue()
        num_messages = Queues.batch_size + 5
        self._send_messages(queue, num_messages, num_groups=1)
        with tempfile.TemporaryDirectory() as dir_path:
            path = os.path.join(dir_path, 'dump.jsonl')
            # Messages held in flight block the rest of their group, so a dump
            # that returns them captures only as many as one receive yields
            Queues().dump(queue_name, path)
            envelope, dumped_messages = self._read_dump(path)
            self.assertEqual(Queues.batch_size, len(dumped_messages))
            queue.reload()
            self.assertEqual(str(num_messages),
                             queue.attributes['ApproximateNumberOfMessages'])
            # Deleting each batch as it is written unblocks the next one
            Queues(delete=True).dump(queue_name, path)
            envelope, dumped_messages = self._read_dump(path)
            self.assertEqual(num_messages, len(dumped_messages))
            self.assertEqual([{'i': i} for i in range(num_messages)],
                             [message['Body'] for message in dumped_messages])
        queue.reload()
        self.assertEqual('0', queue.attributes['ApproximateNumberOfMessages'])

    def test_wait_for_queue_empty_counts_in_flight_messages(self):
        queue_name, queue = self._create_queue()
        self._send_messages(queue, 3)
        queues = Queues()
        # Receiving the messages without deleting them puts them in flight,
        # which leaves the queue anything but empty
        self.assertEqual(3, len(queues._receive_messages(queue)))
        queue.reload()
        self.assertEqual('3', queue.attributes['ApproximateNumberOfMessagesNotVisible'])

        class Waited(Exception):
            pass

        # The method waits by sleeping, so a sleep means it did not mistake
        # the queue for an empty one
        with patch.object(time, 'sleep', side_effect=Waited):
            with self.assertRaises(Waited):
                queues._wait_for_queue_empty(queue)

    def test_dump_of_queue_that_keeps_reporting_messages(self):
        queue_name, queue = self._create_queue()
        self._send_messages(queue, 1)
        with tempfile.TemporaryDirectory() as dir_path:
            path = os.path.join(dir_path, 'dump.jsonl')
            # A queue whose producers are still running may never report zero,
            # so the dump gives up instead of polling it forever
            with patch.object(Queues, '_num_remaining_messages', return_value=7):
                with self.assertLogs('azul.queues', level='WARNING') as logs:
                    Queues(delete=True).dump(queue_name, path)
            envelope, dumped_messages = self._read_dump(path)
        message = one(logs.records).getMessage()
        self.assertIn('still reports 7 message(s)', message)
        self.assertIn(f'after {Queues.max_empty_receives} empty receives', message)
        self.assertEqual(1, len(dumped_messages))

    def test_interrupted_feed(self):
        queue_name, queue = self._create_queue()
        num_messages = Queues.batch_size + 1
        self._send_messages(queue, num_messages)
        with tempfile.TemporaryDirectory() as dir_path:
            path = os.path.join(dir_path, 'dump.jsonl')
            Queues(delete=True).dump(queue_name, path)
            envelope, dumped_messages = self._read_dump(path)
            self.assertEqual(num_messages, len(dumped_messages))

            send_messages = type(queue).send_messages
            num_calls = 0

            def fail_after_first_batch(self, **kwargs):
                nonlocal num_calls
                num_calls += 1
                if num_calls > 1:
                    raise RuntimeError('simulated failure')
                else:
                    return send_messages(self, **kwargs)

            with patch.object(type(queue), 'send_messages', fail_after_first_batch):
                with self.assertRaises(RuntimeError) as context:
                    Queues(delete=True).feed(path, queue_name)
            # The exception that interrupted the feed is the one the operator
            # sees, not a failure of the cleanup that follows it
            self.assertEqual(('simulated failure',), context.exception.args)
            # Only the messages that were not sent remain in the file
            envelope, remaining_messages = self._read_dump(path)
            self.assertEqual(dumped_messages[Queues.batch_size:], remaining_messages)
        queue.reload()
        self.assertEqual(str(Queues.batch_size),
                         queue.attributes['ApproximateNumberOfMessages'])
