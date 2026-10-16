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
        # Every queue is created because a dump inspects the work queues
        self._create_mock_queues()
        queue_name = config.tallies_queue.to_fail.name
        return queue_name, aws.sqs_queue(queue_name)

    def _fill_work_queue(self, num_messages: int = 1):
        queue = aws.sqs_queue(config.tallies_queue.name)
        self._send_messages(queue, num_messages)

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
            expected_envelope = {'queue': queue.url, 'message_type': 'DocumentTally'}
            self.assertEqual(expected_envelope, envelope)
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
        message = one(record.getMessage()
                      for record in logs.records
                      if 'still reports' in record.getMessage())
        self.assertIn('still reports 7 message(s)', message)
        self.assertIn(f'after {Queues.max_empty_receives} empty receives', message)
        self.assertEqual(1, len(dumped_messages))

    def test_dump_of_busy_deployment(self):
        queue_name, queue = self._create_queue()
        self._send_messages(queue, 2)
        self._fill_work_queue(3)
        with tempfile.TemporaryDirectory() as dir_path:
            path = os.path.join(dir_path, 'dump.jsonl')
            # Neither flag leaves the operator with no decision made
            with self.assertRaises(RuntimeError) as context:
                Queues().dump(queue_name, path)
            self.assertIn('--safe', context.exception.args[0])
            self.assertIn('--unsafe', context.exception.args[0])
            self.assertEqual(3, context.exception.args[1])
            # Deleting what is likely an incomplete dump is refused as well
            with self.assertRaises(RuntimeError) as context:
                Queues(delete=True, safe_dump=False).dump(queue_name, path)
            self.assertIn('making --delete unsafe', context.exception.args[0])
            # Dumping without deleting is permitted, with a warning
            with self.assertLogs('azul.queues', level='WARNING') as logs:
                Queues(safe_dump=False).dump(queue_name, path)
            self.assertIn('probably incomplete',
                          ' '.join(record.getMessage() for record in logs.records))
            envelope, dumped_messages = self._read_dump(path)
            self.assertEqual(2, len(dumped_messages))

    def test_dump_of_idle_deployment(self):
        queue_name, queue = self._create_queue()
        self._send_messages(queue, 2)
        with tempfile.TemporaryDirectory() as dir_path:
            path = os.path.join(dir_path, 'dump.jsonl')
            with self.assertLogs('azul.queues', level='WARNING') as logs:
                Queues(delete=True).dump(queue_name, path)
            self.assertIn('snatched up by the lambdas',
                          ' '.join(record.getMessage() for record in logs.records))
            envelope, dumped_messages = self._read_dump(path)
            self.assertEqual(2, len(dumped_messages))

    def test_feed_of_incompatible_messages(self):
        queue_name, queue = self._create_queue()
        self._send_messages(queue, 1)
        with tempfile.TemporaryDirectory() as dir_path:
            path = os.path.join(dir_path, 'dump.jsonl')
            Queues(delete=True).dump(queue_name, path)
            # Notifications are not tallies, whichever queue of theirs is fed
            for other_name in [config.notifications_queue.name,
                               config.notifications_queue.to_fail.name]:
                with self.assertRaises(RuntimeError) as context:
                    Queues().feed(path, other_name)
                self.assertEqual(('DocumentTally', 'IndexAction'),
                                 context.exception.args[1:])
            # The queue the messages came from carries them, as does its
            # counterpart that the retries of those messages end up in
            Queues().feed(path, config.tallies_queue.to_retry.name)

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
            # sees, not a failure of the bookkeeping that follows it
            self.assertEqual(('simulated failure',), context.exception.args)
            # The dump is left as it was, and what was fed is recorded beside it
            envelope, remaining_messages = self._read_dump(path)
            self.assertEqual(dumped_messages, remaining_messages)
            with open(path + Queues.progress_suffix) as file:
                self.assertEqual({'queue': queue.url,
                                  'num_messages': Queues.batch_size},
                                 json.load(file))
            queue.reload()
            self.assertEqual(str(Queues.batch_size),
                             queue.attributes['ApproximateNumberOfMessages'])
            # Feeding again submits only the messages that are left
            Queues(delete=True).feed(path, queue_name)
            self.assertFalse(os.path.exists(path + Queues.progress_suffix))
            self.assertFalse(os.path.exists(path))
        queue.reload()
        self.assertEqual(str(num_messages),
                         queue.attributes['ApproximateNumberOfMessages'])
