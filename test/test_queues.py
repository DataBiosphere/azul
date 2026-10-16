import json
import os
import tempfile
from unittest.mock import (
    patch,
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

    def _send_messages(self, queue, num_messages: int):
        # Each message is put in a group of its own so that none of them is
        # withheld while another one of the same group is in flight
        messages = [
            SQSFifoMessage(body={'i': i}, group_id=str(i))
            for i in range(num_messages)
        ]
        Queues().send_messages(queue, messages)
        return messages

    def _read_dump(self, path: str):
        with open(path) as file:
            return json.load(file)

    def test_round_trip(self):
        queue_name, queue = self._create_queue()
        messages = self._send_messages(queue, 3)
        with tempfile.TemporaryDirectory() as dir_path:
            path = os.path.join(dir_path, 'dump.json')
            Queues(delete=True).dump(queue_name, path)
            queue.reload()
            self.assertEqual('0', queue.attributes['ApproximateNumberOfMessages'])
            content = self._read_dump(path)
            self.assertEqual(queue.url, content['queue'])
            self.assertEqual([message.body for message in messages],
                             [message['Body'] for message in content['messages']])
            Queues().feed(path, queue_name)
        fed_messages = Queues(delete=True).read_messages(queue)
        self.assertEqual([message.body for message in messages],
                         sorted((json.loads(message.body) for message in fed_messages),
                                key=lambda body: body['i']))

    def test_interrupted_feed(self):
        queue_name, queue = self._create_queue()
        num_messages = Queues.batch_size + 1
        self._send_messages(queue, num_messages)
        with tempfile.TemporaryDirectory() as dir_path:
            path = os.path.join(dir_path, 'dump.json')
            Queues(delete=True).dump(queue_name, path)
            dumped_messages = self._read_dump(path)['messages']
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
            self.assertEqual(dumped_messages[Queues.batch_size:],
                             self._read_dump(path)['messages'])
        queue.reload()
        self.assertEqual(str(Queues.batch_size),
                         queue.attributes['ApproximateNumberOfMessages'])
