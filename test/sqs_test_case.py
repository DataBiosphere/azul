import json
import logging
import random
from random import (
    Random,
)
import sys
from unittest.mock import (
    patch,
)
import uuid

from chalice.app import (
    SQSRecord,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.lib import (
    cached_property,
)
from azul.lib.functions import (
    iif,
)
from azul.lib.types import (
    JSON,
    MutableJSONs,
)
from azul.queues import (
    Queues,
)
from azul_test_case import (
    AzulUnitTestCase,
)

log = logging.getLogger(__name__)


class SqsTestCase(AzulUnitTestCase):

    def setUp(self) -> None:
        super().setUp()
        # Long polling would make every receive from an empty queue, and there
        # is one at the end of every drain, wait for the full duration
        self.addPatch(patch.object(Queues, 'receive_wait_time', 0))

    def _create_mock_queues(self, queue_names: list[str] | None = None) -> None:
        if queue_names is not None:
            self.assertIsSubset(set(queue_names), set(config.all_queue_names))
        else:
            queue_names = config.all_queue_names

        sqs = aws.sqs_resource
        for queue_name in queue_names:
            sqs.create_queue(QueueName=queue_name,
                             Attributes=dict(FifoQueue='true') if queue_name.endswith('.fifo') else {})


class WorkQueueTestCase(SqsTestCase):

    @cached_property
    def queues(self) -> Queues:
        return Queues(delete=True)

    @cached_property
    def random(self) -> Random:
        seed = random.randint(0, sys.maxsize)
        log.info('Using random seed %d', seed)
        return random.Random(seed)

    def random_uuid(self) -> str:
        # https://stackoverflow.com/a/41186895/1530508
        bits = self.random.getrandbits(128)
        return str(uuid.UUID(int=bits, version=4))

    def _read_queue(self, queue) -> MutableJSONs:
        messages = self.queues.read_messages(queue)
        # For unknown reasons, Moto 4.0.6 requires reading the queues a second
        # time whereas 2.0.6 didn't. It *is* more realistic, but I am not sure
        # how reliable this is.
        messages += self.queues.read_messages(queue)
        message_bodies = [json.loads(m.body) for m in messages]
        return message_bodies

    def _mock_sqs_record(self,
                         body: JSON,
                         *,
                         attempts: int = 1,
                         fifo: bool = False,
                         ) -> SQSRecord:
        event_dict = {
            'messageId': self.random_uuid(),
            'body': json.dumps(body),
            'receiptHandle': 'ThisWasARandomString',
            'attributes': {
                'ApproximateReceiveCount': str(attempts),
                **iif(fifo, {
                    'MessageGroupId': self.random_uuid(),
                    'MessageDeduplicationId': self.random_uuid()
                })
            }
        }
        return SQSRecord(event_dict=event_dict, context={})
