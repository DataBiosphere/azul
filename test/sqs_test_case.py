import json
import logging
import random
from random import (
    Random,
)
import sys
import uuid

from chalice.app import (
    SQSRecord,
)

from azul import (
    cached_property,
    config,
    iif,
)
from azul.azulclient import (
    AzulClient,
)
from azul.deployment import (
    aws,
)
from azul.queues import (
    Queues,
)
from azul.types import (
    JSON,
    MutableJSONs,
)
from azul_test_case import (
    AzulUnitTestCase,
)

log = logging.getLogger(__name__)


class SqsTestCase(AzulUnitTestCase):

    def _create_mock_queues(self, queue_names: list[str] | None = None) -> None:
        if queue_names is not None:
            self.assertIsSubset(set(queue_names), set(config.all_queue_names))
        else:
            queue_names = config.all_queue_names

        sqs = aws.sqs_resource
        for queue_name in queue_names:
            sqs.create_queue(QueueName=queue_name,
                             Attributes=dict(FifoQueue='true') if queue_name.endswith('.fifo') else {})

    def _create_mock_notifications_queue(self):
        self._create_mock_queues([config.notifications_queue.name])


class WorkQueueTestCase(SqsTestCase):

    @cached_property
    def queues(self) -> Queues:
        return Queues(delete=True)

    @cached_property
    def client(self) -> AzulClient:
        return AzulClient()

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
