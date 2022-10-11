from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul_test_case import (
    AzulUnitTestCase,
)


class SqsTestCase(AzulUnitTestCase):

    def _create_mock_queues(self, *queue_names):
        if queue_names:
            self.assertIsSubset(set(queue_names), set(config.all_queue_names))
        else:
            queue_names = config.all_queue_names

        sqs = aws.resource('sqs')
        for queue_name in queue_names:
            sqs.create_queue(QueueName=queue_name,
                             Attributes=dict(FifoQueue='true') if queue_name.endswith('.fifo') else {})

    def _create_mock_notifications_queue(self):
        self._create_mock_queues(config.notifications_queue_name())
