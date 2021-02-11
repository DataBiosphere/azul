import json
import logging

from chalice.app import (
    SQSRecord,
)
import mock
from more_itertools import (
    one,
)
from moto import (
    mock_sqs,
    mock_sts,
)

from azul import (
    config,
    queues,
)
from azul.azulclient import (
    AzulClient,
)
from azul.deployment import (
    aws,
)
from azul.indexer import (
    SourcedBundleFQID,
)
from azul.indexer.index_controller import (
    IndexController,
)
from azul.logging import (
    configure_test_logging,
)
from indexer import (
    IndexerTestCase,
)

log = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


@mock_sts
@mock_sqs
class TestIndexController(IndexerTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.index_service.create_indices(self.catalog)
        self.client = AzulClient()
        self.controller = IndexController()
        # noinspection PyPropertyAccess
        self.controller.index_service = self.index_service
        self.queue_manager = queues.Queues(delete=True)

    def tearDown(self):
        self.index_service.delete_indices(self.catalog)
        super().tearDown()

    def _create_mock_queues(self):
        sqs = aws.resource('sqs')
        for queue_name in config.all_queue_names:
            sqs.create_queue(QueueName=queue_name)

    def _mock_sqs_record(self, **body):
        event_dict = {
            'body': json.dumps(body),
            'receiptHandle': 'ThisWasARandomString',
            'attributes': {'ApproximateReceiveCount': 1}
        }
        return SQSRecord(event_dict=event_dict, context='controller_test')

    def test_invalid_notification(self):
        event = [
            self._mock_sqs_record(action='foo',
                                  source='foo_source',
                                  notification='bar',
                                  catalog=self.catalog)
        ]
        self.assertRaises(AssertionError, self.controller.contribute, event)

    def test_remote_reindex(self):
        event = [self._mock_sqs_record(action='reindex', prefix='ff')]
        with mock.patch.object(target=AzulClient,
                               attribute=AzulClient.remote_reindex_partition.__name__) as mock_method:
            mock_method.return_value = True
            self.controller.contribute(event)
            mock_method.assert_called_once_with(dict(action='reindex', prefix='ff'))

    def test_contribute_and_aggregate(self):
        """
        Contribution and aggregation of two bundles

        Index two bundles that make contributions to the same project. Inspect
        that the contributions match the tallies that are returned to SQS.
        During aggregation only the project entity is deferred due to
        multiple contributions.
        """
        self._create_mock_queues()
        event = []
        bundles = []
        expected_entities = set()
        prefix = ''
        mock_source = 'foo_source'
        bundle_fqids = [
            SourcedBundleFQID(source=mock_source,
                              uuid='56a338fe-7554-4b5d-96a2-7df127a7640b',
                              version='2018-03-28T151023.074974Z'),
            SourcedBundleFQID(source=mock_source,
                              uuid='b2216048-7eaa-45f4-8077-5a3fb4204953',
                              version='2018-03-29T104041.822717Z')
        ]
        for bundle_fqid in bundle_fqids:
            notification = self.client.synthesize_notification(self.catalog, prefix, bundle_fqid)
            event.append(self._mock_sqs_record(action='add',
                                               catalog=self.catalog,
                                               source=mock_source,
                                               notification=notification))
            bundle = self._load_canned_bundle(bundle_fqid)
            bundles.append(bundle)
            # Invoke the service once to produce a set of expected entities so
            # we don't need to hard-code them. Keep in mind that this test is
            # not covering the service, only the controller.
            contributions = self.index_service.transform(self.catalog, bundle, delete=False)
            expected_entities.update(
                (c.entity.entity_id, c.entity.entity_type)
                for c in contributions
            )

        mock_plugin = mock.MagicMock()
        mock_plugin.fetch_bundle.side_effect = bundles
        mock_plugin.sources = [mock_source]
        with mock.patch.object(IndexController,
                               'repository_plugin',
                               return_value=mock_plugin):
            # Test contribution
            self.controller.contribute(event)
            tallies = [
                json.loads(m.body)
                for m in self.queue_manager.read_messages(self.controller._tallies_queue())
            ]
            entities_from_tallies = {
                (t['entity_id'], t['entity_type'])
                for t in tallies
            }
            self.assertSetEqual(expected_entities, entities_from_tallies)
            self.assertListEqual([mock.call(f) for f in bundle_fqids],
                                 mock_plugin.fetch_bundle.mock_calls)

            # Test aggregation for tallies, inspect for deferred tallies
            event = [self._mock_sqs_record(**t) for t in tallies]
            self.controller.aggregate(event)
            messages = self.queue_manager.read_messages(self.controller._tallies_queue())

            # Check that aggregation of project entity was deferred
            project_tally = json.loads(one(messages).body)
            expected_tally = {
                'catalog': 'test',
                'entity_type': 'projects',
                'entity_id': '93f6a42f-1790-4af4-b5d1-8c436cb6feae',
                'num_contributions': 2
            }
            self.assertDictEqual(project_tally, expected_tally)

            # Test aggregation of deferred project entity
            event = [self._mock_sqs_record(**project_tally)]
            self.controller.aggregate(event)
            messages = self.queue_manager.read_messages(self.controller._tallies_queue())
            self.assertEqual(0, len(messages))
