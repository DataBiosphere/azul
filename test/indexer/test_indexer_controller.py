import json
import logging
import os
from unittest.mock import (
    MagicMock,
    call,
    patch,
)

from chalice.app import (
    SQSRecord,
)
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
from azul.indexer.document import (
    EntityReference,
)
from azul.indexer.index_controller import (
    IndexController,
)
from azul.indexer.index_service import (
    IndexService,
)
from azul.logging import (
    configure_test_logging,
)
from azul.plugins.repository.dss import (
    DSSSourceRef,
    Plugin,
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
    partition_prefix_length = 0

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

    def _mock_sqs_record(self, body):
        event_dict = {
            'body': json.dumps(body),
            'receiptHandle': 'ThisWasARandomString',
            'attributes': {'ApproximateReceiveCount': 1}
        }
        return SQSRecord(event_dict=event_dict, context='controller_test')

    @property
    def _notifications_queue(self):
        return self.controller._notifications_queue()

    @property
    def _tallies_queue(self):
        return self.controller._tallies_queue()

    def _read_queue(self, queue):
        messages = self.queue_manager.read_messages(queue)
        tallies = [json.loads(m.body) for m in messages]
        return tallies

    def test_invalid_notification(self):
        event = [
            self._mock_sqs_record(dict(action='foo',
                                       source='foo_source',
                                       notification='bar',
                                       catalog=self.catalog))
        ]
        self.assertRaises(KeyError, self.controller.contribute, event)

    def test_remote_reindex(self):
        with patch.dict(os.environ, dict(AZUL_DSS_QUERY_PREFIX='ff',
                                         AZUL_DSS_ENDPOINT='foo_source')):
            source = DSSSourceRef.for_dss_endpoint('foo_source')
            self.index_service.repository_plugin(self.catalog)._assert_source(source)
            self._create_mock_queues()
            self.client.remote_reindex(self.catalog, {str(source.spec)})
            notification = one(self._read_queue(self._notifications_queue))
            expected_notification = dict(action='reindex',
                                         catalog='test',
                                         source=source.to_json(),
                                         prefix='')
            self.assertEqual(expected_notification, notification)
            event = [self._mock_sqs_record(notification)]

            bundle_fqids = [
                SourcedBundleFQID(source=source,
                                  uuid='ffa338fe-7554-4b5d-96a2-7df127a7640b',
                                  version='2018-03-28T151023.074974Z')
            ]

            with patch.object(Plugin, 'list_bundles', return_value=bundle_fqids):
                self.controller.contribute(event)

            notification = one(self._read_queue(self._notifications_queue))
            expected_source = dict(id=source.id, spec=str(source.spec))
            source = notification['notification']['source']
            self.assertEqual(expected_source, source)

    def test_contribute_and_aggregate(self):
        """
        Contribution and aggregation of two bundles

        Index two bundles that make contributions to the same project. Inspect
        that the contributions match the tallies that are returned to SQS.
        During aggregation only the project entity is deferred due to
        multiple contributions.
        """
        self.maxDiff = None
        self._create_mock_queues()
        source = DSSSourceRef.for_dss_endpoint('foo_source')
        fqids = [
            SourcedBundleFQID(source=source,
                              uuid='56a338fe-7554-4b5d-96a2-7df127a7640b',
                              version='2018-03-28T151023.074974Z'),
            SourcedBundleFQID(source=source,
                              uuid='b2216048-7eaa-45f4-8077-5a3fb4204953',
                              version='2018-03-29T104041.822717Z')
        ]

        # Load canned bundles
        bundles = {
            fqid: self._load_canned_bundle(fqid)
            for fqid in fqids
        }

        # Synthesize notifications
        notifications = [
            dict(action='add',
                 catalog=self.catalog,
                 notification=self.client.synthesize_notification(fqid))
            for fqid in fqids
        ]

        # Invoke the service once to produce a set of expected entities so we
        # don't need to hard-code them. Keep in mind that this test is not
        # intended to cover the service, only the controller.
        expected_entities = set()
        for bundle in bundles.values():
            contributions = self.index_service.transform(self.catalog, bundle, delete=False)
            expected_entities.update(c.entity for c in contributions)

        # Test contribution
        mock_plugin = MagicMock()
        mock_plugin.fetch_bundle.side_effect = list(bundles.values())
        mock_plugin.source_from_json.return_value = source
        mock_plugin.sources = [source]
        with patch.object(IndexService, 'repository_plugin', return_value=mock_plugin):
            event = list(map(self._mock_sqs_record, notifications))
            self.controller.contribute(event)

        # Assert plugin calls by controller
        expected_calls = [call(source.to_json())] * len(bundles)
        self.assertEqual(expected_calls, mock_plugin.source_from_json.mock_calls)
        expected_calls = list(map(call, fqids))
        self.assertEqual(expected_calls, mock_plugin.fetch_bundle.mock_calls)

        # Assert tallies
        tallies = self._read_queue(self._tallies_queue)
        entities = {
            EntityReference(entity_id=t['entity_id'], entity_type=t['entity_type'])
            for t in tallies
        }
        self.assertEqual(expected_entities, entities)

        # Test aggregation
        notifications = map(self._mock_sqs_record, tallies)
        self.controller.aggregate(notifications)

        # Assert that aggregation of project entity was deferred
        tallies = self._read_queue(self._tallies_queue)
        expected_tallies = [
            {
                'catalog': 'test',
                'entity_type': 'projects',
                'entity_id': '93f6a42f-1790-4af4-b5d1-8c436cb6feae',
                'num_contributions': 2
            }
        ]
        self.assertEqual(expected_tallies, tallies)

        # Test aggregation of deferred tally
        notifications = map(self._mock_sqs_record, tallies)
        self.controller.aggregate(notifications)

        # Assert that remaining tallies were consumed
        messages = self._read_queue(self._tallies_queue)
        self.assertEqual(0, len(messages))
