from bisect import (
    insort,
)
from collections import (
    defaultdict,
)
from functools import (
    partial,
)
import json
import os
from unittest.mock import (
    MagicMock,
    call,
    patch,
)

from chalice.app import (
    SQSRecord,
)
from elasticsearch import (
    TransportError,
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
from azul.indexer import (
    BundlePartition,
)
from azul.indexer.document import (
    Contribution,
)
from azul.indexer.index_controller import (
    IndexController,
)
from azul.indexer.index_service import (
    IndexService,
    IndexWriter,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins.repository.dss import (
    DSSBundleFQID,
    DSSSourceRef,
    Plugin,
)
from azul.types import (
    JSONs,
)
from azul_test_case import (
    DCP1TestCase,
)
from indexer import (
    IndexerTestCase,
)
from sqs_test_case import (
    SqsTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


@mock_sts
@mock_sqs
class TestIndexController(DCP1TestCase, IndexerTestCase, SqsTestCase):
    partition_prefix_length = 0

    def setUp(self) -> None:
        super().setUp()
        self.index_service.create_indices(self.catalog)
        self.client = AzulClient()
        app = MagicMock()
        self.controller = IndexController(app=app)
        app.catalog = self.catalog
        IndexController.index_service.fset(self.controller, self.index_service)
        self.queue_manager = queues.Queues(delete=True)

    def tearDown(self):
        self.index_service.delete_indices(self.catalog)
        super().tearDown()

    def _mock_sqs_record(self, body, *, attempts: int = 1):
        event_dict = {
            'body': json.dumps(body),
            'receiptHandle': 'ThisWasARandomString',
            'attributes': {'ApproximateReceiveCount': attempts}
        }
        return SQSRecord(event_dict=event_dict, context={})

    @property
    def _notifications_queue(self):
        return self.controller._notifications_queue()

    @property
    def _notifications_retry_queue(self):
        return self.controller._notifications_queue(retry=True)

    @property
    def _tallies_queue(self):
        return self.controller._tallies_queue()

    @property
    def _tallies_retry_queue(self):
        return self.controller._tallies_queue(retry=True)

    def _read_queue(self, queue) -> JSONs:
        messages = self.queue_manager.read_messages(queue)
        # For unknown reasons, Moto 4.0.6 requires reading the queues a second
        # time whereas 2.0.6 didn't. It *is* more realistic but I am not sure
        # how reliable this is.
        messages += self.queue_manager.read_messages(queue)
        tallies = [json.loads(m.body) for m in messages]
        return tallies

    def _fqid_from_notification(self, notification):
        fqid = notification['notification']['bundle_fqid']
        return DSSBundleFQID(uuid=fqid['uuid'],
                             version=fqid['version'],
                             source=DSSSourceRef.from_json(fqid['source']))

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
                                         AZUL_DSS_SOURCE='foo_source:/0')):
            source = DSSSourceRef.for_dss_source(config.dss_source)
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
                DSSBundleFQID(source=source,
                              uuid='ffa338fe-7554-4b5d-96a2-7df127a7640b',
                              version='2018-03-28T15:10:23.074974Z')
            ]

            with patch.object(Plugin, 'list_bundles', return_value=bundle_fqids):
                self.controller.contribute(event)

            notification = one(self._read_queue(self._notifications_queue))
            expected_source = dict(id=source.id, spec=str(source.spec))
            source = notification['notification']['bundle_fqid']['source']
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
        source = DSSSourceRef.for_dss_source('foo_source:/0')
        fqids = [
            DSSBundleFQID(source=source,
                          uuid='56a338fe-7554-4b5d-96a2-7df127a7640b',
                          version='2018-03-28T15:10:23.074974Z'),
            DSSBundleFQID(source=source,
                          uuid='b2216048-7eaa-45f4-8077-5a3fb4204953',
                          version='2018-03-29T10:40:41.822717Z')
        ]

        # Load canned bundles
        bundles = {
            fqid: self._load_canned_bundle(fqid)
            for fqid in fqids
        }

        # Synthesize initial notifications
        notifications = [
            dict(action='add',
                 catalog=self.catalog,
                 notification=self.client.synthesize_notification(fqid))
            for fqid in fqids
        ]

        # Invoke the service once to produce a set of expected entities so we
        # don't need to hard-code them. Keep in mind that this test is not
        # intended to cover the service, only the controller.
        expected_digest = defaultdict(list)
        for fqid, bundle in bundles.items():
            contributions, replicas = self.index_service.transform(self.catalog,
                                                                   bundle,
                                                                   delete=False)
            for contribution in contributions:
                assert isinstance(contribution, Contribution)
                # Initially, each entity gets a tally of 1
                expected_digest[contribution.entity.entity_type].append(1)

        # Prove that we have two contributions per "container" type, for when we
        # test poison tallies and deferrals below. Note that the two project
        # contributions are to the same entity, the bundle contributions are not.
        for entity_type in ['projects', 'bundles']:
            self.assertEqual([1, 1], expected_digest[entity_type])

        # Test partitioning and contribution
        for i in range(2):
            mock_plugin = MagicMock()
            notified_fqids = list(map(self._fqid_from_notification, notifications))
            notified_bundles = [bundles[fqid] for fqid in notified_fqids]
            mock_plugin.fetch_bundle.side_effect = notified_bundles
            mock_plugin.resolve_bundle.side_effect = DSSBundleFQID.from_json
            mock_plugin.sources = [source]
            with patch.object(IndexService, 'repository_plugin', return_value=mock_plugin):
                with patch.object(BundlePartition, 'max_partition_size', 4):
                    event = list(map(self._mock_sqs_record, notifications))
                    self.controller.contribute(event)

            # Assert plugin calls by controller
            expected_calls = [call(fqid.to_json()) for fqid in notified_fqids]
            self.assertEqual(expected_calls, mock_plugin.resolve_bundle.mock_calls)
            expected_calls = list(map(call, notified_fqids))
            self.assertEqual(expected_calls, mock_plugin.fetch_bundle.mock_calls)

            # Assert partitioned notifications, straight from the retry queue
            notifications = self._read_queue(self._notifications_retry_queue)
            if i == 0:
                # Fingerprint the partitions from the resulting notifications
                partitions = defaultdict(set)
                for n in notifications:
                    fqid = self._fqid_from_notification(n)
                    partition = BundlePartition.from_json(n['notification']['partition'])
                    partitions[fqid].add(partition)
                # Assert that each bundle was partitioned ...
                self.assertEqual(partitions.keys(), set(fqids))
                # ... into two partitions. The number of partitions depends on
                # the patched max_partition_size above and the number of
                # entities in the canned bundles.
                self.assertEqual([2] * len(fqids), list(map(len, partitions.values())))
            else:
                # The partitions resulting from the first iteration should not
                # need to be partitioned again
                self.assertEqual([], notifications)

        # We got a tally of one for each
        tallies = self._read_queue(self._tallies_queue)
        digest = self._digest_tallies(tallies)
        self.assertEqual(expected_digest, digest)

        # Test aggregation
        notifications = map(partial(self._mock_sqs_record), tallies)
        with patch.object(IndexWriter, 'write', side_effect=TransportError):
            try:
                self.controller.aggregate(notifications)
            except TransportError:
                pass
            else:
                self.fail()

        self.assertEqual([], self._read_queue(self._tallies_queue))

        # Poison the two project and the two bundle tallies, by simulating
        # a number of failed attempts at processing them
        attempts = self.controller.num_batched_aggregation_attempts
        # While 0 is a valid value, the test logic below wouldn't work with it
        self.assertGreater(attempts, 0)
        notifications = [
            self._mock_sqs_record(tally,
                                  attempts=(attempts + 1
                                            if tally['entity_type'] in {'bundles', 'projects'}
                                            else 1))
            for tally in tallies
        ]
        self.controller.aggregate(notifications, retry=True)

        tallies = self._read_queue(self._tallies_retry_queue)
        digest = self._digest_tallies(tallies)
        # The two project tallies were consolidated (despite being poisoned) and
        # the resulting tally was deferred
        expected_digest['projects'] = [2]
        # One of the poisoned bundle tallies was referred. Since it was
        # poisoned, all other tallies were deferred
        expected_digest['bundles'] = [1]
        self.assertEqual(expected_digest, digest)

        # Aggregate the remaining deferred tallies
        notifications = map(self._mock_sqs_record, tallies)
        self.controller.aggregate(notifications, retry=True)

        # All tallies were referred
        self.assertEqual([], self._read_queue(self._tallies_retry_queue))
        self.assertEqual([], self._read_queue(self._tallies_queue))

    def _digest_tallies(self, tallies):
        entities = defaultdict(list)
        for tally in tallies:
            insort(entities[tally['entity_type']], tally['num_contributions'])
        return entities
