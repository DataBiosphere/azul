from bisect import (
    insort,
)
from collections import (
    defaultdict,
)
from functools import (
    partial,
)
from unittest.mock import (
    MagicMock,
    PropertyMock,
    call,
    patch,
)
from uuid import (
    uuid4,
)

import attrs
from more_itertools import (
    one,
)
from moto import (
    mock_aws,
)
from opensearchpy import (
    TransportError,
)
from requests import (
    Request,
    Response,
    Session,
)

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.hmac import (
    SignatureHelper,
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
from azul.indexer.index_queue_service import (
    IndexQueueService,
)
from azul.indexer.index_service import (
    IndexWriter,
)
from azul.indexer.repository_service import (
    RepositoryService,
)
from azul.lib.types import (
    JSON,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.plugins.repository.tdr import (
    TDRBundleFQID,
    TDRPlugin,
)
from azul.plugins.repository.tdr_hca import (
    Plugin,
)
from azul.terra import (
    TDRSourceRef,
)
from azul_test_case import (
    BundleNotificationTestCase,
    DCP1TestCase,
    DCP2TestCase,
)
from indexer.test_indexer import (
    DCP2IndexerTestCase,
)
from sqs_test_case import (
    SqsTestCase,
    WorkQueueTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


@mock_aws
class TestIndexController(DCP2IndexerTestCase, WorkQueueTestCase):
    source = DCP2TestCase.source.with_prefix(
        attrs.evolve(DCP2TestCase.source.ref.prefix,
                     partition=0)
    )

    def setUp(self) -> None:
        super().setUp()
        self.index_service.create_indices(self.catalog)
        app = MagicMock()
        self.controller = IndexController(app=app)
        app.catalog = self.catalog
        IndexQueueService._index_service.fset(self.queue_service, self.index_service)

    @property
    def queue_service(self):
        return self.controller.index_queue_service

    @property
    def index_repository_service(self):
        return self.queue_service._repository_service

    def tearDown(self):
        self._purge_indices()
        super().tearDown()

    def _fqid_from_message(self, message: JSON) -> TDRBundleFQID:
        fqid = message['bundle_fqid']
        return TDRBundleFQID(uuid=fqid['uuid'],
                             version=fqid['version'],
                             source=TDRSourceRef.from_json(fqid['source']))

    def test_invalid_notification(self):
        event = [
            self._mock_sqs_record(dict(action='foo',
                                       source='foo_source',
                                       notification='bar',
                                       catalog=self.catalog))
        ]
        self.assertRaises(KeyError, self.controller.contribute, event)

    @patch.object(RepositoryPlugin, 'partition_source_for_indexing')
    @patch.object(TDRPlugin, 'resolve_source')
    def test_index_catalog(self, resolve_source, partition_source):
        source = self.source.ref
        resolve_source.return_value = attrs.evolve(source, prefix=None)
        partition_source.return_value = source
        plugin = self.index_repository_service.repository_plugin(self.catalog)
        plugin._assert_source(source)
        self._create_mock_queues(config.indexer_queue_names)
        self.queue_service.index_catalog(self.catalog, [source.spec])
        messages = one(self._read_queue(self.queue_service._notifications_queue()))
        expected_notification = dict(action='IndexPartitionAction',
                                     catalog=self.catalog,
                                     source=source.to_json(),
                                     prefix='')
        self.assertEqual(expected_notification, messages)
        event = [self._mock_sqs_record(messages)]

        bundle_fqids = [
            TDRBundleFQID(source=source,
                          uuid='4426adc5-b3c5-5aab-ab86-51d8ce44dfbe',
                          version='2020-08-10T21:24:26.174274Z')
        ]

        with patch.object(Plugin, 'list_bundles', return_value=bundle_fqids):
            self.controller.contribute(event)

        messages = one(self._read_queue(self.queue_service._notifications_queue()))
        expected_source = dict(id=source.id,
                               spec=str(source.spec),
                               prefix=str(source.prefix),
                               type=source.cls_to_json())
        source = messages['bundle_fqid']['source']
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
        self._create_mock_queues(config.indexer_queue_names)
        source = self.source.ref
        fqids = [
            TDRBundleFQID(source=source,
                          uuid='4426adc5-b3c5-5aab-ab86-51d8ce44dfbe',
                          version='2020-08-10T21:24:26.174274Z'),
            TDRBundleFQID(source=source,
                          uuid='1b6d8348-d6e9-406a-aa6a-7ee886e52bf9',
                          version='2019-09-24T09:35:06.958773Z')
        ]

        # Load canned bundles
        bundles = {
            fqid: self._load_canned_bundle(fqid)
            for fqid in fqids
        }

        # Synthesize initial notifications
        messages = [
            self.queue_service.index_bundle_message(self.catalog, fqid.to_json()).body
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
        for i in range(3):
            mock_plugin = MagicMock()
            notified_fqids = list(map(self._fqid_from_message, messages))
            notified_bundles = [bundles[fqid] for fqid in notified_fqids]
            mock_plugin.fetch_bundle.side_effect = notified_bundles
            type(mock_plugin).bundle_fqid_cls = PropertyMock(return_value=TDRBundleFQID)
            mock_plugin.sources = [source]
            with patch.object(RepositoryService,
                              'repository_plugin',
                              return_value=mock_plugin):
                with patch.object(BundlePartition,
                                  'max_partition_size',
                                  4):
                    event = list(map(self._mock_sqs_record, messages))
                    self.controller.contribute(event)

            # Assert plugin calls by controller
            expected_calls = list(map(call, notified_fqids))
            self.assertEqual(expected_calls, mock_plugin.fetch_bundle.mock_calls)

            # Assert partitioned notifications, straight from the retry queue
            messages = self._read_queue(self.queue_service._notifications_queue(retry=True))
            # Fingerprint the partitions from the resulting notifications
            partitions = defaultdict(set)
            for n in messages:
                fqid = self._fqid_from_message(n)
                partition = BundlePartition.from_json(n['bundle_partition'])
                partitions[fqid].add(partition)
            partitions = {k: len(v) for k, v in partitions.items()}
            if i == 0:
                # Assert that each bundle was partitioned. The number of
                # partitions for each bundle depends on the the number of
                # entities in that bundle and the patched max_partition_size
                self.assertEqual({fqids[0]: 2, fqids[1]: 8}, partitions)
            elif i == 1:
                # The partitions resulting from the first iteration should not
                # need to be partitioned again
                self.assertEqual({fqids[1]: 2}, partitions)
            elif i == 2:
                self.assertEqual({}, partitions)

        tallies_queue = self.queue_service._tallies_queue()
        tallies_retry_queue = self.queue_service._tallies_queue(retry=True)

        # We got a tally of one for each
        tallies = self._read_queue(tallies_queue)
        digest = self._digest_tallies(tallies)
        self.assertEqual(expected_digest, digest)

        # Test aggregation
        messages = map(partial(self._mock_sqs_record, fifo=True), tallies)
        with patch.object(IndexWriter, 'write', side_effect=TransportError):
            try:
                self.controller.aggregate(messages)
            except TransportError:
                pass
            else:
                self.fail()

        self.assertEqual([], self._read_queue(tallies_queue))

        # Poison the two project and the two bundle tallies, by simulating
        # a number of failed attempts at processing them
        attempts = self.queue_service._num_batched_aggregation_attempts
        # While 0 is a valid value, the test logic below wouldn't work with it
        self.assertGreater(attempts, 0)
        messages = [
            self._mock_sqs_record(tally,
                                  attempts=(attempts + 1
                                            if tally['entity_type'] in {'bundles', 'projects'}
                                            else 1),
                                  fifo=True)
            for tally in tallies
        ]
        self.controller.aggregate(messages, retry=True)

        tallies = self._read_queue(tallies_retry_queue)
        digest = self._digest_tallies(tallies)
        # The two project tallies were consolidated (despite being poisoned) and
        # the resulting tally was deferred
        expected_digest['projects'] = [2]
        # One of the poisoned bundle tallies was referred. Since it was
        # poisoned, all other tallies were deferred
        expected_digest['bundles'] = [1]
        self.assertEqual(expected_digest, digest)

        # Aggregate the remaining deferred tallies
        messages = map(partial(self._mock_sqs_record, fifo=True), tallies)
        self.controller.aggregate(messages, retry=True)

        # All tallies were referred
        self.assertEqual([], self._read_queue(tallies_queue))
        self.assertEqual([], self._read_queue(tallies_retry_queue))

    def _digest_tallies(self, tallies):
        entities = defaultdict(list)
        for tally in tallies:
            insort(entities[tally['entity_type']], tally['num_contributions'])
        return entities


class TestIndexerApp(LocalAppTestCase,
                     DCP1TestCase,
                     SqsTestCase,
                     BundleNotificationTestCase):

    @classmethod
    def app_name(cls) -> str:
        return 'indexer'

    @mock_aws
    def test_successful_notifications(self):
        self._create_mock_notifications_queue()
        body = {
            'bundle_fqid': {
                'uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd',
                'version': '2018-03-28T13:55:26.044Z'
            }
        }
        for delete in False, True:
            with self.subTest(delete=delete):
                response = self._test(body, delete=delete, valid_hmac_key=True)
                self.assertEqual(202, response.status_code)
                self.assertEqual('', response.text)

    @mock_aws
    def test_invalid_notifications(self):
        bodies = {
            'Missing notification entry: bundle_fqid': {},
            'Missing notification entry: bundle_fqid.uuid': {
                'bundle_fqid': {
                    'version': '2018-03-28T13:55:26.044Z'
                }
            },
            "Invalid type: uuid: <class 'NoneType'> (should be str)": {
                'bundle_fqid': {
                    'uuid': None,
                    'version': '2018-03-28T13:55:26.044Z'
                }
            },
            'Missing notification entry: bundle_fqid.version': {
                'bundle_fqid': {
                    'uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd'
                }
            },
            "Invalid type: version: <class 'NoneType'> (should be str)": {
                'bundle_fqid': {
                    'uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd',
                    'version': None
                }
            },
            'Invalid syntax: }9fccaed8-cdbc-445e-a3a0-6edc11f4b73f{ (should be a UUID)': {
                'bundle_fqid': {
                    'uuid': '}9fccaed8-cdbc-445e-a3a0-6edc11f4b73f{',
                    'version': '2019-12-31T00:00:00.000Z'
                }
            },
            'Invalid syntax: bundle_version can not be empty': {
                'bundle_fqid': {
                    'uuid': str(uuid4()),
                    'version': ''
                }
            }
        }
        for delete in False, True:
            for expected_message, body in bodies.items():
                with self.subTest(delete=delete, expected_message=expected_message):
                    response = self._test(body, delete=delete, valid_hmac_key=True)
                    expected_response = {
                        'Code': 'BadRequestError',
                        'Message': expected_message
                    }
                    self.assertEqual(400, response.status_code)
                    self.assertEqual(expected_response, response.json())

    @mock_aws
    def test_unauthorized_notification(self):
        self._create_mock_notifications_queue()
        body = {
            'bundle_fqid': {
                'uuid': str(uuid4()),
                'version': 'SomeBundleVersion'
            }
        }
        for delete in False, True:
            with self.subTest(delete=delete):
                response = self._test(body, delete=delete, valid_hmac_key=False)
                self.assertEqual(401, response.status_code)

    def test_invalid_catalog(self):
        for delete in False, True:
            with self.subTest(delete=delete):
                with patch.object(self, 'catalog', '-'):
                    response = self._test({}, delete=delete, valid_hmac_key=True)
                    expected_response = {
                        'Code': 'BadRequestError',
                        'Message': "('Catalog name is invalid', '-')"
                    }
                    self.assertEqual(400, response.status_code)
                    self.assertEqual(expected_response, response.json())

    def test_invalid_auth(self):
        for delete in False, True:
            with self.subTest(delete=delete):
                request = self._prepare_request({}, delete=delete)
                request.headers['Authorization'] = 'Bearer foo'
                with Session() as session:
                    response = session.send(request.prepare())
                self.assertEqual(401, response.status_code)
                expected_response = {
                    'Code': 'UnauthorizedError',
                    'Message': 'Expecting HMAC authentication'
                }
                self.assertEqual(expected_response, response.json())

    def _test(self, body: JSON, *, delete: bool, valid_hmac_key: bool) -> Response:
        request = self._prepare_request(body, delete)
        with patch.object(aws, 'get_hmac_key_and_id') as get_hmac_key_and_id:
            get_hmac_key_and_id.return_value = b'good key', 'the id'
            hmac_support = SignatureHelper()
            if valid_hmac_key:
                return hmac_support.sign_and_send(request)
            else:
                with patch.object(hmac_support, 'resolve_private_key') as p:
                    p.return_value = b'bad key'
                    return hmac_support.sign_and_send(request)

    def _prepare_request(self, body: JSON, delete: bool) -> Request:
        url = self.base_url.set(path=(self.catalog, 'bundles'))
        method = 'DELETE' if delete else 'POST'
        request = Request(method=method, url=str(url), json=body)
        return request

    def _create_mock_notifications_queue(self):
        self._create_mock_queues([config.notifications_queue.name])
