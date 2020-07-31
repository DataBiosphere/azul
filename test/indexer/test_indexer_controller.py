import json
import logging
from typing import (
    List,
    Sequence,
)

import boto3
from chalice.app import (
    SQSRecord,
)
import mock
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
    BundleFQID,
)
from azul.indexer.document import (
    Contribution,
)
from azul.indexer.index_controller import (
    IndexController,
)
from azul.indexer.index_service import (
    CataloguedTallies,
)
from azul.logging import (
    configure_test_logging,
)
from indexer import (
    ForcedRefreshIndexService,
    IndexerTestCase,
)

log = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


Contributions = Sequence[Contribution]


@mock_sts
@mock_sqs
class TestIndexController(IndexerTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.index_service.create_indices(self.catalog)
        self.client = AzulClient()
        self.controller = IndexController()
        self.queue_manager = queues.Queues(delete=True)

    def tearDown(self):
        self.index_service.delete_indices(self.catalog)
        super().tearDown()

    def _create_mock_queues(self):
        sqs = boto3.resource('sqs')
        for queue_name in config.all_queue_names:
            sqs.create_queue(QueueName=queue_name)

    def _mock_sqs_record(self, **body):
        event_dict = {
            'body': json.dumps(body),
            'receiptHandle': 'ThisWasARandomString',
            'attributes': {'ApproximateReceiveCount': 1}
        }
        return SQSRecord(event_dict=event_dict, context='controller_test')

    def test_contribution_action_invalid(self):
        event = [
            self._mock_sqs_record(action='banana',
                                  notification='slug',
                                  catalog=self.catalog)
        ]
        self.assertRaises(AssertionError, self.controller.contribute, event)

    def test_contribution_action_reindex(self):
        event = [self._mock_sqs_record(action='reindex', prefix='ff')]
        with mock.patch('azul.azulclient.AzulClient.do_remote_reindex') as mock_reindex:
            mock_reindex.return_value = True
            self.controller.contribute(event)
            self.assertEqual(1, mock_reindex.call_count)

    def test_contribute_and_aggregate(self):
        """
        Index two bundles that make contributions to the same project.
        Inspect that the contributions match the tallies that are returned to
        SQS. During aggregation only the project entity is deferred due to
        multiple contributions.
        """
        self._create_mock_queues()
        event = []
        bundles = []
        contributions_per_bundle: List[Contributions] = []
        bundle_fqids = [
            BundleFQID('56a338fe-7554-4b5d-96a2-7df127a7640b', '2018-03-28T151023.074974Z'),
            BundleFQID('b2216048-7eaa-45f4-8077-5a3fb4204953', '2018-03-29T104041.822717Z')
        ]
        for bundle_fqid in bundle_fqids:
            notification = self.client.synthesize_notification(self.catalog, bundle_fqid)
            event.append(self._mock_sqs_record(action='add',
                                               catalog=self.catalog,
                                               notification=notification))
            bundle = self._load_canned_bundle(bundle_fqid)
            bundles.append(bundle)
            contributions_per_bundle.append(self.index_service.transform(self.catalog, bundle=bundle, delete=False))
        repository_patch = mock.patch('azul.indexer.index_controller.IndexController.repository_plugin')
        service_patch = mock.patch('azul.indexer.index_controller.IndexController.index_service',
                                   new_callable=mock.PropertyMock)
        with repository_patch as mock_repository, service_patch as mock_index_service:
            mock_index_service.return_value = ForcedRefreshIndexService()
            mock_repository.return_value = mock.MagicMock()
            mock_repository.return_value.fetch_bundle.side_effect = bundles
            # Test contributions from two bundles with from the same project
            tallies = self._assert_notification_contributions(event, contributions_per_bundle)
            # Test aggregation for tallies, inspect for deferred tallies
            project_tally = self._assert_aggregation_creates_deferral(tallies)
            # Test aggregation for deferred tallies
            self._assert_aggregation_handles_deferral(project_tally)

    def _assert_notification_contributions(self,
                                           event: Sequence[SQSRecord],
                                           contributions_per_bundle: Sequence[Contributions]):
        self.controller.contribute(event)
        tallies = [
            json.loads(m.body)
            for m in self.queue_manager.read_messages(self.controller._tallies_queue())
        ]
        entities_from_tallies = {(t['entity_id'], t['entity_type']) for t in tallies}
        entities_from_contributions = {
            (c.entity.entity_id, c.entity.entity_type)
            for cs in contributions_per_bundle for c in cs
        }
        self.assertSetEqual(entities_from_tallies, entities_from_contributions)
        return tallies

    def _assert_aggregation_creates_deferral(self, tallies: Sequence[CataloguedTallies]):
        event = []
        for tally in tallies:
            event.append(self._mock_sqs_record(**tally))
        self.controller.aggregate(event)
        messages = self.queue_manager.read_messages(self.controller._tallies_queue())
        # Check that the deferral for the project entity was returned to the tallies queue.
        self.assertEqual(1, len(messages))
        project_tally = json.loads(messages[0].body)
        expected_tally = {
            'catalog': 'test',
            'entity_type': 'projects',
            'entity_id': '93f6a42f-1790-4af4-b5d1-8c436cb6feae',
            'num_contributions': 2
        }
        self.assertDictEqual(project_tally, expected_tally)
        return project_tally

    def _assert_aggregation_handles_deferral(self, deffered_tally: CataloguedTallies):
        event = [self._mock_sqs_record(**deffered_tally)]
        self.controller.aggregate(event)
        messages = self.queue_manager.read_messages(self.controller._tallies_queue())
        self.assertEqual(0, len(messages), )
