from collections import defaultdict
import http
import json
import logging
import time
from typing import (
    List,
    MutableMapping,
    cast,
)
import uuid

from boltons.cacheutils import cachedproperty
import boto3
import chalice
from chalice.app import (
    Request,
    SQSRecord,
)
from dataclasses import (
    dataclass,
    replace,
)
from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata
from more_itertools import chunked

from azul import (
    config,
    hmac,
)
from azul.azulclient import AzulClient
from azul.dss import direct_access_client
from azul.indexer import Bundle
from azul.indexer.document import (
    Contribution,
    EntityReference,
)
from azul.indexer.index_service import IndexService
from azul.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
)

log = logging.getLogger(__name__)


class IndexController:
    # The number of documents to be queued in a single SQS `send_messages`.
    # Theoretically, larger batches are better but SQS currently limits the
    # batch size to 10.
    #
    document_batch_size = 10

    @cachedproperty
    def index_service(self):
        return IndexService()

    def handle_notification(self, request: Request):
        hmac.verify(current_request=request)
        notification = request.json_body
        log.info("Received notification %r", notification)
        self._validate_notification(notification)
        if request.context['path'] == '/':
            result = self._handle_notification('add', notification)
        elif request.context['path'] in ('/delete', '/delete/'):
            result = self._handle_notification('delete', notification)
        else:
            assert False
        return result

    def _handle_notification(self, action: str, notification: JSON):
        if config.test_mode:
            if 'test_name' not in notification:
                log.error('Rejecting non-test notification in test mode: %r.', notification)
                raise chalice.ChaliceViewError('The indexer is currently in test mode where it only accepts specially '
                                               'instrumented notifications. Please try again later')
        else:
            if 'test_name' in notification:
                log.error('Rejecting test notification in production mode: %r.', notification)
                raise chalice.BadRequestError('Cannot process test notifications outside of test mode')

        message = dict(action=action, notification=notification)
        self._notify_queue.send_message(MessageBody=json.dumps(message))
        log.info("Queued notification %r", notification)
        return chalice.app.Response(body='', status_code=http.HTTPStatus.ACCEPTED)

    def _validate_notification(self, notification):
        try:
            match = notification['match']
        except KeyError:
            raise chalice.BadRequestError('Missing notification entry: match')

        try:
            bundle_uuid = match['bundle_uuid']
        except KeyError:
            raise chalice.BadRequestError('Missing notification entry: bundle_uuid')

        try:
            bundle_version = match['bundle_version']
        except KeyError:
            raise chalice.BadRequestError('Missing notification entry: bundle_version')

        if not isinstance(bundle_uuid, str):
            raise chalice.BadRequestError(f'Invalid type: bundle_uuid: {type(bundle_uuid)} (should be str)')

        if not isinstance(bundle_version, str):
            raise chalice.BadRequestError(f'Invalid type: bundle_version: {type(bundle_version)} (should be str)')

        if bundle_uuid.lower() != str(uuid.UUID(bundle_uuid)).lower():
            raise chalice.BadRequestError(f'Invalid syntax: {bundle_uuid} (should be a UUID)')

        if not bundle_version:
            raise chalice.BadRequestError('Invalid syntax: bundle_version can not be empty')

    def contribute(self, event):
        for record in event:
            message = json.loads(record.body)
            attempts = record.to_dict()['attributes']['ApproximateReceiveCount']
            log.info(f'Worker handling message {message}, attempt #{attempts} (approx).')
            start = time.time()
            try:
                action = message['action']
                if action == 'reindex':
                    AzulClient.do_remote_reindex(message)
                else:
                    notification = message['notification']
                    if action == 'add':
                        contributions = self.transform(notification, delete=False)
                    elif action == 'delete':
                        contributions = self.transform(notification, delete=True)
                    else:
                        assert False

                    log.info("Writing %i contributions to index.", len(contributions))
                    tallies = self.index_service.contribute(contributions)
                    tallies = [DocumentTally.for_entity(entity, num_contributions)
                               for entity, num_contributions in tallies.items()]

                    log.info("Queueing %i entities for aggregating a total of %i contributions.",
                             len(tallies), sum(tally.num_contributions for tally in tallies))
                    for batch in chunked(tallies, self.document_batch_size):
                        entries = [dict(tally.to_message(), Id=str(i)) for i, tally in enumerate(batch)]
                        self._document_queue.send_messages(Entries=entries)
            except BaseException:
                log.warning(f"Worker failed to handle message {message}.", exc_info=True)
                raise
            else:
                duration = time.time() - start
                log.info(f'Worker successfully handled message {message} in {duration:.3f}s.')

    def transform(self, dss_notification: JSON, delete: bool) -> List[Contribution]:
        """
        Transform the metadata in the bundle referenced by the given
        notification into a list of contributions to documents, each document
        representing one metadata entity in the index.
        """
        bundle_uuid = dss_notification['match']['bundle_uuid']
        bundle_version = dss_notification['match']['bundle_version']
        bundle = self._get_bundle(bundle_uuid, bundle_version)

        # Filter out bundles that don't have project metadata. `project.json` is
        # used in very old v5 bundles which only occur as cans in tests.
        if 'project_0.json' in bundle.metadata_files or 'project.json' in bundle.metadata_files:
            self._add_test_modifications(bundle, dss_notification)
            return self.index_service.transform(bundle, delete)
        else:
            log.warning('Ignoring bundle %s, version %s because it lacks project metadata.')
            return []

    def _get_bundle(self, bundle_uuid, bundle_version) -> Bundle:
        now = time.time()
        dss_client = direct_access_client(num_workers=config.num_dss_workers)
        _, manifest, metadata_files = download_bundle_metadata(client=dss_client,
                                                               replica='aws',
                                                               uuid=bundle_uuid,
                                                               version=bundle_version,
                                                               num_workers=config.num_dss_workers)
        log.info("It took %.003fs to download bundle %s.%s", time.time() - now, bundle_uuid, bundle_version)
        assert _ == bundle_version
        return Bundle(uuid=bundle_uuid,
                      version=bundle_version,
                      # FIXME: remove need for cast by fixing declaration in metadata API
                      #        https://github.com/DataBiosphere/hca-metadata-api/issues/13
                      manifest=cast(MutableJSONs, manifest),
                      metadata_files=cast(MutableJSON, metadata_files))

    def _add_test_modifications(self, bundle: Bundle, dss_notification: JSON) -> None:
        try:
            test_name = dss_notification['test_name']
        except KeyError:
            pass
        else:
            for file in bundle.manifest:
                if file['name'] == 'project_0.json':
                    test_uuid = dss_notification['test_uuid']
                    file['uuid'] = test_uuid
                    project_json = bundle.metadata_files['project_0.json']
                    project_json['project_core']['project_short_name'] = test_name
                    project_json['provenance']['document_id'] = test_uuid
                    break
            else:
                assert False
            # When indexing a test bundle we want to change its UUID so that we
            # can delete it later. We change the version to ensure that the test
            # bundle will always be selected to contribute to a shared entity
            # (the test bundle version was set to the current time when the
            # notification is sent).
            bundle.uuid = dss_notification['test_bundle_uuid']
            bundle.version = dss_notification['test_bundle_version']

    def aggregate(self, event):
        # Consolidate multiple tallies for the same entity and process entities with only one message. Because SQS FIFO
        # queues try to put as many messages from the same message group in a reception batch, a single message per
        # group may indicate that that message is the last one in the group. Inversely, multiple messages per group
        # in a batch are a likely indicator for the presence of even more queued messages in that group. The more
        # bundle contributions we defer, the higher the amortized savings on aggregation become. Aggregating bundle
        # contributions is a costly operation for any entity with many contributions e.g., a large project.
        #
        tallies_by_id: MutableMapping[EntityReference, List[DocumentTally]] = defaultdict(list)
        for record in event:
            tally = DocumentTally.from_sqs_record(record)
            log.info('Attempt %i of handling %i contribution(s) for entity %s/%s',
                     tally.attempts, tally.num_contributions, tally.entity.entity_type, tally.entity.entity_id)
            tallies_by_id[tally.entity].append(tally)
        deferrals, referrals = [], []
        for tallies in tallies_by_id.values():
            if len(tallies) == 1:
                referrals.append(tallies[0])
            elif len(tallies) > 1:
                deferrals.append(tallies[0].consolidate(tallies[1:]))
            else:
                assert False
        if referrals:
            for tally in referrals:
                log.info('Aggregating %i contribution(s) to entity %s/%s',
                         tally.num_contributions, tally.entity.entity_type, tally.entity.entity_id)
            tallies = {tally.entity: tally.num_contributions for tally in referrals}
            self.index_service.aggregate(tallies)
        if deferrals:
            for tally in deferrals:
                log.info('Deferring aggregation of %i contribution(s) to entity %s/%s',
                         tally.num_contributions, tally.entity.entity_type, tally.entity.entity_id)
            entries = [dict(tally.to_message(), Id=str(i)) for i, tally in enumerate(deferrals)]
            self._document_queue.send_messages(Entries=entries)

    @cachedproperty
    def _sqs(self):
        return boto3.resource('sqs')

    def _queue(self, queue_name):
        return self._sqs.get_queue_by_name(QueueName=queue_name)

    @property
    def _notify_queue(self):
        return self._queue(config.notify_queue_name)

    @property
    def _document_queue(self):
        return self._queue(config.document_queue_name)


@dataclass(frozen=True)
class DocumentTally:
    """
    Tracks the number of bundle contributions to a particular metadata entity.

    Each instance represents a message in the document queue.
    """
    entity: EntityReference
    num_contributions: int
    attempts: int

    @classmethod
    def from_sqs_record(cls, record: SQSRecord) -> 'DocumentTally':
        body = json.loads(record.body)
        attributes = record.to_dict()['attributes']
        return cls(entity=EntityReference(entity_type=body['entity_type'],
                                          entity_id=body['entity_id']),
                   num_contributions=body['num_contributions'],
                   attempts=int(attributes['ApproximateReceiveCount']))

    @classmethod
    def for_entity(cls, entity: EntityReference, num_contributions: int) -> 'DocumentTally':
        return cls(entity=entity,
                   num_contributions=num_contributions,
                   attempts=0)

    def to_json(self) -> JSON:
        return {
            'entity_type': self.entity.entity_type,
            'entity_id': self.entity.entity_id,
            'num_contributions': self.num_contributions
        }

    def to_message(self) -> JSON:
        return dict(MessageBody=json.dumps(self.to_json()),
                    MessageGroupId=self.entity.entity_id,
                    MessageDeduplicationId=str(uuid.uuid4()))

    def consolidate(self, others: List['DocumentTally']) -> 'DocumentTally':
        assert all(self.entity == other.entity for other in others)
        return replace(self, num_contributions=sum((other.num_contributions for other in others),
                                                   self.num_contributions))
