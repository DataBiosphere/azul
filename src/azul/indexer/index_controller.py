from collections import (
    defaultdict,
)
from dataclasses import (
    dataclass,
    replace,
)
import http
import json
import logging
import time
from typing import (
    List,
    MutableMapping,
)
import uuid

import chalice
from chalice.app import (
    SQSRecord,
    UnauthorizedError,
)
from more_itertools import (
    chunked,
)

from azul import (
    CatalogName,
    cache,
    cached_property,
    config,
    require,
)
from azul.azulclient import (
    AzulClient,
)
from azul.chalice import (
    AzulRequest,
)
from azul.deployment import (
    aws,
)
from azul.hmac import (
    HMACAuthentication,
)
from azul.indexer import (
    SourcedBundleFQID,
)
from azul.indexer.document import (
    Contribution,
    EntityReference,
)
from azul.indexer.index_service import (
    CataloguedEntityReference,
    IndexService,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.types import (
    JSON,
)

log = logging.getLogger(__name__)


class IndexController:
    # The number of documents to be queued in a single SQS `send_messages`.
    # Theoretically, larger batches are better but SQS currently limits the
    # batch size to 10.
    #
    document_batch_size = 10

    @cached_property
    def index_service(self):
        return IndexService()

    @cache
    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def handle_notification(self, catalog: CatalogName, action: str, request: AzulRequest):
        if isinstance(request.authentication, HMACAuthentication):
            assert request.authentication.identity() is not None
            config.Catalog.validate_name(catalog, exception=chalice.BadRequestError)
            require(action in ('add', 'delete'), exception=chalice.BadRequestError)
            notification = request.json_body
            log.info('Received notification %r for catalog %r', notification, catalog)
            self._validate_notification(notification)
            self._queue_notification(action, notification, catalog)
            return chalice.app.Response(body='', status_code=http.HTTPStatus.ACCEPTED)
        else:
            raise UnauthorizedError()

    def _queue_notification(self, action: str, notification: JSON, catalog: CatalogName):
        message = {
            'catalog': catalog,
            'action': action,
            'notification': notification
        }
        self._notifications_queue.send_message(MessageBody=json.dumps(message))
        log.info('Queued notification message %r', message)

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

    def contribute(self, event, retry=False):
        for record in event:
            message = json.loads(record.body)
            attempts = record.to_dict()['attributes']['ApproximateReceiveCount']
            log.info(f'Worker handling message {message}, attempt #{attempts} (approx).')
            start = time.time()
            try:
                action = message['action']
                if action == 'reindex':
                    AzulClient().remote_reindex_partition(message)
                else:
                    notification = message['notification']
                    catalog = message['catalog']
                    assert catalog is not None
                    if action == 'add':
                        delete = False
                    elif action == 'delete':
                        delete = True
                    else:
                        assert False
                    contributions = self.transform(catalog, notification, delete)
                    log.info("Writing %i contributions to index.", len(contributions))
                    tallies = self.index_service.contribute(catalog, contributions)
                    tallies = [DocumentTally.for_entity(catalog, entity, num_contributions)
                               for entity, num_contributions in tallies.items()]

                    log.info("Queueing %i entities for aggregating a total of %i contributions.",
                             len(tallies), sum(tally.num_contributions for tally in tallies))
                    for batch in chunked(tallies, self.document_batch_size):
                        entries = [dict(tally.to_message(), Id=str(i)) for i, tally in enumerate(batch)]
                        self._tallies_queue().send_messages(Entries=entries)
            except BaseException:
                log.warning(f"Worker failed to handle message {message}.", exc_info=True)
                raise
            else:
                duration = time.time() - start
                log.info(f'Worker successfully handled message {message} in {duration:.3f}s.')

    def transform(self, catalog: CatalogName, notification: JSON, delete: bool) -> List[Contribution]:
        """
        Transform the metadata in the bundle referenced by the given
        notification into a list of contributions to documents, each document
        representing one metadata entity in the index.
        """
        match, source = notification['match'], notification['source']
        plugin = self.repository_plugin(catalog)
        source = plugin.source_from_json(source)
        bundle_fqid = SourcedBundleFQID(source=source,
                                        uuid=match['bundle_uuid'],
                                        version=match['bundle_version'])
        bundle = plugin.fetch_bundle(bundle_fqid)

        # Filter out bundles that don't have project metadata. `project.json` is
        # used in very old v5 bundles which only occur as cans in tests.
        if 'project_0.json' in bundle.metadata_files or 'project.json' in bundle.metadata_files:
            return self.index_service.transform(catalog, bundle, delete)
        else:
            log.warning('Ignoring bundle %s, version %s because it lacks project metadata.')
            return []

    def aggregate(self, event, retry=False):
        # Consolidate multiple tallies for the same entity and process entities
        # with only one message. Because SQS FIFO queues try to put as many
        # messages from the same message group in a reception batch, a single
        # message per group may indicate that that message is the last one in
        # the group. Inversely, multiple messages per group in a batch are a
        # likely indicator for the presence of even more queued messages in
        # that group. The more bundle contributions we defer, the higher the
        # amortized savings on aggregation become. Aggregating bundle
        # contributions is a costly operation for any entity with many
        # contributions e.g., a large project.
        #
        tallies_by_entity: MutableMapping[CataloguedEntityReference, List[DocumentTally]] = defaultdict(list)
        for record in event:
            tally = DocumentTally.from_sqs_record(record)
            log.info('Attempt %i of handling %i contribution(s) for entity %s',
                     tally.attempts, tally.num_contributions, tally.entity)
            tallies_by_entity[tally.entity].append(tally)
        deferrals, referrals = [], []
        try:
            for tallies in tallies_by_entity.values():
                if len(tallies) == 1:
                    referrals.append(tallies[0])
                elif len(tallies) > 1:
                    deferrals.append(tallies[0].consolidate(tallies[1:]))
                else:
                    assert False
            if referrals:
                for tally in referrals:
                    log.info('Aggregating %i contribution(s) to entity %s',
                             tally.num_contributions, tally.entity)
                tallies = {
                    tally.entity: tally.num_contributions
                    for tally in referrals
                }
                self.index_service.aggregate(tallies)
            if deferrals:
                for tally in deferrals:
                    log.info('Deferring aggregation of %i contribution(s) to entity %s',
                             tally.num_contributions, tally.entity)
                entries = [dict(tally.to_message(), Id=str(i)) for i, tally in enumerate(deferrals)]
                # Hopefully this is more or less atomic. If we crash below here,
                # tallies will be inflated because some or all deferrals have
                # been sent and the original tallies will be returned.
                self._tallies_queue(retry=retry).send_messages(Entries=entries)
        except BaseException:
            log.warning('Failed to aggregate tallies: %r', tallies_by_entity.values(), exc_info=True)
            raise

    @property
    def _sqs(self):
        return aws.resource('sqs')

    def _queue(self, queue_name):
        return self._sqs.get_queue_by_name(QueueName=queue_name)

    @property
    def _notifications_queue(self, retry=False):
        return self._queue(config.notifications_queue_name(retry=retry))

    def _tallies_queue(self, retry=False):
        return self._queue(config.tallies_queue_name(retry=retry))


@dataclass(frozen=True)
class DocumentTally:
    """
    Tracks the number of bundle contributions to a particular metadata entity.

    Each instance represents a message in the document queue.
    """
    entity: CataloguedEntityReference
    num_contributions: int
    attempts: int

    @classmethod
    def from_sqs_record(cls, record: SQSRecord) -> 'DocumentTally':
        body = json.loads(record.body)
        attributes = record.to_dict()['attributes']
        return cls(entity=CataloguedEntityReference(catalog=body['catalog'],
                                                    entity_type=body['entity_type'],
                                                    entity_id=body['entity_id']),
                   num_contributions=body['num_contributions'],
                   attempts=int(attributes['ApproximateReceiveCount']))

    @classmethod
    def for_entity(cls,
                   catalog: CatalogName,
                   entity: EntityReference,
                   num_contributions: int) -> 'DocumentTally':
        return cls(entity=CataloguedEntityReference(catalog=catalog,
                                                    entity_type=entity.entity_type,
                                                    entity_id=entity.entity_id),
                   num_contributions=num_contributions,
                   attempts=0)

    def to_json(self) -> JSON:
        return {
            'catalog': self.entity.catalog,
            'entity_type': self.entity.entity_type,
            'entity_id': self.entity.entity_id,
            'num_contributions': self.num_contributions
        }

    def to_message(self) -> JSON:
        return dict(MessageBody=json.dumps(self.to_json()),
                    MessageGroupId=str(self.entity),
                    MessageDeduplicationId=str(uuid.uuid4()))

    def consolidate(self, others: List['DocumentTally']) -> 'DocumentTally':
        assert all(
            self.entity == other.entity
            for other in others
        )
        return replace(self, num_contributions=sum((other.num_contributions for other in others),
                                                   self.num_contributions))
