from collections import (
    defaultdict,
)
import logging
from typing import (
    Iterable,
    Self,
    TYPE_CHECKING,
)

import attrs

from azul import (
    CatalogName,
    config,
)
from azul.deployment import (
    aws,
)
from azul.indexer import (
    BundlePartition,
)
from azul.indexer.document import (
    CataloguedEntityReference,
    Contribution,
    EntityReference,
    Replica,
)
from azul.indexer.index_service import (
    IndexService,
)
from azul.indexer.repository_service import (
    RepositoryService,
)
from azul.lib import (
    cached_property,
)
from azul.lib.types import (
    JSON,
    json_int,
    json_mapping,
    json_str,
)
from azul.queues import (
    Action,
    Queues,
    SQSFifoMessage,
    SQSMessage,
)
from azul.source import (
    SourceRef,
    SourceSpec,
)

if TYPE_CHECKING:
    from mypy_boto3_sqs.service_resource import (
        Queue,
    )

log = logging.getLogger(__name__)


@attrs.frozen(kw_only=True)
class IndexAction(Action):
    catalog: CatalogName


@attrs.frozen(kw_only=True)
class IndexBundleAction(IndexAction):
    bundle_fqid: JSON
    bundle_partition: BundlePartition


class AddBundleAction(IndexBundleAction):
    pass


class DeleteBundleAction(IndexBundleAction):
    pass


@attrs.frozen(kw_only=True)
class IndexPartitionAction(IndexAction):
    source: SourceRef
    prefix: str


class IndexQueueService:

    @cached_property
    def _index_service(self) -> IndexService:
        return IndexService()

    @cached_property
    def _repository_service(self) -> RepositoryService:
        return RepositoryService()

    @cached_property
    def _queues(self) -> Queues:
        return Queues()

    def _notifications_queue(self, *, retry: bool = False) -> Queue:
        name = config.notifications_queue.derive(retry=retry).name
        return aws.sqs_queue(name)

    def _tallies_queue(self, *, retry: bool = False) -> Queue:
        name = config.tallies_queue.derive(retry=retry).name
        return aws.sqs_queue(name)

    def queue_notifications(self,
                            messages: Iterable[SQSMessage],
                            *,
                            retry: bool = False
                            ) -> int:
        queue = self._notifications_queue(retry=retry)
        return self._queues.send_messages(queue, messages)

    def queue_notification(self,
                           message: SQSMessage,
                           *,
                           retry: bool
                           ) -> None:
        queue = self._notifications_queue(retry=retry)
        self._queues.send_message(queue, message)

    def _queue_tallies(self,
                       messages: Iterable[SQSMessage],
                       *,
                       retry: bool = False
                       ) -> int:
        queue = self._tallies_queue(retry=retry)
        # Logging tallies would be excessively verbose
        return self._queues.send_messages(queue, messages, log_level=0)

    def index_bundle_message(self,
                             catalog: CatalogName,
                             bundle_fqid: JSON,
                             bundle_partition: BundlePartition = BundlePartition.root,
                             *,
                             delete: bool = False
                             ) -> SQSMessage:
        action_cls = DeleteBundleAction if delete else AddBundleAction
        action = action_cls(catalog=catalog,
                            bundle_fqid=bundle_fqid,
                            bundle_partition=bundle_partition)
        return SQSMessage(body=json_mapping(action.to_json()))

    def index_partition_message(self,
                                catalog: CatalogName,
                                source: SourceRef,
                                prefix: str
                                ) -> SQSMessage:
        action = IndexPartitionAction(catalog=catalog,
                                      source=source,
                                      prefix=prefix)
        return SQSMessage(body=json_mapping(action.to_json()))

    def index_catalog(self, catalog: CatalogName, sources: Iterable[SourceSpec]):
        service = self._repository_service
        plugin = service.repository_plugin(catalog)
        for source_spec in sources:
            source_ref = plugin.resolve_source(source_spec)
            source_ref = plugin.partition_source_for_indexing(catalog, source_ref)
            prefix = source_ref.prefix
            assert prefix is not None, source_ref

            def message(partition_prefix: str) -> SQSMessage:
                log.info('Remotely reindexing prefix %r of source %r into catalog %r',
                         partition_prefix, str(source_ref.spec), catalog)
                return self.index_partition_message(catalog, source_ref, partition_prefix)

            messages = map(message, prefix.partition_prefixes())
            self.queue_notifications(messages)

    def contribute(self, action: IndexAction):
        if isinstance(action, IndexPartitionAction):
            self._index_partition(action)
        elif isinstance(action, IndexBundleAction):
            self._index_bundle(action)

    def _index_partition(self, message: IndexPartitionAction) -> None:
        service = self._repository_service
        catalog, prefix, source = message.catalog, message.prefix, message.source
        assert isinstance(catalog, str) and isinstance(prefix, str)
        bundle_fqids = service.list_bundles(catalog, source, prefix)
        # All AnVIL bundles and entities use the same version
        if not config.is_anvil_enabled(catalog):
            bundle_fqids = service.filter_obsolete_bundle_versions(bundle_fqids)
            log.info('After filtering obsolete versions, '
                     '%i bundles remain in prefix %r of source %r in catalog %r',
                     len(bundle_fqids), prefix, str(source.spec), catalog)
        messages = (
            self.index_bundle_message(catalog, bundle_fqid.to_json())
            for bundle_fqid in bundle_fqids
        )
        num_messages = self.queue_notifications(messages)
        log.info('Successfully queued %i notification(s) for prefix %s of '
                 'source %r', num_messages, prefix, source)

    def _index_bundle(self, message: IndexBundleAction):
        catalog = json_str(message.catalog)
        assert catalog is not None
        delete = isinstance(message, DeleteBundleAction)
        if not delete:
            assert isinstance(message, AddBundleAction)
        bundle_fqid = json_mapping(message.bundle_fqid)
        bundle_partition = message.bundle_partition
        contributions, replicas = self._transform(catalog,
                                                  bundle_fqid,
                                                  bundle_partition,
                                                  delete=delete)
        log.info('Writing %i contributions to index.', len(contributions))
        tallies = self._index_service.contribute(catalog, contributions)
        tallies = [DocumentTally.for_entity(catalog, entity, num_contributions)
                   for entity, num_contributions in tallies.items()]

        if replicas:
            if delete:
                # FIXME: Replica index does not support deletions
                #        https://github.com/DataBiosphere/azul/issues/5846
                log.warning('Deletion of replicas is not supported')
            else:
                log.info('Writing %i replicas to index.', len(replicas))
                num_written = self._index_service.replicate(catalog, replicas)
                log.info('Successfully wrote %i replicas', num_written)
        else:
            log.info('No replicas to write.')

        log.info('Queueing %i entities for aggregating a total of %i contributions.',
                 len(tallies), sum(tally.num_contributions for tally in tallies))
        messages = (tally.to_message() for tally in tallies)
        self._queue_tallies(messages)

    def _transform(self,
                   catalog: CatalogName,
                   bundle_fqid: JSON,
                   bundle_partition: BundlePartition,
                   *,
                   delete: bool
                   ) -> tuple[list[Contribution], list[Replica]]:
        """
        Transform the metadata in the bundle referenced by the given
        notification into a list of contributions to documents, each document
        representing one metadata entity in the index. Replicas of the original,
        untransformed metadata are returned as well.
        """
        bundle = self._repository_service.fetch_bundle(catalog,
                                                       bundle_fqid)
        results = self._index_service.transform(catalog,
                                                bundle,
                                                bundle_partition,
                                                delete=delete)
        if isinstance(results, list):
            for bundle_partition in results:
                assert isinstance(bundle_partition, BundlePartition)
                # There's a good chance that the partition will also fail in
                # the non-retry Lambda function so we'll go straight to retry.
                message = self.index_bundle_message(catalog,
                                                    bundle_fqid,
                                                    bundle_partition,
                                                    delete=delete)
                self.queue_notification(message, retry=True)
            return [], []
        elif isinstance(results, tuple):
            return results
        else:
            assert False, results

    #: The number of failed attempts before a tally is referred as a batch of 1.
    #: Note that the retry lambda does first attempts, too, namely on re-fed and
    #: deferred tallies.
    #
    _num_batched_aggregation_attempts = 3

    def aggregate(self, tallies: list[DocumentTally], *, retry: bool):
        tallies_by_entity: dict[CataloguedEntityReference, list[DocumentTally]] = defaultdict(list)
        for tally in tallies:
            tallies_by_entity[tally.entity].append(tally)
        deferrals, referrals = [], []
        for tallies in tallies_by_entity.values():
            if len(tallies) == 1:
                referrals.append(tallies[0])
            elif len(tallies) > 1:
                deferrals.append(tallies[0].consolidate(tallies[1:]))
            else:
                assert False
        if referrals:
            for i, tally in enumerate(referrals):
                if tally.attempts > self._num_batched_aggregation_attempts:
                    log.info('Only aggregating problematic entity %s, deferring all others',
                             tally.entity)
                    referrals.pop(i)
                    deferrals.extend(referrals)
                    referrals = [tally]
                    break

            log.info('Referring %i tallies', len(referrals))
            tally_by_entity = {}
            for tally in referrals:
                log.info('Aggregating %i contribution(s) to entity %s',
                         tally.num_contributions, tally.entity)
                tally_by_entity[tally.entity] = tally.num_contributions

            self._index_service.aggregate(tally_by_entity)

            for tally in referrals:
                log.info('Successfully aggregated %i contribution(s) to entity %s',
                         tally.num_contributions, tally.entity)
            log.info('Successfully referred %i tallies', len(referrals))
        if deferrals:
            log.info('Deferring %i tallies', len(deferrals))
            for tally in deferrals:
                log.info('Deferring aggregation of %i contribution(s) to entity %s',
                         tally.num_contributions, tally.entity)
            messages = (tally.to_message() for tally in deferrals)
            # Hopefully this is more or less atomic. If we crash below here,
            # tallies will be inflated because some or all deferrals have
            # been sent and the original tallies will be returned.
            self._queue_tallies(messages, retry=retry)


@attrs.frozen(kw_only=True)
class DocumentTally:
    """
    Tracks the number of bundle contributions to a particular metadata entity.

    Each instance represents a message in the document queue.
    """
    entity: CataloguedEntityReference
    num_contributions: int
    attempts: int

    @classmethod
    def for_entity(cls,
                   catalog: CatalogName,
                   entity: EntityReference,
                   num_contributions: int) -> Self:
        return cls(entity=CataloguedEntityReference(catalog=catalog,
                                                    entity_type=entity.entity_type,
                                                    entity_id=entity.entity_id),
                   num_contributions=num_contributions,
                   attempts=0)

    @classmethod
    def from_message(cls, msg: SQSFifoMessage) -> Self:
        return cls.from_json(msg.body, json_int(msg.attempts))

    @classmethod
    def from_json(cls, json: JSON, attempts: int) -> Self:
        return cls(entity=CataloguedEntityReference(catalog=json_str(json['catalog']),
                                                    entity_type=json_str(json['entity_type']),
                                                    entity_id=json_str(json['entity_id'])),
                   num_contributions=json_int(json['num_contributions']),
                   attempts=attempts)

    def to_json(self) -> JSON:
        return {
            'catalog': self.entity.catalog,
            'entity_type': self.entity.entity_type,
            'entity_id': self.entity.entity_id,
            'num_contributions': self.num_contributions
        }

    def to_message(self) -> SQSFifoMessage:
        return SQSFifoMessage(body=self.to_json(),
                              group_id=str(self.entity))

    def consolidate(self, others: list[DocumentTally]) -> Self:
        assert all(self.entity == other.entity for other in others)
        num_contributions = sum((other.num_contributions for other in others),
                                start=self.num_contributions)
        return attrs.evolve(self, num_contributions=num_contributions)
