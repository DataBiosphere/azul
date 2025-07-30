from collections import (
    defaultdict,
)
from enum import (
    auto,
)
import logging
from typing import (
    Iterable,
    Self,
    TYPE_CHECKING,
    cast,
)

import attrs

from azul import (
    CatalogName,
    cached_property,
    config,
    json_mapping,
)
from azul.deployment import (
    aws,
)
from azul.indexer import (
    BundlePartition,
    SourceRef,
    SourceSpec,
)
from azul.indexer.document import (
    Contribution,
    EntityReference,
    Replica,
)
from azul.indexer.index_repository_service import (
    IndexRepositoryService,
)
from azul.indexer.index_service import (
    CataloguedEntityReference,
    IndexService,
)
from azul.queues import (
    Action,
    Queues,
    SQSFifoMessage,
    SQSMessage,
)
from azul.types import (
    JSON,
    json_int,
    json_str,
)

if TYPE_CHECKING:
    from mypy_boto3_sqs.service_resource import (
        Queue,
    )

log = logging.getLogger(__name__)


class IndexAction(Action):
    reindex = auto()
    add = auto()
    delete = auto()


class IndexQueueService:

    @cached_property
    def index_service(self) -> IndexService:
        return IndexService()

    @cached_property
    def index_repository_service(self) -> IndexRepositoryService:
        return IndexRepositoryService()

    @cached_property
    def queues(self) -> Queues:
        return Queues()

    def notifications_queue(self, *, retry: bool = False) -> 'Queue':
        name = config.notifications_queue.derive(retry=retry).name
        return aws.sqs_queue(name)

    def tallies_queue(self, *, retry: bool = False) -> 'Queue':
        name = config.tallies_queue.derive(retry=retry).name
        return aws.sqs_queue(name)

    def queue_notifications(self,
                            messages: Iterable[SQSMessage],
                            *,
                            retry: bool = False
                            ) -> int:
        queue = self.notifications_queue(retry=retry)
        return self.queues.send_messages(queue, messages)

    def queue_notification(self,
                           message: SQSMessage,
                           *,
                           retry: bool
                           ) -> None:
        queue = self.notifications_queue(retry=retry)
        self.queues.send_message(queue, message)
        log.info('Queued notification message %r', message)

    def queue_tallies(self,
                      messages: Iterable[SQSMessage],
                      *,
                      retry: bool = False
                      ) -> int:
        queue = self.tallies_queue(retry=retry)
        return self.queues.send_messages(queue, messages)

    def index_bundle_message(self,
                             action: IndexAction,
                             catalog: CatalogName,
                             bundle_fqid: JSON,
                             bundle_partition: BundlePartition = BundlePartition.root,
                             ) -> SQSMessage:
        return SQSMessage(
            body={
                'action': action.to_json(),
                'catalog': catalog,
                'bundle_fqid': bundle_fqid,
                'bundle_partition': bundle_partition.to_json(),
            }
        )

    def index_partition_message(self,
                                catalog: CatalogName,
                                source: SourceRef,
                                prefix: str
                                ) -> SQSMessage:
        return SQSMessage(
            body={
                'action': IndexAction.reindex.to_json(),
                'catalog': catalog,
                'source': cast(JSON, source.to_json()),
                'prefix': prefix
            }
        )

    def remote_reindex(self, catalog: CatalogName, sources: Iterable[SourceSpec]):
        service = self.index_repository_service
        plugin = service.repository_plugin(catalog)
        for source_spec in sources:
            source_ref = plugin.resolve_source(source_spec)
            source_ref = plugin.partition_source_for_indexing(catalog, source_ref)

            def message(partition_prefix: str) -> SQSMessage:
                log.info('Remotely reindexing prefix %r of source_ref %r into catalog %r',
                         partition_prefix, str(source_ref.spec), catalog)
                return self.index_partition_message(catalog, source_ref, partition_prefix)

            messages = map(message, source_ref.spec.prefix.partition_prefixes())
            self.queue_notifications(messages)

    def remote_reindex_partition(self, message: JSON) -> None:
        service = self.index_repository_service
        catalog, prefix = message['catalog'], message['prefix']
        assert isinstance(catalog, str) and isinstance(prefix, str)
        source = json_mapping(message['source'])
        plugin = service.repository_plugin(catalog)
        source = plugin.source_ref_cls.from_json(source)
        bundle_fqids = service.list_bundles(catalog, source, prefix)
        # All AnVIL bundles and entities use the same version
        if not config.is_anvil_enabled(catalog):
            bundle_fqids = service.filter_obsolete_bundle_versions(bundle_fqids)
            log.info('After filtering obsolete versions, '
                     '%i bundles remain in prefix %r of source %r in catalog %r',
                     len(bundle_fqids), prefix, str(source.spec), catalog)
        messages = (
            self.index_bundle_message(IndexAction.add, catalog, bundle_fqid.to_json())
            for bundle_fqid in bundle_fqids
        )
        num_messages = self.queue_notifications(messages)
        log.info('Successfully queued %i notification(s) for prefix %s of '
                 'source %r', num_messages, prefix, source)

    def contribute(self, action: IndexAction, message: JSON):
        if action is IndexAction.reindex:
            self.remote_reindex_partition(message)
        else:
            catalog = json_str(message['catalog'])
            assert catalog is not None
            delete = action is IndexAction.delete
            bundle_fqid = json_mapping(message['bundle_fqid'])
            bundle_partition = json_mapping(message['bundle_partition'])
            bundle_partition = BundlePartition.from_json(bundle_partition)
            contributions, replicas = self.transform(catalog,
                                                     bundle_fqid,
                                                     bundle_partition,
                                                     delete=delete)
            log.info('Writing %i contributions to index.', len(contributions))
            tallies = self.index_service.contribute(catalog, contributions)
            tallies = [DocumentTally.for_entity(catalog, entity, num_contributions)
                       for entity, num_contributions in tallies.items()]

            if replicas:
                if delete:
                    # FIXME: Replica index does not support deletions
                    #        https://github.com/DataBiosphere/azul/issues/5846
                    log.warning('Deletion of replicas is not supported')
                else:
                    log.info('Writing %i replicas to index.', len(replicas))
                    num_written = self.index_service.replicate(catalog, replicas)
                    log.info('Successfully wrote %i replicas', num_written)
            else:
                log.info('No replicas to write.')

            log.info('Queueing %i entities for aggregating a total of %i contributions.',
                     len(tallies), sum(tally.num_contributions for tally in tallies))
            messages = (tally.to_message() for tally in tallies)
            self.queue_tallies(messages)

    def transform(self,
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
        bundle = self.index_repository_service.fetch_bundle(catalog,
                                                            bundle_fqid)
        results = self.index_service.transform(catalog,
                                               bundle,
                                               bundle_partition,
                                               delete=delete)
        if isinstance(results, list):
            action = IndexAction.delete if delete else IndexAction.add
            for bundle_partition in results:
                assert isinstance(bundle_partition, BundlePartition)
                # There's a good chance that the partition will also fail in
                # the non-retry Lambda function so we'll go straight to retry.
                message = self.index_bundle_message(action,
                                                    catalog,
                                                    bundle_fqid,
                                                    bundle_partition)
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
    num_batched_aggregation_attempts = 3

    def aggregate(self, tallies: list['DocumentTally'], *, retry: bool):
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
                if tally.attempts > self.num_batched_aggregation_attempts:
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

            self.index_service.aggregate(tally_by_entity)

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
            self.queue_tallies(messages, retry=retry)


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

    def consolidate(self, others: list['DocumentTally']) -> Self:
        assert all(self.entity == other.entity for other in others)
        num_contributions = sum((other.num_contributions for other in others),
                                start=self.num_contributions)
        return attrs.evolve(self, num_contributions=num_contributions)
