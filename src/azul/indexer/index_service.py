from collections import (
    Counter,
    defaultdict,
)
from collections.abc import (
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from itertools import (
    groupby,
)
import logging
from operator import (
    attrgetter,
)
from typing import (
    MutableSet,
    Optional,
    TYPE_CHECKING,
    Type,
    Union,
    cast,
)

from elasticsearch import (
    ConflictError,
    ElasticsearchException,
)
from elasticsearch.exceptions import (
    NotFoundError,
    RequestError,
)
from elasticsearch.helpers import (
    streaming_bulk,
)
from more_itertools import (
    first,
    one,
    unzip,
)

from azul import (
    CatalogName,
    cache,
    config,
    freeze,
)
from azul.deployment import (
    aws,
)
from azul.es import (
    ESClientFactory,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
    BundleFQIDJSON,
    BundlePartition,
    BundleUUID,
    SourcedBundleFQIDJSON,
)
from azul.indexer.aggregate import (
    Entities,
)
from azul.indexer.document import (
    Aggregate,
    AggregateCoordinates,
    CataloguedContribution,
    CataloguedEntityReference,
    CataloguedFieldTypes,
    Contribution,
    Document,
    DocumentCoordinates,
    DocumentType,
    EntityID,
    EntityReference,
    EntityType,
    IndexName,
    Replica,
    VersionType,
)
from azul.indexer.document_service import (
    DocumentService,
)
from azul.indexer.transform import (
    Transformer,
)
from azul.logging import (
    silenced_es_logger,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.types import (
    AnyJSON,
    CompositeJSON,
    JSON,
    JSONs,
)

log = logging.getLogger(__name__)

Tallies = Mapping[EntityReference, int]

CataloguedTallies = Mapping[CataloguedEntityReference, int]

MutableCataloguedTallies = dict[CataloguedEntityReference, int]


class IndexExistsAndDiffersException(Exception):
    pass


class IndexService(DocumentService):

    @cache
    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def settings(self, index_name: IndexName) -> JSON:
        index_name.validate()
        aggregate = index_name.doc_type is DocumentType.aggregate
        catalog = index_name.catalog
        assert catalog is not None, catalog
        if config.catalogs[catalog].is_integration_test_catalog:
            # The IT catalogs are far smaller than non-IT catalogs. There is no
            # need for the same degree of concurrency as the non-IT catalogs.
            num_shards = 1
            num_replicas = 0
        else:
            num_nodes = aws.es_instance_count
            num_workers = config.contribution_concurrency(retry=False)

            # Put the sole primary aggregate shard on one node and a replica
            # on all others. The reason for just one primary shard is that
            # aggregate indices are small and don't need to be sharded. Each
            # shard incurs a significant overhead in ES so we want to
            # minimize their number in the absence of overriding concerns
            # like optimization for write concurrency. The reason for putting
            # a replica on all other nodes is that we do want a full copy of
            # each aggregate index on every node so that every node can
            # answer client requests without coordinating with other nodes.
            #
            # Linearly scale the number of contribution shards with the number
            # of contribution writers. There was no notable difference in
            # speed between factors 1 and 1/4 but the memory pressure was
            # unsustainably high with factor 1. In later experiments a factor
            # of 1/8 was determined to be preferential, but I don't recall
            # the details. We neglected to document our process at the time.
            #
            # There is no need to replicate the contribution indices because
            # their durability does not matter to us as much. If a node goes
            # down, we'll just reindex. Since service requests only hit the
            # aggregate indices, we can lose all but one node before
            # customers are affected.
            #
            num_shards = 1 if aggregate else max(num_nodes, num_workers // 8)
            num_replicas = (num_nodes - 1) if aggregate else 0
        return {
            'index': {
                'number_of_shards': num_shards,
                'number_of_replicas': num_replicas,
                'refresh_interval': f'{config.es_refresh_interval}s'
            }
        }

    def index_names(self, catalog: CatalogName) -> list[IndexName]:
        return [
            IndexName.create(catalog=catalog,
                             entity_type=entity_type,
                             doc_type=doc_type)
            for entity_type in self.entity_types(catalog)
            for doc_type in (DocumentType.contribution, DocumentType.aggregate)
        ] + (
            [
                IndexName.create(catalog=catalog,
                                 entity_type='replica',
                                 doc_type=DocumentType.replica)
            ]
            if config.enable_replicas else
            []
        )

    def fetch_bundle(self,
                     catalog: CatalogName,
                     bundle_fqid: SourcedBundleFQIDJSON
                     ) -> Bundle:
        plugin = self.repository_plugin(catalog)
        bundle_fqid = plugin.resolve_bundle(bundle_fqid)
        return plugin.fetch_bundle(bundle_fqid)

    def index(self, catalog: CatalogName, bundle: Bundle) -> None:
        """
        Index the bundle referenced by the given notification into the specified
        catalog. This is an inefficient default implementation. A more efficient
        implementation would transform many bundles, collect their contributions
        and aggregate all affected entities at the end.
        """
        transforms = self.deep_transform(catalog, bundle, delete=False)
        tallies = {}
        for contributions, replicas in transforms:
            tallies.update(self.contribute(catalog, contributions))
            self.replicate(catalog, replicas)
        self.aggregate(tallies)

    def delete(self, catalog: CatalogName, bundle: Bundle) -> None:
        """
        Synchronous form of delete that is currently only used for testing.

        In production code, there is an SQS queue between the calls to
        `contribute()` and `aggregate()`.
        """
        # FIXME: this only works if the bundle version is not being indexed
        #        concurrently. The fix could be to optimistically lock on the
        #        aggregate version (https://github.com/DataBiosphere/azul/issues/611)
        transforms = self.deep_transform(catalog, bundle, delete=True)
        tallies = {}
        for contributions, replicas in transforms:
            # FIXME: these are all modified contributions, not new ones. This also
            #        happens when we reindex without deleting the indices first. The
            #        tallies refer to number of updated or added contributions but
            #        we treat them as if they are all new when we estimate the
            #        number of contributions per bundle.
            # https://github.com/DataBiosphere/azul/issues/610
            tallies.update(self.contribute(catalog, contributions))
            self.replicate(catalog, replicas)
        self.aggregate(tallies)

    def deep_transform(self,
                       catalog: CatalogName,
                       bundle: Bundle,
                       partition: BundlePartition = BundlePartition.root,
                       *,
                       delete: bool
                       ) -> Iterator[tuple[list[Contribution], list[Replica]]]:
        """
        Recursively transform the given partition of the specified bundle and
        any divisions of that partition. This should be used by synchronous
        indexing. The default asynchronous indexing would defer divisions of the
        starting partition and schedule a follow-on notification for each of the
        divisions.
        """
        results = self.transform(catalog, bundle, partition, delete=delete)
        result = first(results, None)
        if isinstance(result, BundlePartition):
            for sub_partition in results:
                yield from self.deep_transform(catalog, bundle, sub_partition, delete=delete)
        elif isinstance(results, tuple):
            yield results
        elif result is None:
            yield [], []
        else:
            assert False, type(result)

    def transform(self,
                  catalog: CatalogName,
                  bundle: Bundle,
                  partition: BundlePartition = BundlePartition.root,
                  *,
                  delete: bool,
                  ) -> Union[list[BundlePartition], tuple[list[Contribution], list[Replica]]]:
        """
        Return a list of contributions and a list of replicas for the entities
        in the given partition of the specified bundle, or a set of divisions of
        the given partition if it contains too many entities.

        :param catalog: the name of the catalog to contribute to

        :param bundle: the bundle to transform

        :param partition: the bundle partition to transform

        :param delete: True, if the bundle should be removed from the catalog.
                       The resulting contributions will be deletions instead
                       of additions.
        """
        plugin = self.metadata_plugin(catalog)
        bundle.reject_joiner(catalog)
        transformers = plugin.transformers(bundle, delete=delete)
        log.info('Estimating size of partition %s of bundle %s, version %s.',
                 partition, bundle.uuid, bundle.version)
        num_entities = sum(transformer.estimate(partition) for transformer in transformers)
        num_divisions = partition.divisions(num_entities)
        if num_divisions > 1:
            log.info('Dividing partition %s of bundle %s, version %s, '
                     'with %i entities into %i sub-partitions.',
                     partition, bundle.uuid, bundle.version, num_entities, num_divisions)
            return partition.divide(num_divisions)
        else:
            log.info('Transforming %i entities in partition %s of bundle %s, version %s.',
                     num_entities, partition, bundle.uuid, bundle.version)
            contributions = []
            replicas = []
            for transformer in transformers:
                # The cast is necessary because unzip()'s type stub doesn't
                # support heterogeneous tuples.
                transforms = cast(
                    tuple[Iterable[Optional[Contribution]], Iterable[Optional[Replica]]],
                    unzip(transformer.transform(partition))
                )
                if transforms:
                    contributions_part, replicas_part = transforms
                    contributions.extend(filter(None, contributions_part))
                    replicas.extend(filter(None, replicas_part))
            return contributions, replicas

    def create_indices(self, catalog: CatalogName):
        es_client = ESClientFactory.get()
        for index_name in self.index_names(catalog):
            while True:
                settings = self.settings(index_name)
                mappings = self.metadata_plugin(catalog).mapping(index_name)
                try:
                    with silenced_es_logger():
                        index = es_client.indices.get(index=str(index_name))
                except NotFoundError:
                    try:
                        es_client.indices.create(index=str(index_name),
                                                 body=dict(settings=settings,
                                                           mappings=mappings))
                    except RequestError as e:
                        if e.error == 'resource_already_exists_exception':
                            log.info('Another party concurrently created index %s (%r), retrying.',
                                     index_name, index_name)
                        else:
                            raise
                else:
                    self._check_index(settings=settings,
                                      mappings=mappings,
                                      index=index[str(index_name)])
                    break

    def _check_index(self, *, settings: JSON, mappings: JSON, index: JSON):

        def stringify(value: AnyJSON) -> AnyJSON:
            return (
                {k: stringify(v) for k, v in value.items()}
                if isinstance(value, dict) else
                [stringify(v) for v in value]
                if isinstance(value, list) else
                str(value)
            )

        def setify(value: CompositeJSON
                   ) -> Union[set[tuple[str, AnyJSON]], set[AnyJSON]]:
            value = freeze(value)
            return set(
                value.items()
                if isinstance(value, Mapping) else
                value
            )

        def flatten(value: JSON, *path) -> Iterable[tuple[tuple[str, ...], AnyJSON]]:
            for k, v in value.items():
                if isinstance(v, Mapping):
                    yield from flatten(v, *path, k)
                else:
                    yield (*path, k), v

        # Compare the index settings
        expected, actual = (
            setify(dict(flatten(stringify(s))))
            for s in [settings, index['settings']]
        )
        if not expected <= actual:
            raise IndexExistsAndDiffersException('settings', settings, index['settings'])

        # Compare the static field mapping
        key = 'properties'
        expected, actual = (
            setify(dict(flatten(m.get(key, {}))))
            for m in [mappings, index['mappings']]
        )
        if not expected <= actual:
            raise IndexExistsAndDiffersException(key, mappings, index['mappings'])

        # Compare the dynamic field mapping
        key = 'dynamic_templates'
        expected, actual = (
            setify(m.get(key, []))
            for m in [mappings, index['mappings']]
        )
        if not expected == actual:
            raise IndexExistsAndDiffersException(key, mappings, index['mappings'])

        # Compare the rest of the mapping
        expected, actual = (
            setify(dict(flatten({
                k: v
                for k, v in m.items()
                if k not in {'properties', 'dynamic_templates'}
            })))
            for m in [mappings, index['mappings']]
        )
        if not expected <= actual:
            raise IndexExistsAndDiffersException('mappings', mappings, index['mappings'])

    def delete_indices(self, catalog: CatalogName):
        es_client = ESClientFactory.get()
        for index_name in self.index_names(catalog):
            if es_client.indices.exists(index_name):
                es_client.indices.delete(index=index_name)

    def contribute(self,
                   catalog: CatalogName,
                   contributions: list[Contribution]
                   ) -> CataloguedTallies:
        """
        Write the given entity contributions to the index and return tallies, a
        dictionary tracking the number of contributions made to each entity.

        Tallies for overwritten documents are not counted. This means a tally
        with a count of 0 may exist. This is ok. See description of aggregate().
        """
        tallies = Counter()
        writer = self._create_writer(catalog)
        while contributions:
            writer.write(contributions)
            retry_contributions = []
            for c in contributions:
                if c.coordinates in writer.retries:
                    retry_contributions.append(c)
                else:
                    entity = CataloguedEntityReference.for_entity(catalog, c.coordinates.entity)
                    # Don't count overwrites, but ensure entry exists
                    was_overwrite = c.version_type is VersionType.none
                    tallies[entity] += 0 if was_overwrite else 1
            contributions = retry_contributions
        writer.raise_on_errors()
        return tallies

    def aggregate(self, tallies: CataloguedTallies):
        """
        Read all contributions to the entities listed in the given tallies from
        the index, aggregate the contributions into one aggregate per entity and
        write the resulting aggregates to the index.

        Normally there is a one-to-one correspondence between number of
        contributions for an entity and the value for a tally, however tallies
        are not counted for updates. This means, in the case of a duplicate
        notification or writing over an already populated index, it's possible
        to receive a tally with a value of 0. We still need to aggregate (if the
        indexed format changed for example). Tallies are a lower bound for the
        number of contributions in the index for a given entity.

        Also note that the input tallies can refer to entities from different
        catalogs.
        """
        # Use catalog specified in each tally
        writer = self._create_writer(catalog=None)
        while True:
            # Read the aggregates
            old_aggregates = self._read_aggregates(tallies)
            total_tallies: MutableCataloguedTallies = Counter(tallies)
            total_tallies.update({
                old_aggregate.coordinates.entity: old_aggregate.num_contributions
                for old_aggregate in old_aggregates.values()
            })

            # Read all contributions
            contributions = self._read_contributions(total_tallies)
            actual_tallies = Counter(contribution.coordinates.entity
                                     for contribution in contributions)
            if tallies.keys() != actual_tallies.keys():
                message = 'Could not find all expected contributions.'
                args = (tallies, actual_tallies) if config.debug else ()
                raise EventualConsistencyException(message, *args)
            assert all(tallies[entity] <= actual_tally
                       for entity, actual_tally in actual_tallies.items())

            # Combine the contributions into new aggregates, one per entity
            new_aggregates = self._aggregate(contributions)

            # Remove old aggregates (leaving over only deletions) while
            # propagating the expected document version to the corresponding new
            # aggregate
            for new_aggregate in new_aggregates:
                old_aggregate = old_aggregates.pop(new_aggregate.coordinates.entity, None)
                new_aggregate.version = None if old_aggregate is None else old_aggregate.version

            # Empty out the left-over, deleted aggregates
            for old_aggregate in old_aggregates.values():
                old_aggregate.contents = {}
                new_aggregates.append(old_aggregate)

            # Write new aggregates
            writer.write(new_aggregates)

            # Retry writes if necessary
            if writer.retries:
                tallies: CataloguedTallies = {
                    aggregate.coordinates.entity: tallies[aggregate.coordinates.entity]
                    for aggregate in new_aggregates
                    if aggregate.coordinates in writer.retries
                }
            else:
                break
        writer.raise_on_errors()

    def replicate(self,
                  catalog: CatalogName,
                  replicas: list[Replica]
                  ) -> tuple[int, int]:
        writer = self._create_writer(catalog)
        num_replicas = len(replicas)
        num_written, num_present = 0, 0
        while replicas:
            writer.write(replicas)
            retry_replicas = []
            for r in replicas:
                if r.coordinates in writer.retries:
                    conflicts = writer.conflicts[r.coordinates]
                    if conflicts == 0:
                        retry_replicas.append(r)
                    elif conflicts == 1:
                        # FIXME: Track replica hub IDs
                        #        https://github.com/DataBiosphere/azul/issues/5360
                        writer.conflicts.pop(r.coordinates)
                        num_present += 1
                    else:
                        assert False, (conflicts, r.coordinates)
                else:
                    num_written += 1
            replicas = retry_replicas

        writer.raise_on_errors()
        assert num_written + num_present == num_replicas, (
            num_written, num_present, num_replicas
        )
        return num_written, num_present

    def _read_aggregates(self,
                         entities: CataloguedTallies
                         ) -> dict[CataloguedEntityReference, Aggregate]:
        coordinates = [
            AggregateCoordinates(entity=entity)
            for entity in entities
        ]
        request = {
            'docs': [
                {
                    '_index': coordinate.index_name,
                    '_id': coordinate.document_id
                }
                for coordinate in coordinates
            ]
        }
        catalogs = {coordinate.entity.catalog for coordinate in coordinates}
        mandatory_source_fields = set()
        for catalog in catalogs:
            aggregate_cls = self.aggregate_class(catalog)
            mandatory_source_fields.update(aggregate_cls.mandatory_source_fields())
        response = ESClientFactory.get().mget(body=request,
                                              _source_includes=list(mandatory_source_fields))

        def aggregates():
            for doc in response['docs']:
                try:
                    found = doc['found']
                except KeyError:
                    raise RuntimeError('Malformed document', doc)
                else:
                    if found:
                        coordinate = DocumentCoordinates.from_hit(doc)
                        aggregate_cls = self.aggregate_class(coordinate.entity.catalog)
                        aggregate = aggregate_cls.from_index(self.catalogued_field_types(),
                                                             doc,
                                                             coordinates=coordinate)
                        yield aggregate

        return {a.coordinates.entity: a for a in aggregates()}

    def _read_contributions(self,
                            tallies: CataloguedTallies
                            ) -> list[CataloguedContribution]:
        es_client = ESClientFactory.get()

        entity_ids_by_index: dict[str, MutableSet[str]] = defaultdict(set)
        for entity in tallies.keys():
            index = str(IndexName.create(catalog=entity.catalog,
                                         entity_type=entity.entity_type,
                                         doc_type=DocumentType.contribution))
            entity_ids_by_index[index].add(entity.entity_id)

        query = {
            'bool': {
                'should': [
                    {
                        'bool': {
                            'must': [
                                {
                                    'term': {
                                        '_index': index
                                    }
                                },
                                {
                                    'terms': {
                                        'entity_id.keyword': list(entity_ids)
                                    }
                                }
                            ]
                        }
                    } for index, entity_ids in entity_ids_by_index.items()
                ]
            }
        }

        index = sorted(list(entity_ids_by_index.keys()))
        num_contributions = sum(tallies.values())
        log.info('Reading %i expected contribution(s)', num_contributions)

        def pages() -> Iterable[JSONs]:
            body = dict(query=query)
            while True:
                response = es_client.search(index=index,
                                            sort=['_index', 'document_id.keyword'],
                                            body=body,
                                            size=config.contribution_page_size,
                                            track_total_hits=False,
                                            seq_no_primary_term=Contribution.needs_seq_no_primary_term)
                hits = response['hits']['hits']
                log.debug('Read a page with %i contribution(s)', len(hits))
                if hits:
                    yield hits
                    body['search_after'] = hits[-1]['sort']
                else:
                    break

        contributions = [
            Contribution.from_index(self.catalogued_field_types(), hit)
            for hits in pages()
            for hit in hits
        ]

        log.info('Read %i contribution(s)', len(contributions))
        if log.isEnabledFor(logging.DEBUG):
            entity_ref = attrgetter('entity')
            log.debug(
                'Number of contributions read, by entity: %r',
                {
                    f'{entity.entity_type}/{entity.entity_id}': sum(1 for _ in contribution_group)
                    for entity, contribution_group in groupby(sorted(contributions, key=entity_ref), key=entity_ref)
                }
            )
        return contributions

    def _aggregate(self,
                   contributions: list[CataloguedContribution]
                   ) -> list[Aggregate]:
        # Group contributions by entity and bundle UUID
        contributions_by_bundle: Mapping[
            tuple[CataloguedEntityReference, BundleUUID],
            list[CataloguedContribution]
        ] = defaultdict(list)
        tallies: MutableCataloguedTallies = Counter()
        for contribution in contributions:
            entity = contribution.coordinates.entity
            bundle_uuid = contribution.coordinates.bundle.uuid
            contributions_by_bundle[entity, bundle_uuid].append(contribution)
            # Track the raw, unfiltered number of contributions per entity.
            assert isinstance(contribution.coordinates.entity, CataloguedEntityReference)
            tallies[contribution.coordinates.entity] += 1

        # For each entity and bundle, find the most recent contribution that is
        # not a deletion
        contributions_by_entity: dict[
            CataloguedEntityReference, list[CataloguedContribution]] = defaultdict(list)
        for (entity, bundle_uuid), contributions in contributions_by_bundle.items():
            contributions = sorted(contributions,
                                   key=attrgetter('coordinates.bundle.version', 'coordinates.deleted'),
                                   reverse=True)
            for bundle_version, group in groupby(contributions, key=attrgetter('coordinates.bundle.version')):
                contribution: Contribution = next(group)
                if not contribution.coordinates.deleted:
                    assert bundle_uuid == contribution.coordinates.bundle.uuid
                    assert bundle_version == contribution.coordinates.bundle.version
                    assert entity == contribution.coordinates.entity
                    contributions_by_entity[entity].append(contribution)
                    break
        log.info('Selected %i contribution(s) to be aggregated.',
                 sum(len(contributions) for contributions in contributions_by_entity.values()))
        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                'Number of contributions selected for aggregation, by entity: %r',
                {
                    f'{entity.entity_type}/{entity.entity_id}': len(contributions)
                    for entity, contributions in sorted(contributions_by_entity.items())
                }
            )

        # Create lookup for transformer by entity type
        transformers: dict[tuple[CatalogName, str], Type[Transformer]] = {
            (catalog, transformer_cls.entity_type()): transformer_cls
            for catalog in config.catalogs
            for transformer_cls in self.transformer_types(catalog)
        }

        # Aggregate contributions for the same entity
        aggregates = []
        for entity, contributions in contributions_by_entity.items():
            transformer = transformers[entity.catalog, entity.entity_type]
            contents = self._aggregate_entity(transformer, contributions)
            bundles = [
                BundleFQIDJSON(uuid=c.coordinates.bundle.uuid,
                               version=c.coordinates.bundle.version)
                for c in contributions
            ]
            # FIXME: Replace hard coded limit with a config property
            #       https://github.com/DataBiosphere/azul/issues/3725
            max_bundles = 100
            if len(bundles) > max_bundles:
                log.warning('Only aggregating %i out of %i bundles for outer entity %r',
                            max_bundles, len(bundles), entity)
            bundles = bundles[:max_bundles]
            sources = set(c.source for c in contributions)
            aggregate_cls = self.aggregate_class(entity.catalog)
            if TYPE_CHECKING:  # work around https://youtrack.jetbrains.com/issue/PY-44728
                aggregate_cls = Aggregate
            aggregate = aggregate_cls(coordinates=AggregateCoordinates(entity=entity),
                                      version=None,
                                      sources=sources,
                                      contents=contents,
                                      bundles=bundles,
                                      num_contributions=tallies[entity])
            aggregates.append(aggregate)

        return aggregates

    def _aggregate_entity(self,
                          transformer: Type[Transformer],
                          contributions: list[Contribution]
                          ) -> JSON:
        contents = self._reconcile(transformer, contributions)
        aggregate_contents = {}
        inner_entity_types = transformer.inner_entity_types()
        inner_entity_counts = []
        for entity_type, entities in contents.items():
            num_entities = len(entities)
            if entity_type in inner_entity_types:
                assert num_entities <= 1
                inner_entity_counts.append(num_entities)
            else:
                aggregator = transformer.aggregator(entity_type)
                if aggregator is not None:
                    entities = aggregator.aggregate(entities)
            aggregate_contents[entity_type] = entities
        if inner_entity_counts:
            assert sum(inner_entity_counts) > 0
        return aggregate_contents

    def _reconcile(self,
                   transformer: Type[Transformer],
                   contributions: Sequence[Contribution],
                   ) -> Mapping[EntityType, Entities]:
        """
        Given all the contributions to a certain outer entity, reconcile
        potentially different copies of the same inner entity in those
        contributions.
        """
        if len(contributions) == 1:
            return one(contributions).contents
        else:
            result: dict[EntityType, dict[EntityID, tuple[JSON, BundleFQID]]]
            result = defaultdict(dict)
            for contribution in contributions:
                that_bundle = contribution.coordinates.bundle
                for entity_type, those_entities in contribution.contents.items():
                    these_entities = result[entity_type]
                    for that_entity in those_entities:
                        entity_id = transformer.inner_entity_id(entity_type, that_entity)
                        this = these_entities.get(entity_id, (None, None))
                        this_entity, this_bundle = this
                        that = (that_entity, that_bundle)
                        if this_entity is None:
                            these_entities[entity_id] = that
                        else:
                            that = transformer.reconcile_inner_entities(entity_type, this=this, that=that)
                            if this != that:
                                these_entities[entity_id] = that
            return {
                entity_type: [entity for entity, _ in entities.values()]
                for entity_type, entities in result.items()
            }

    def _create_writer(self, catalog: Optional[CatalogName]) -> 'IndexWriter':
        # We allow one conflict retry in the case of duplicate notifications and
        # switch from 'add' to 'update'. After that, there should be no
        # conflicts because we use an SQS FIFO message group per entity. For
        # other errors we use SQS message redelivery to take care of the
        # retries.
        return IndexWriter(catalog,
                           self.catalogued_field_types(),
                           refresh=False,
                           conflict_retry_limit=1,
                           error_retry_limit=0)


class IndexWriter:

    def __init__(self,
                 catalog: Optional[CatalogName],
                 field_types: CataloguedFieldTypes,
                 refresh: Union[bool, str],
                 conflict_retry_limit: int,
                 error_retry_limit: int) -> None:
        """
        :param field_types: A mapping of field paths to field type

        :param refresh: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/docs-refresh.html

        :param conflict_retry_limit: The maximum number of retries (the second
                                     attempt is the first retry) on version
                                     conflicts. Specify 0 for no retries or None
                                     for unlimited retries.

        :param error_retry_limit: The maximum number of retries (the second
                                  attempt is the first retry) on other errors.
                                  Specify 0 for no retries or None for
                                  unlimited retries.
        """
        super().__init__()
        self.catalog = catalog
        self.field_types = field_types
        self.refresh = refresh
        self.conflict_retry_limit = conflict_retry_limit
        self.error_retry_limit = error_retry_limit
        self.es_client = ESClientFactory.get()
        self.errors: dict[DocumentCoordinates, int] = defaultdict(int)
        self.conflicts: dict[DocumentCoordinates, int] = defaultdict(int)
        self.retries: Optional[MutableSet[DocumentCoordinates]] = None

    bulk_threshold = 32

    def write(self, documents: list[Document]):
        """
        Make an attempt to write the documents into the index, updating local
        state with failures and conflicts

        :param documents: Documents to index
        """
        self.retries = set()
        if len(documents) < self.bulk_threshold:
            self._write_individually(documents)
        else:
            self._write_bulk(documents)

    def _write_individually(self, documents: Iterable[Document]):
        log.info('Writing documents individually')
        for doc in documents:
            if isinstance(doc, Replica):
                assert doc.version_type is VersionType.create_only, doc
            try:
                method = self.es_client.delete if doc.delete else self.es_client.index
                method(refresh=self.refresh, **doc.to_index(self.catalog, self.field_types))
            except ConflictError as e:
                self._on_conflict(doc, e)
            except ElasticsearchException as e:
                self._on_error(doc, e)
            else:
                self._on_success(doc)

    def _write_bulk(self, documents: Iterable[Document]):
        # FIXME: document this quirk
        documents: dict[DocumentCoordinates, Document] = {
            doc.coordinates.with_catalog(self.catalog): doc
            for doc in documents
        } if self.catalog is not None else {
            doc.coordinates: doc
            for doc in documents
        }
        actions = [
            doc.to_index(self.catalog, self.field_types, bulk=True)
            for doc in documents.values()
        ]
        log.info('Writing documents using streaming_bulk().')
        # We cannot use parallel_bulk() for 1024+ actions because Lambda doesn't
        # support shared memory. See the issue below for details.
        #
        # https://github.com/DataBiosphere/azul/issues/3200
        #
        # Another caveat to keep in mind is that streaming_bulk() may still
        # exceed the maximum request size if one or more actions exceed it.
        # There is no way to split a single action and hence a single document
        # into multiple requests.
        #
        response = streaming_bulk(client=self.es_client,
                                  actions=actions,
                                  refresh=self.refresh,
                                  raise_on_error=False,
                                  max_chunk_bytes=config.max_chunk_size)
        for success, info in response:
            op_type, info = one(info.items())
            assert op_type in ('index', 'create', 'delete')
            coordinates = DocumentCoordinates.from_hit(info)
            doc = documents[coordinates]
            if success:
                self._on_success(doc)
            else:
                if info['status'] == 409:
                    self._on_conflict(doc, info)
                else:
                    self._on_error(doc, info)

    def _on_success(self, doc: Document):
        coordinates = doc.coordinates
        self.conflicts.pop(coordinates, None)
        self.errors.pop(coordinates, None)
        if isinstance(doc, Aggregate):
            log.debug('Successfully wrote %s with %i contribution(s).',
                      coordinates, doc.num_contributions)
        else:
            log.debug('Successfully wrote %s.', coordinates)

    def _on_error(self, doc: Document, e: Union[Exception, JSON]):
        self.errors[doc.coordinates] += 1
        if self.error_retry_limit is None or self.errors[doc.coordinates] <= self.error_retry_limit:
            action = 'retrying'
            self.retries.add(doc.coordinates)
        else:
            action = 'giving up'
        log.warning('There was a general error with document %r: %r. Total # of errors: %i, %s.',
                    doc.coordinates, e, self.errors[doc.coordinates], action,
                    exc_info=isinstance(e, Exception))

    def _on_conflict(self, doc: Document, e: Union[Exception, JSON]):
        self.conflicts[doc.coordinates] += 1
        self.errors.pop(doc.coordinates, None)  # a conflict resets the error count
        if self.conflict_retry_limit is None or self.conflicts[doc.coordinates] <= self.conflict_retry_limit:
            action = 'retrying'
            self.retries.add(doc.coordinates)
        else:
            action = 'giving up'
        if doc.version_type is VersionType.create_only:
            log.warning('Document %r exists. Retrying with overwrite.', doc.coordinates)
            # Try again but allow overwriting
            doc.version_type = VersionType.none
        else:
            log.warning('There was a conflict with document %r: %r. Total # of errors: %i, %s.',
                        doc.coordinates, e, self.conflicts[doc.coordinates], action)

    def raise_on_errors(self):
        if self.errors or self.conflicts:
            log.warning('Failures: %r', self.errors)
            log.warning('Conflicts: %r', self.conflicts)
            raise RuntimeError('Failed to index documents. Failures: %i, conflicts: %i.' %
                               (len(self.errors), len(self.conflicts)))


class EventualConsistencyException(RuntimeError):
    pass
