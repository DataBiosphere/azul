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
    Any,
    TYPE_CHECKING,
    cast,
    overload,
)

from more_itertools import (
    first,
    one,
)
from opensearchpy import (
    ConflictError,
    OpenSearchException,
)
from opensearchpy.exceptions import (
    NotFoundError,
    RequestError,
)
from opensearchpy.helpers import (
    streaming_bulk,
)

from azul import (
    CatalogName,
    config,
)
from azul.deployment import (
    aws,
)
from azul.field_type import (
    CataloguedFieldTypes,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
    BundlePartition,
    BundleUUID,
)
from azul.indexer.document import (
    Aggregate,
    AggregateCoordinates,
    CataloguedContribution,
    CataloguedEntityReference,
    Contribution,
    Document,
    DocumentCoordinates,
    DocumentType,
    EntityID,
    EntityReference,
    EntityType,
    IndexName,
    OpType,
    Replica,
    ReplicaCoordinates,
)
from azul.indexer.document_service import (
    DocumentService,
)
from azul.indexer.transform import (
    Transformer,
)
from azul.lib import (
    R,
)
from azul.lib.json_freeze import (
    freeze,
)
from azul.lib.types import (
    AnyJSON,
    JSON,
    JSONArray,
    JSONs,
    PrimitiveJSON,
    json_element_mappings,
    json_items_are_sequences_of_mappings,
    json_mapping,
    json_sequence,
)
from azul.logging import (
    silenced_opensearch_logger,
)
from azul.opensearch import (
    OpenSearchClientFactory,
)

log = logging.getLogger(__name__)

type Tallies = Mapping[EntityReference, int]

type CataloguedTallies = Mapping[CataloguedEntityReference, int]

type MutableCataloguedTallies = dict[CataloguedEntityReference, int]


class IndexExistsAndDiffersException(Exception):
    pass


class IndexService(DocumentService):

    def _settings(self, index_name: IndexName) -> JSON:
        index_name.validate()
        aggregate = index_name.doc_type is DocumentType.aggregate
        # There is a terminology collision between OpenSearch's concept of an
        # index replica, and our Azul-specific concept of an entity/document
        # replica.
        replica = index_name.doc_type is DocumentType.replica
        catalog = index_name.catalog
        assert catalog is not None, catalog
        if (
            config.catalogs[catalog].is_integration_test_catalog
            or config.deployment.is_unit_test
        ):
            # The test catalogs are far smaller than non-test catalogs. There is
            # no need for the same degree of concurrency as the non-test catalogs.
            # Fixing the number of shards also helps keep the order of documents
            # in the index deterministic, which helps with writing unit tests,
            # e.g. the verbatim PFB manifest tests.
            num_shards = 1
            num_replicas = 0
        else:
            num_nodes = aws.opensearch_instance_count
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
            num_replicas = (num_nodes - 1) if aggregate or replica else 0
        return {
            'index': {
                'number_of_shards': num_shards,
                'number_of_replicas': num_replicas,
                'refresh_interval': f'{config.opensearch_refresh_interval}s'
            }
        }

    def index_names(self, catalog: CatalogName) -> list[IndexName]:
        return [
            IndexName.create(catalog=catalog,
                             qualifier=entity_type,
                             doc_type=doc_type)
            for entity_type in self.entity_types(catalog)
            for doc_type in (DocumentType.contribution, DocumentType.aggregate)
        ] + (
            [
                IndexName.create(catalog=catalog,
                                 qualifier=ReplicaCoordinates.index_qualifier,
                                 doc_type=DocumentType.replica)
            ]
            if config.enable_replicas else
            []
        )

    def index(self, catalog: CatalogName, bundle: Bundle) -> None:
        """
        Index the bundle referenced by the given notification into the specified
        catalog. This is an inefficient default implementation. A more efficient
        implementation would transform many bundles, collect their contributions
        and aggregate all affected entities at the end.
        """
        transforms = self._deep_transform(catalog, bundle, delete=False)
        tallies: MutableCataloguedTallies = {}
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
        transforms = self._deep_transform(catalog, bundle, delete=True)
        tallies: MutableCataloguedTallies = {}
        for contributions, replicas in transforms:
            # FIXME: these are all modified contributions, not new ones. This also
            #        happens when we reindex without deleting the indices first. The
            #        tallies refer to number of updated or added contributions but
            #        we treat them as if they are all new when we estimate the
            #        number of contributions per bundle.
            # https://github.com/DataBiosphere/azul/issues/610
            tallies.update(self.contribute(catalog, contributions))
        # FIXME: Replica index does not support deletions
        #        https://github.com/DataBiosphere/azul/issues/5846
        self.aggregate(tallies)

    def _deep_transform(self,
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
                assert isinstance(sub_partition, BundlePartition)
                yield from self._deep_transform(catalog, bundle, sub_partition, delete=delete)
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
                  ) -> list[BundlePartition] | tuple[list[Contribution], list[Replica]]:
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
        bundle.reject_joiner()
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
            replicas_by_coords: dict[ReplicaCoordinates, Replica] = {}
            for transformer in transformers:
                for document in transformer.transform(partition):
                    if isinstance(document, Contribution):
                        contributions.append(document)
                    elif isinstance(document, Replica):
                        try:
                            dup = replicas_by_coords[document.coordinates]
                        except KeyError:
                            replicas_by_coords[document.coordinates] = document
                        else:
                            dup.hub_ids.extend(document.hub_ids)
                    else:
                        assert False, document
            return contributions, list(replicas_by_coords.values())

    def create_indices(self, catalog: CatalogName):
        opensearch = OpenSearchClientFactory.get()
        for index_name in self.index_names(catalog):
            while True:
                settings = self._settings(index_name)
                mappings = self.metadata_plugin(catalog).mapping(index_name)
                try:
                    with silenced_opensearch_logger():
                        index = opensearch.indices.get(index=str(index_name))
                except NotFoundError:
                    try:
                        opensearch.indices.create(index=str(index_name),
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

        @overload
        def stringify(value: PrimitiveJSON) -> str:
            ...

        @overload
        def stringify(value: JSON) -> JSON:
            ...

        @overload
        def stringify(value: JSONArray) -> JSONArray:
            ...

        def stringify(value):
            return (
                {k: stringify(v) for k, v in value.items()}
                if isinstance(value, dict) else
                [stringify(v) for v in value]
                if isinstance(value, list) else
                str(value)
            )

        def setify_mapping[K](value: Mapping[K, AnyJSON]) -> set[tuple[K, AnyJSON]]:
            return set((k, freeze(v)) for k, v in value.items())

        def setify_sequence(value: JSONArray) -> set[AnyJSON]:
            return set(json_sequence(freeze(value)))

        def flatten(value: JSON, *path: str) -> Iterable[tuple[tuple[str, ...], AnyJSON]]:
            for k, v in value.items():
                if isinstance(v, Mapping):
                    yield from flatten(v, *path, k)
                else:
                    yield (*path, k), v

        # Compare the index settings
        expected, actual = (
            setify_mapping(dict(flatten(stringify(s))))
            for s in [settings, json_mapping(index['settings'])]
        )
        if not expected <= actual:
            raise IndexExistsAndDiffersException('settings', settings, index['settings'])

        # Compare the static field mapping
        key = 'properties'
        expected, actual = (
            setify_mapping(dict(flatten(json_mapping(m.get(key, {})))))
            for m in [mappings, json_mapping(index['mappings'])]
        )
        if not expected <= actual:
            raise IndexExistsAndDiffersException(key, mappings, index['mappings'])

        # Compare the dynamic field mapping
        key = 'dynamic_templates'
        expected, actual = (
            setify_sequence(json_sequence(m.get(key, [])))
            for m in [mappings, json_mapping(index['mappings'])]
        )
        if not expected == actual:
            raise IndexExistsAndDiffersException(key, mappings, index['mappings'])

        # Compare the rest of the mapping
        expected, actual = (
            setify_mapping(dict(flatten({
                k: v
                for k, v in m.items()
                if k not in {'properties', 'dynamic_templates'}
            })))
            for m in [mappings, json_mapping(index['mappings'])]
        )
        if not expected <= actual:
            raise IndexExistsAndDiffersException('mappings', mappings, index['mappings'])

    def delete_indices(self, catalog: CatalogName):
        opensearch = OpenSearchClientFactory.get()
        for index_name in self.index_names(catalog):
            if opensearch.indices.exists(index=str(index_name)):
                opensearch.indices.delete(index=str(index_name))

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
        tallies: MutableCataloguedTallies = Counter()
        writer = self._create_writer(DocumentType.contribution, catalog)
        while contributions:
            writer.write(contributions)
            retry_contributions = []
            for c in contributions:
                if c.coordinates in writer.retries:
                    retry_contributions.append(c)
                else:
                    entity = CataloguedEntityReference.for_entity(catalog, c.coordinates.entity)
                    # Don't count overwrites, but ensure entry exists
                    was_overwrite = c.op_type is OpType.index
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
        # Attempting to filter by an empty array of coordinates while reading
        # the aggregates will fail with a 400 error from OpenSearch. This
        # happens when indexing replica bundles for AnVIL, since they emit no
        # contributions.
        if not tallies:
            return
        # Use catalog specified in each tally
        writer = self._create_writer(DocumentType.aggregate, catalog=None)
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

            for aggregate in new_aggregates:
                assert len(aggregate.sources) == 1, R(
                    'Entity has an invalid number of sources',
                    aggregate.entity,
                    aggregate.sources
                )

            # Write new aggregates
            writer.write(new_aggregates)

            # Retry writes if necessary
            if writer.retries:
                tallies = {
                    aggregate.coordinates.entity: tallies[aggregate.coordinates.entity]
                    for aggregate in new_aggregates
                    if aggregate.coordinates in writer.retries
                }
            else:
                break
        writer.raise_on_errors()

    def replicate(self, catalog: CatalogName, replicas: list[Replica]) -> int:
        writer = self._create_writer(DocumentType.replica, catalog)
        num_replicas = len(replicas)
        num_written = 0
        while replicas:
            writer.write(replicas)
            retry_replicas = []
            for r in replicas:
                if r.coordinates in writer.retries:
                    retry_replicas.append(r)
                else:
                    num_written += 1
            replicas = retry_replicas

        writer.raise_on_errors()
        assert num_written == num_replicas, (num_written, num_replicas)
        return num_written

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
        opensearch = OpenSearchClientFactory.get()
        response = opensearch.mget(body=request,
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
        opensearch = OpenSearchClientFactory.get()

        entity_ids_by_index: dict[str, set[str]] = defaultdict(set)
        for entity in tallies.keys():
            index = str(IndexName.create(catalog=entity.catalog,
                                         qualifier=entity.entity_type,
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

        indices = sorted(entity_ids_by_index.keys())
        num_contributions = sum(tallies.values())
        log.info('Reading %i expected contribution(s)', num_contributions)

        def pages() -> Iterable[JSONs]:
            body = dict(query=query)
            while True:
                response = opensearch.search(index=indices,
                                             sort=['_index', 'document_id.keyword'],
                                             body=body,
                                             size=config.contribution_page_size,
                                             track_total_hits=False,
                                             seq_no_primary_term=True)
                hits = response['hits']['hits']
                log.debug('Read a page with %i contribution(s)', len(hits))
                if hits:
                    yield hits
                    body['search_after'] = hits[-1]['sort']
                else:
                    break

        contributions: list[CataloguedContribution] = [
            Contribution.from_index(self.catalogued_field_types(), hit)
            for hits in pages()
            for hit in hits
        ]

        log.info('Read %i contribution(s)', len(contributions))
        if log.isEnabledFor(logging.DEBUG):
            entity_ref = attrgetter('entity')
            contributions_by_entity = cast(
                Iterator[tuple[EntityReference, Iterator[Contribution]]],
                groupby(sorted(contributions, key=entity_ref), key=entity_ref)
            )
            log.debug(
                'Number of contributions read, by entity: %r',
                {
                    f'{entity.entity_type}/{entity.entity_id}': sum(1 for _ in contribution_group)
                    for entity, contribution_group in contributions_by_entity
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
            CataloguedEntityReference,
            list[CataloguedContribution]
        ] = defaultdict(list)
        for (entity, bundle_uuid), contributions in contributions_by_bundle.items():
            contributions = sorted(contributions,
                                   key=attrgetter('coordinates.bundle.version', 'coordinates.deleted'),
                                   reverse=True)
            for bundle_version, group in groupby(contributions, key=attrgetter('coordinates.bundle.version')):
                contribution = next(group)
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
        transformers: dict[tuple[CatalogName, str], type[Transformer]] = {
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
                BundleFQID(uuid=c.coordinates.bundle.uuid,
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
                          transformer: type[Transformer],
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
                   transformer: type[Transformer],
                   contributions: Sequence[Contribution],
                   ) -> Mapping[EntityType, JSONs]:
        """
        Given all the contributions to a certain outer entity, reconcile
        potentially different copies of the same inner entity in those
        contributions.
        """
        if len(contributions) == 1:
            single_result = contributions[0].contents
            assert json_items_are_sequences_of_mappings(single_result)
            return single_result
        else:
            result: dict[EntityType, dict[EntityID, tuple[JSON, BundleFQID]]]
            result = defaultdict(dict)
            for contribution in contributions:
                that_bundle = contribution.coordinates.bundle
                for entity_type, those_entities in contribution.contents.items():
                    these_entities = result[entity_type]
                    for that_entity in json_element_mappings(those_entities):
                        that = (that_entity, that_bundle)
                        entity_id = transformer.inner_entity_id(entity_type, that_entity)
                        try:
                            this = these_entities[entity_id]
                        except KeyError:
                            these_entities[entity_id] = that
                        else:
                            that = transformer.reconcile_inner_entities(entity_type, this=this, that=that)
                            if this != that:
                                these_entities[entity_id] = that
            return {
                entity_type: [entity for entity, _ in entities.values()]
                for entity_type, entities in result.items()
            }

    def _create_writer(self,
                       doc_type: DocumentType,
                       catalog: CatalogName | None
                       ) -> IndexWriter:
        # We allow one conflict retry in the case of duplicate notifications and
        # switch from 'add' to 'update'. After that, there should be no
        # conflicts because we use an SQS FIFO message group per entity.
        # Conflicts are common when writing replicas due to entities being
        # shared between bundles. For other errors we use SQS message redelivery
        # to take care of the retries.
        limits = {
            DocumentType.contribution: 1,
            DocumentType.aggregate: 1,
            DocumentType.replica: config.replica_conflict_limit
        }
        return IndexWriter(catalog,
                           self.catalogued_field_types(),
                           refresh=False,
                           conflict_retry_limit=limits[doc_type],
                           error_retry_limit=0)


class IndexWriter:

    def __init__(self,
                 catalog: CatalogName | None,
                 field_types: CataloguedFieldTypes,
                 refresh: bool | str,
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
        self.opensearch = OpenSearchClientFactory.get()
        self.errors: dict[DocumentCoordinates, int] = defaultdict(int)
        self.conflicts: dict[DocumentCoordinates, int] = defaultdict(int)
        self.retries: set[DocumentCoordinates] = set()

    bulk_threshold = 32

    def write(self, documents: Sequence[Document]):
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
            try:
                method = getattr(self.opensearch, doc.op_type.name)
                method(refresh=self.refresh, **doc.to_index(self.catalog, self.field_types))
            except ConflictError as e:
                self._on_conflict(doc, e)
            except OpenSearchException as e:
                self._on_error(doc, e)
            else:
                self._on_success(doc)

    def _write_bulk(self, documents: Iterable[Document]):
        # FIXME: document this quirk
        docs_by_coordinates: dict[DocumentCoordinates, Document] = {
            doc.coordinates.with_catalog(self.catalog): doc
            for doc in documents
        } if self.catalog is not None else {
            doc.coordinates: doc
            for doc in documents
        }

        def expand_action(doc: Any) -> tuple[JSON, JSON | None]:
            # Document.to_index returns the keyword arguments to the ES client
            # method referenced by Document.op_type. In bulk requests, these
            # methods are not invoked individually. This function converts the
            # keyword arguments returned by Document.to_index to the form
            # internally used by the ES client's `bulk` method: a pair
            # consisting of 1) the action and associated metadata and 2) an
            # optional document source.
            assert isinstance(doc, Document), doc
            action = dict(doc.to_index(self.catalog, self.field_types))
            action.update(json_mapping(action.pop('params', {})))
            action['_index'] = action.pop('index')
            action['_id'] = action.pop('id')
            try:
                body = json_mapping(action.pop('body'))
            except KeyError:
                body = None
            action = {doc.op_type.name: action}
            return action, body

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
        # Technically, we're not supposed to pass Document instances in the
        # `action` parameter but we're exploiting the undocumented fact that the
        # method immediately maps the value of the `expand_action_callback`
        # parameter over the list passed in the `actions` parameter.
        response = streaming_bulk(client=self.opensearch,
                                  actions=list(docs_by_coordinates.values()),
                                  expand_action_callback=expand_action,
                                  refresh=self.refresh,
                                  raise_on_error=False,
                                  max_chunk_bytes=config.max_chunk_size)
        for success, info in response:
            op_type, info = one(info.items())
            assert op_type in OpType.__members__, op_type
            coordinates = DocumentCoordinates.from_hit(info)
            doc = docs_by_coordinates[coordinates]
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

    def _on_error(self, doc: Document, e: Exception | JSON):
        self.errors[doc.coordinates] += 1
        if self.error_retry_limit is None or self.errors[doc.coordinates] <= self.error_retry_limit:
            action = 'retrying'
            self.retries.add(doc.coordinates)
        else:
            action = 'giving up'
        log.warning('There was a general error with document %r: %r. Total # of errors: %i, %s.',
                    doc.coordinates, e, self.errors[doc.coordinates], action,
                    exc_info=isinstance(e, Exception))

    def _on_conflict(self, doc: Document, e: Exception | JSON):
        self.conflicts[doc.coordinates] += 1
        self.errors.pop(doc.coordinates, None)  # a conflict resets the error count
        if self.conflict_retry_limit is None or self.conflicts[doc.coordinates] <= self.conflict_retry_limit:
            action = 'retrying'
            self.retries.add(doc.coordinates)
        else:
            action = 'giving up'

        def warn():
            log.warning('There was a conflict with document %r: %r. Total # of errors: %i, %s.',
                        doc.coordinates, e, self.conflicts[doc.coordinates], action)

        if doc.op_type is OpType.create:
            try:
                doc.op_type = OpType.index
            except NotImplementedError:
                # We don't expect all Document types will let us modify op_type
                warn()
            else:
                log.warning('Document %r exists. Retrying with overwrite.', doc.coordinates)
        else:
            warn()

    def raise_on_errors(self):
        if self.errors or self.conflicts:
            log.warning('Failures: %r', self.errors)
            log.warning('Conflicts: %r', self.conflicts)
            raise RuntimeError('Failed to index documents. Failures: %i, conflicts: %i.' %
                               (len(self.errors), len(self.conflicts)))


class EventualConsistencyException(RuntimeError):
    pass
