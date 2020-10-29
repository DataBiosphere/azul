from collections import (
    Counter,
    defaultdict,
)
from itertools import (
    groupby,
)
import logging
from operator import (
    attrgetter,
)
from typing import (
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    MutableSet,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from elasticsearch import (
    ConflictError,
    ElasticsearchException,
)
from elasticsearch.helpers import (
    parallel_bulk,
    scan,
    streaming_bulk,
)
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    config,
)
from azul.deployment import (
    aws,
)
from azul.es import (
    ESClientFactory,
)
from azul.indexer import (
    Bundle,
    BundleUUID,
    BundleVersion,
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
    EntityID,
    EntityReference,
    EntityType,
    VersionType,
)
from azul.indexer.document_service import (
    DocumentService,
)
from azul.indexer.transform import (
    Transformer,
)
from azul.types import (
    JSON,
    MutableJSON,
)

log = logging.getLogger(__name__)

Tallies = Mapping[EntityReference, int]

CataloguedTallies = Mapping[CataloguedEntityReference, int]

MutableCataloguedTallies = MutableMapping[CataloguedEntityReference, int]

CollatedEntities = MutableMapping[EntityID, Tuple[BundleUUID, BundleVersion, JSON]]


class IndexService(DocumentService):

    def settings(self, index_name) -> JSON:

        aggregate = config.parse_es_index_name(index_name).aggregate
        num_nodes = aws.es_instance_count
        num_workers = config.indexer_concurrency

        # Put a primary aggregate shard on every node. Linearly scale the number
        # of contribution shards with the number of indexer workers i.e.,
        # writers. There was no notable difference in speed between factors 1
        # and 1/4 but the memory pressure was unsustainably high with factor 1.
        #
        num_shards = num_nodes if aggregate else max(num_nodes, num_workers // 4)

        # Replicate aggregate shards over the entire cluster, every node should
        # store the complete aggregate index to avoid intra-cluster talk when
        # responding to client queries, hopefully accelerating them. Aggregate
        # indices are small so distributing them fully makes sense.
        #
        # No replication for contribution indices because durability does not
        # matter to us as much. If a node goes down, we'll just have to reindex.
        # I have yet to perform a successful replacement of a cluster node.
        # With bigger cluster sizes this may become an issue again but I simply
        # don't sufficiently understand that scenario to be able to prepare for
        # it with confidence.
        #
        num_replicas = (num_nodes - 1) if aggregate else 0
        return {
            'index': {
                'number_of_shards': num_shards,
                'number_of_replicas': num_replicas,
                'refresh_interval': f'{config.es_refresh_interval}s'
            }
        }

    def index_names(self, catalog: CatalogName) -> List[str]:
        return [
            config.es_index_name(catalog=catalog,
                                 entity_type=entity_type,
                                 aggregate=aggregate)
            for entity_type in self.entity_types(catalog)
            for aggregate in (False, True)
        ]

    def index(self, catalog: CatalogName, bundle: Bundle) -> None:
        """
        Index the bundle referenced by the given notification. This is an
        inefficient default implementation. A more efficient implementation
        would transform many bundles, collect their contributions and aggregate
        all affected entities at the end.
        """
        contributions = self.transform(catalog, bundle, delete=False)
        tallies = self.contribute(catalog, contributions)
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
        contributions = self.transform(catalog, bundle, delete=True)
        # FIXME: these are all modified contributions, not new ones. This also
        #        happens when we reindex without deleting the indices first. The
        #        tallies refer to number of updated or added contributions but
        #        we treat them as if they are all new when we estimate the
        #        number of contributions per bundle.
        # https://github.com/DataBiosphere/azul/issues/610
        tallies = self.contribute(catalog, contributions)
        self.aggregate(tallies)

    def transform(self, catalog: CatalogName, bundle: Bundle, delete: bool):
        log.info('Transforming metadata for bundle %s, version %s.', bundle.uuid, bundle.version)
        contributions = []
        for transformer_cls in self.transformers(catalog):
            transformer: Transformer = transformer_cls.create(bundle, deleted=delete)
            contributions.extend(transformer.transform())
        return contributions

    def create_indices(self, catalog: CatalogName):
        es_client = ESClientFactory.get()
        for index_name in self.index_names(catalog):
            es_client.indices.create(index=index_name,
                                     ignore=[400],
                                     body=dict(settings=self.settings(index_name),
                                               mappings=dict(doc=self.metadata_plugin(catalog).mapping())))

    def delete_indices(self, catalog: CatalogName):
        es_client = ESClientFactory.get()
        for index_name in self.index_names(catalog):
            if es_client.indices.exists(index_name):
                es_client.indices.delete(index=index_name)

    def contribute(self, catalog: CatalogName, contributions: List[Contribution]) -> CataloguedTallies:
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
            # Read all contributions from Elasticsearch
            contributions = self._read_contributions(total_tallies)
            actual_tallies = Counter(contribution.coordinates.entity
                                     for contribution in contributions)
            if tallies.keys() != actual_tallies.keys():
                message = 'Could not find all expected contributions.'
                args = (tallies, actual_tallies) if config.debug else ()
                raise EventualConsistencyException(message, *args)
            assert all(tallies[entity] <= actual_tally
                       for entity, actual_tally in actual_tallies.items())
            # Combine the contributions into old_aggregates, one per entity
            new_aggregates = self._aggregate(contributions)
            # Set the expected document version from the old version
            for new_aggregate in new_aggregates:
                old_aggregate = old_aggregates.pop(new_aggregate.coordinates.entity, None)
                new_aggregate.version = None if old_aggregate is None else old_aggregate.version
            # Empty out any unreferenced aggregates (can only happen for deletions)
            for old_aggregate in old_aggregates.values():
                old_aggregate.contents = {}
                new_aggregates.append(old_aggregate)
            # Write aggregates to Elasticsearch
            writer.write(new_aggregates)
            # Retry if necessary
            if not writer.retries:
                break
            tallies: CataloguedTallies = {
                aggregate.coordinates.entity: tallies[aggregate.coordinates.entity]
                for aggregate in new_aggregates
                if aggregate.coordinates in writer.retries
            }
        writer.raise_on_errors()

    def _read_aggregates(self, entities: CataloguedTallies) -> Dict[CataloguedEntityReference, Aggregate]:
        coordinates = [
            AggregateCoordinates(entity=entity)
            for entity in entities
        ]
        request = {
            'docs': [
                {
                    '_type': coordinate.type,
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
                                              _source_include=list(mandatory_source_fields))

        def aggregates():
            for doc in response['docs']:
                if doc['found']:
                    coordinate = DocumentCoordinates.from_hit(doc)
                    aggregate_cls = self.aggregate_class(coordinate.entity.catalog)
                    aggregate = aggregate_cls.from_index(self.catalogued_field_types(),
                                                         doc,
                                                         coordinates=coordinate)
                    yield aggregate

        return {a.coordinates.entity: a for a in aggregates()}

    def _read_contributions(self, tallies: CataloguedTallies) -> List[CataloguedContribution]:
        es_client = ESClientFactory.get()
        entity_ids_by_index: MutableMapping[str, MutableSet[str]] = defaultdict(set)
        for entity in tallies.keys():
            index = config.es_index_name(catalog=entity.catalog,
                                         entity_type=entity.entity_type,
                                         aggregate=False)
            entity_ids_by_index[index].add(entity.entity_id)
        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                            "_index": index
                                        }
                                    },
                                    {
                                        "terms": {
                                            "entity_id.keyword": list(entity_ids)
                                        }
                                    }
                                ]
                            }
                        } for index, entity_ids in entity_ids_by_index.items()
                    ]
                }
            }
        }
        index = sorted(list(entity_ids_by_index.keys()))
        # scan() uses a server-side cursor and is expensive. Only use it if the number of contributions is large
        page_size = 1000  # page size of 100 caused excessive ScanError occurrences
        num_contributions = sum(tallies.values())
        hits = None
        if num_contributions <= page_size:
            log.info('Reading %i expected contribution(s) using search().', num_contributions)
            response = es_client.search(index=index, body=query, size=page_size, doc_type=Document.type)
            total_hits = response['hits']['total']
            if total_hits <= page_size:
                hits = response['hits']['hits']
                if len(hits) != total_hits:
                    message = f'Search returned {len(hits)} hits but reports total to be {total_hits}'
                    raise EventualConsistencyException(message)
            else:
                log.info('Expected only %i contribution(s) but got %i.', num_contributions, total_hits)
                num_contributions = total_hits
        if hits is None:
            log.info('Reading %i expected contribution(s) using scan().', num_contributions)
            hits = scan(es_client, index=index, query=query, size=page_size, doc_type=Document.type)
        contributions = [
            Contribution.from_index(self.catalogued_field_types(), hit)
            for hit in hits
        ]
        log.info('Read %i contribution(s). ', len(contributions))
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

    def _aggregate(self, contributions: List[CataloguedContribution]) -> List[Aggregate]:
        # Group contributions by entity and bundle UUID
        contributions_by_bundle: Mapping[
            Tuple[CataloguedEntityReference, BundleUUID],
            List[CataloguedContribution]
        ] = defaultdict(list)
        tallies: MutableCataloguedTallies = Counter()
        for contribution in contributions:
            entity = contribution.coordinates.entity
            bundle_uuid = contribution.coordinates.bundle.uuid
            contributions_by_bundle[entity, bundle_uuid].append(contribution)
            # Track the raw, unfiltered number of contributions per entity.
            assert isinstance(contribution.coordinates.entity, CataloguedEntityReference)
            tallies[contribution.coordinates.entity] += 1

        # For each entity and bundle, find the most recent contribution that is not a deletion
        contributions_by_entity: MutableMapping[
            CataloguedEntityReference, List[CataloguedContribution]] = defaultdict(list)
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
        transformers: Dict[Tuple[CatalogName, str], Type[Transformer]] = {
            (catalog, transformer.entity_type()): transformer
            for catalog in config.catalogs
            for transformer in self.transformers(catalog)
        }

        # Aggregate contributions for the same entity
        aggregates = []
        for entity, contributions in contributions_by_entity.items():
            transformer = transformers[entity.catalog, entity.entity_type]
            contents = self._aggregate_entity(transformer, contributions)
            bundles = [
                dict(uuid=c.coordinates.bundle.uuid,
                     version=c.coordinates.bundle.version)
                for c in contributions
            ]
            aggregate_cls = self.aggregate_class(entity.catalog)
            aggregate = aggregate_cls(coordinates=AggregateCoordinates(entity=entity),
                                      version=None,
                                      contents=contents,
                                      bundles=bundles,
                                      num_contributions=tallies[entity])
            aggregates.append(aggregate)

        return aggregates

    def _aggregate_entity(self,
                          transformer: Type[Transformer],
                          contributions: List[Contribution]) -> MutableJSON:
        contents = self._select_latest(contributions)
        aggregate_contents = {}
        for entity_type, entities in contents.items():
            if entity_type == transformer.entity_type():
                assert len(entities) == 1
            else:
                aggregator = transformer.get_aggregator(entity_type)
                if aggregator is not None:
                    entities = aggregator.aggregate(entities)
            aggregate_contents[entity_type] = entities
        return aggregate_contents

    def _select_latest(self, contributions: Sequence[Contribution]) -> MutableMapping[EntityType, Entities]:
        """
        Collect the latest version of each inner entity from multiple given documents.

        If two or more contributions contain copies of the same inner entity, potentially with different contents, the
        copy from the contribution with the latest bundle version will be selected.
        """
        if len(contributions) == 1:
            return one(contributions).contents
        else:
            contents: MutableMapping[EntityType, CollatedEntities] = defaultdict(dict)
            for contribution in contributions:
                for entity_type, entities in contribution.contents.items():
                    collated_entities = contents[entity_type]
                    entity: JSON
                    for entity in entities:
                        entity_id = entity['document_id']  # FIXME: the key 'document_id' is HCA specific
                        cur_bundle_uuid, cur_bundle_version, cur_entity = \
                            collated_entities.get(entity_id, (None, '', None))
                        if cur_entity is not None and entity.keys() != cur_entity.keys():
                            symmetric_difference = set(entity.keys()).symmetric_difference(cur_entity)
                            log.warning('Document shape of `%s` entity `%s` does not match between bundles '
                                        '%s, version %s and %s, version %s: %s',
                                        entity_type, entity_id,
                                        cur_bundle_uuid, cur_bundle_version,
                                        contribution.coordinates.bundle.uuid,
                                        contribution.coordinates.bundle.version,
                                        symmetric_difference)
                        if cur_bundle_version < contribution.coordinates.bundle.version:
                            collated_entities[entity_id] = (
                                contribution.coordinates.bundle.uuid,
                                contribution.coordinates.bundle.version,
                                entity
                            )
            return {
                entity_type: [entity for _, _, entity in entities.values()]
                for entity_type, entities in contents.items()
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
        self.errors: MutableMapping[DocumentCoordinates, int] = defaultdict(int)
        self.conflicts: MutableMapping[DocumentCoordinates, int] = defaultdict(int)
        self.retries: Optional[MutableSet[DocumentCoordinates]] = None

    bulk_threshold = 32

    def write(self, documents: List[Document]):
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
        documents: Dict[DocumentCoordinates, Document] = {
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
        if len(actions) < 1024:
            log.info('Writing documents using streaming_bulk().')
            helper = streaming_bulk
        else:
            log.info('Writing documents using parallel_bulk().')
            helper = parallel_bulk
        response = helper(client=self.es_client,
                          actions=actions,
                          refresh=self.refresh,
                          raise_on_error=False,
                          max_chunk_bytes=10485760)
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

    def _on_error(self, doc: Document, e):
        self.errors[doc.coordinates] += 1
        if self.error_retry_limit is None or self.errors[doc.coordinates] <= self.error_retry_limit:
            action = 'retrying'
            self.retries.add(doc.coordinates)
        else:
            action = 'giving up'
        log.warning('There was a general error with document %r: %r. Total # of errors: %i, %s.',
                    doc.coordinates, e, self.errors[doc.coordinates], action)

    def _on_conflict(self, doc: Document, e):
        self.conflicts[doc.coordinates] += 1
        self.errors.pop(doc.coordinates, None)  # a conflict resets the error count
        if self.conflict_retry_limit is None or self.conflicts[doc.coordinates] <= self.conflict_retry_limit:
            action = 'retrying'
            self.retries.add(doc.coordinates)
        else:
            action = 'giving up'
        if doc.version_type is VersionType.create_only:
            log.warning('Writing document %r requires overwrite. Possible causes include duplicate notifications '
                        'or reindexing without clearing the index.', doc.coordinates)
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
