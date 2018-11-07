from abc import ABC
from collections import OrderedDict, defaultdict
import logging
from typing import Callable, List, Mapping, MutableMapping, MutableSet, Optional, Sequence, Union

from elasticsearch import ConflictError, ElasticsearchException
from elasticsearch.helpers import parallel_bulk, streaming_bulk
from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata

from azul import config
from azul.base_config import BaseIndexProperties
from azul.es import ESClientFactory
from azul.transformer import DocumentCoordinates, ElasticSearchDocument
from azul.types import JSON

log = logging.getLogger(__name__)

DocumentsById = Mapping[DocumentCoordinates, ElasticSearchDocument]

DocumentHandler = Callable[[DocumentsById], None]


class BaseIndexer(ABC):
    """
    The base indexer class provides the framework to do indexing.
    """

    def __init__(self, properties: BaseIndexProperties,
                 document_handler: DocumentHandler = None,
                 refresh: Union[bool, str] = False) -> None:
        self.properties = properties
        self.document_handler = document_handler or self.write
        self.refresh = refresh

    def index(self, dss_notification: JSON) -> None:
        bundle_uuid = dss_notification['match']['bundle_uuid']
        bundle_version = dss_notification['match']['bundle_version']
        _, manifest, metadata_files = download_bundle_metadata(client=config.dss_client(self.properties.dss_url),
                                                               replica='aws',
                                                               uuid=bundle_uuid,
                                                               version=bundle_version,
                                                               num_workers=config.num_dss_workers)
        assert _ == bundle_version

        # FIXME: this seems out of place. Consider creating indices at deploy time and avoid the mostly
        # redundant requests for every notification (https://github.com/DataBiosphere/azul/issues/427)
        es_client = ESClientFactory.get()
        for index_name in self.properties.index_names:
            es_client.indices.create(index=index_name,
                                     ignore=[400],
                                     body=dict(settings=self.properties.settings,
                                               mappings=dict(doc=self.properties.mapping)))

        # Collect the documents to be indexed
        indexable_documents = {}
        for transformer in self.properties.transformers:
            documents = transformer.create_documents(uuid=bundle_uuid,
                                                     version=bundle_version,
                                                     manifest=manifest,
                                                     metadata_files=metadata_files)
            for document in documents:
                indexable_documents[document.coordinates] = document

        self.document_handler(indexable_documents)

    def collate(self, documents: Sequence[ElasticSearchDocument]) -> DocumentsById:
        """
        Group the given documents by ID and consolidate the documents within each group into a single document.
        """
        groups_by_id: MutableMapping[DocumentCoordinates, List[ElasticSearchDocument]] = defaultdict(list)
        for document in documents:
            groups_by_id[document.coordinates].append(document)
        documents_by_id = {}
        for documents in groups_by_id.values():
            first, rest = documents[0], documents[1:]
            first.consolidate(rest)
            documents_by_id[first.coordinates] = first
        return documents_by_id

    def write(self, contributions: DocumentsById, conflict_retry_limit=None, error_retry_limit=2) -> None:
        """
        For each of the given document contributions, this method loads the existing document using the coordinates
        of the contribution, merges the contribution into that document, derives an aggregate from the merged
        document and lastly writes both the merged document and the aggregate.
        
        :param contributions: the document contributions to be written by their coordinates

        :param conflict_retry_limit: The maximum number of retries (the second attempt is the first retry) on version
                                     conflicts. Specify 0 for no retries or None for unlimited retries.

        :param error_retry_limit: The maximum number of retries (the second attempt is the first retry) on other
                                  errors. Specify 0 for no retries or None for unlimited retries.
        """
        writer = IndexWriter(refresh=self.refresh,
                             conflict_retry_limit=conflict_retry_limit,
                             error_retry_limit=error_retry_limit)
        while contributions:
            originals = self._merge_existing_docs(contributions)
            aggregates = self._aggregate_docs(originals)
            log.info("Writing %i modified and %i aggregate document(s) for of a total of %i contribution(s).",
                     len(originals), len(aggregates), len(contributions))
            writer.write(aggregates, originals)
            contributions = {k: v for k, v in contributions.items() if k in writer.retries}
        writer.raise_on_errors()

    def _aggregate_docs(self, modified_docs):
        aggregate_docs = {}
        for doc in modified_docs.values():
            for transformer in self.properties.transformers:
                aggregate_doc = transformer.aggregate_document(doc)
                if aggregate_doc is not None:
                    assert aggregate_doc.aggregate
                    aggregate_docs[aggregate_doc.coordinates] = aggregate_doc
        return aggregate_docs

    def _merge_existing_docs(self, documents: DocumentsById) -> DocumentsById:
        mget_body = dict(docs=[dict(_index=doc.document_index,
                                    _type=doc.document_type,
                                    _id=doc.document_id) for doc in documents.values()])
        response = ESClientFactory.get().mget(body=mget_body)
        existing_documents = [doc for doc in response["docs"] if doc['found']]
        if existing_documents:
            log.info("Merging %i existing document(s).", len(existing_documents))
            # Make a mutable copy …
            merged_documents = dict(documents)
            # … and update with documents already in index
            for existing in existing_documents:
                existing = ElasticSearchDocument.from_index(existing)
                update = merged_documents[existing.coordinates]
                if existing.update_with(update):
                    merged_documents[existing.coordinates] = existing
                else:
                    log.debug('Successfully verified document %s/%s with contributions from %i bundle(s).',
                              existing.entity_type, existing.document_id, len(existing.bundles))
                    merged_documents.pop(existing.coordinates)
            return merged_documents
        else:
            log.info("No existing documents to merge with.")
            return documents

    def delete(self, dss_notification: JSON) -> None:
        bundle_uuid = dss_notification['match']['bundle_uuid']
        bundle_version = dss_notification['match']['bundle_version']
        docs = self._get_docs_by_uuid_and_version(bundle_uuid, bundle_version)

        documents_by_id = {}
        for hit in docs['hits']['hits']:
            doc = ElasticSearchDocument.from_index(hit)
            if not doc.aggregate:
                for bundle in doc.bundles:
                    if bundle.version == bundle_version and bundle.uuid == bundle_uuid:
                        bundle.delete()
                documents_by_id[doc.coordinates] = doc

        self.document_handler(documents_by_id)

    def _get_docs_by_uuid_and_version(self, bundle_uuid, bundle_version):
        search_query = {
            "version": True,
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "bundles.uuid.keyword": {
                                    "value": bundle_uuid
                                }
                            }
                        },
                        {
                            "term": {
                                "bundles.version.keyword": {
                                    "value": bundle_version
                                }
                            }
                        }
                    ]
                }
            }
        }
        es_client = ESClientFactory.get()
        return es_client.search(doc_type="doc", body=search_query)


class IndexWriter:
    def __init__(self,
                 refresh: Union[bool, str],
                 conflict_retry_limit: Optional[int],
                 error_retry_limit: Optional[int]) -> None:
        super().__init__()
        self.refresh = refresh
        self.conflict_retry_limit = conflict_retry_limit
        self.error_retry_limit = error_retry_limit
        self.es_client = ESClientFactory.get()
        self.errors: MutableMapping[DocumentCoordinates, int] = defaultdict(int)
        self.conflicts: MutableMapping[DocumentCoordinates, int] = defaultdict(int)
        self.retries: MutableSet[DocumentCoordinates] = None

    # Since we are piggy-backing the versioning of the aggregate documents onto the versioning of the original
    # documents, we need to ensure that all concurrent writers update the documents in the same order or else we risk
    #  dead-lock. It is also important that we write the aggregate before the original, to ensure that success in
    # writing the original implies success in writing the aggregate. Without this invariant we could not safely skip
    # writing both original and aggregate was found to be original already up-to-date in the index.

    def write(self, originals: DocumentsById, aggregates: DocumentsById):
        self.retries = set()
        self._write(aggregates)
        self._write(originals)

    def _write(self, documents: DocumentsById):
        documents = sorted(documents.values(), key=lambda doc: (doc.entity_type, doc.document_id))
        documents = OrderedDict((doc.coordinates, doc) for doc in documents)
        if len(documents) < 128:
            self._write_individually(documents)
        else:
            self._write_bulk(documents)

    def _write_individually(self, documents):
        log.info('Writing documents individually')
        for doc in documents.values():
            if self._writeable(doc):
                try:
                    version_type = self._version_type(doc)
                    self.es_client.index(index=doc.document_index,
                                         doc_type=doc.document_type,
                                         body=doc.to_source(),
                                         id=doc.document_id,
                                         refresh=self.refresh,
                                         version=doc.document_version,
                                         version_type=version_type)
                except ConflictError as e:
                    self._on_conflict(doc, e)
                except ElasticsearchException as e:
                    self._on_error(doc, e)
                else:
                    self._on_success(doc)

    def _write_bulk(self, documents):
        actions = [dict(_op_type='index',
                        _index=doc.document_index,
                        _type=doc.document_type,
                        _source=doc.to_source(),
                        _id=doc.document_id,
                        version=doc.document_version,
                        version_type=self._version_type(doc))
                   for doc in documents.values()
                   if self._writeable(doc)]
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
            coordinates = info['index']['_index'], info['index']['_id']
            doc = documents[coordinates]
            if success:
                self._on_success(doc)
            else:
                if info['index']['status'] == 409:
                    self._on_conflict(doc, info)
                else:
                    self._on_error(doc, info)

    def _version_type(self, doc):
        """
        Return the ES version type to be used for this document

        If writing the aggregate succeeds and writing the orginal fails (either due to a conflict or a general
        error), the aggregate will be one version ahead of the original. In fact, the version difference will
        invariantly be either 0 or 1. This is acceptable and the inconsistency will be fixed eventually by a retry.
        We could actually disable versioning on aggregates without detriment but in order to assert the invariant
        here we use `external_gte` for aggregates.
        """
        return 'external' if doc.original else 'external_gte'

    def _writeable(self, doc):
        """
        Return True if this document can be written.

        If writing the aggregate document fails, don't even attempt to write the original. This is not just an
        optimization: If a loser to the aggregate race continued on to write the original and won that race,
        it would spoil the aggregate winner's victory and could lead to deadlock. Deadlocks manifest itself in two or
        more processes processes spoiling each others victory perpetually.
        """
        return doc.aggregate or (doc.aggregate_coordinates not in self.errors and
                                 doc.aggregate_coordinates not in self.conflicts)

    def _on_success(self, doc):
        self.conflicts.pop(doc.coordinates, None)
        self.errors.pop(doc.coordinates, None)
        log.debug('Successfully wrote document %s%s/%s with contributions from %i bundle(s).',
                  doc.entity_type, '_aggregate' if doc.aggregate else '', doc.document_id, len(doc.bundles))

    def _on_error(self, doc, e):
        self.errors[doc.coordinates] += 1
        if self.error_retry_limit is None or self.errors[doc.coordinates] <= self.error_retry_limit:
            action = 'retrying'
            # Always retry the original. This will regenerate and retry the aggregate.
            self.retries.add(doc.original_coordinates)
        else:
            action = 'giving up'
        log.warning('There was a general error with document %r: %r. Total # of errors: %i, %s.',
                    doc.coordinates, e, self.errors[doc.coordinates], action)

    def _on_conflict(self, doc, e):
        self.conflicts[doc.coordinates] += 1
        self.errors.pop(doc.coordinates, None)  # a conflict resets the error count
        if self.conflict_retry_limit is None or self.conflicts[doc.coordinates] <= self.conflict_retry_limit:
            action = 'retrying'
            # Always retry the original. This will regenerate and retry the aggregate.
            self.retries.add(doc.original_coordinates)
        else:
            action = 'giving up'
        log.warning('There was a conflict with document %r: %r. Total # of errors: %i, %s.',
                    doc.coordinates, e, self.conflicts[doc.coordinates], action)

    def raise_on_errors(self):
        if self.errors or self.conflicts:
            log.warning('Failures: %r', self.errors)
            log.warning('Conflicts: %r', self.conflicts)
            raise RuntimeError('Failed to index documents. Failures: %i, conflicts: %i.' %
                               (len(self.errors), len(self.conflicts)))
