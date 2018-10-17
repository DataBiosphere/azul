from abc import ABC
from collections import defaultdict, OrderedDict
from itertools import chain
import logging
from operator import attrgetter
from typing import Callable, List, Mapping, MutableMapping, Sequence, Union

from elasticsearch import ConflictError, ElasticsearchException
from elasticsearch.helpers import parallel_bulk, streaming_bulk
from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata

from azul import config
from azul.base_config import BaseIndexProperties
from azul.es import ESClientFactory
from azul.transformer import ElasticSearchDocument, DocumentCoordinates
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

    def write(self, documents: DocumentsById, conflict_retry_limit=None, error_retry_limit=3) -> None:
        errored_documents = defaultdict(int)
        conflict_documents = defaultdict(int)
        es_client = ESClientFactory.get()

        while documents:
            modified_docs = self._merge_existing_docs(documents)
            aggregate_docs = self._aggregate_docs(modified_docs)
            log.info("Writing %i modified and %i aggregate document(s) for of a total of %i contribution(s).",
                     len(modified_docs), len(aggregate_docs), len(documents))
            # Since we are piggy-backing the versioning of the aggregate documents onto the versioning of the
            # origignal documents, we need to ensure that all processes update the documents in the same order or
            # else we risk dead-lock.
            write_documents = sorted(chain(modified_docs.values(), aggregate_docs.values()),
                                     key=attrgetter('entity_type', 'document_id', 'aggregate'))
            write_documents = OrderedDict((doc.coordinates, doc) for doc in write_documents)
            assert len(write_documents) == len(modified_docs) + len(aggregate_docs)

            retry_ids = set()

            def on_success(coordinates):
                doc = write_documents[coordinates]
                assert doc.coordinates == coordinates
                log.debug('Successfully wrote document %s%s/%s with contributions from %i bundle(s).',
                          doc.entity_type, '_aggregate' if doc.aggregate else '', doc.document_id, len(doc.bundles))
                conflict_documents.pop(coordinates, None)
                errored_documents.pop(coordinates, None)

            def on_conflict(coordinates, error):
                doc = write_documents[coordinates]
                assert doc.coordinates == coordinates
                errored_documents.pop(coordinates, None)  # a conflict resets the error count
                if conflict_retry_limit is None or conflict_documents[coordinates] < conflict_retry_limit:
                    conflict_documents[coordinates] += 1
                    action = 'retrying'
                    retry_ids.add(doc.original_coordinates if doc.aggregate else doc.coordinates)
                else:
                    action = 'giving up'
                log.warning('There was a conflict with document %r: %r. Total # of errors: %i, %s.',
                            coordinates, error, conflict_documents[coordinates], action)

            def on_error(coordinates, error):
                doc = write_documents[coordinates]
                assert doc.coordinates == coordinates
                if error_retry_limit is None or errored_documents[coordinates] < error_retry_limit:
                    errored_documents[coordinates] += 1
                    action = 'retrying'
                    retry_ids.add(doc.original_coordinates if doc.aggregate else doc.coordinates)
                else:
                    action = 'giving up'
                log.warning('There was a general error with document %r: %r. Total # of errors: %i, %s.',
                            coordinates, error, errored_documents[coordinates], action)

            if len(write_documents) < 128:
                for doc in write_documents.values():
                    if doc.aggregate and doc.original_coordinates in retry_ids:
                        # If writing the orginal document failed, don't even attempt to write the aggregate. This is
                        # not just an optimization: If a loser to the orginal race continued on to write the
                        # aggregate won that race, it would spoil the original winner's victory. This could lead to
                        # deadlock
                        continue
                    try:
                        es_client.index(index=doc.document_index,
                                        doc_type=doc.document_type,
                                        body=doc.to_source(),
                                        id=doc.document_id,
                                        refresh=self.refresh,
                                        version=doc.document_version,
                                        version_type='external')
                    except ConflictError as e:
                        on_conflict(doc.coordinates, e)
                    except ElasticsearchException as e:
                        on_error(doc.coordinates, e)
                    else:
                        on_success(doc.coordinates)
            else:
                actions = [dict(_op_type="index",
                                _index=doc.document_index,
                                _type=doc.document_type,
                                _source=doc.to_source(),
                                _id=doc.document_id,
                                version=doc.document_version,
                                version_type="external") for doc in write_documents.values()]
                if len(actions) < 1024:
                    log.info("Using streaming_bulk().")
                    helper = streaming_bulk
                else:
                    log.info("Using parallel_bulk().")
                    helper = parallel_bulk
                response = helper(client=es_client,
                                  actions=actions,
                                  refresh=self.refresh,
                                  raise_on_error=False,
                                  max_chunk_bytes=10485760)
                for success, info in response:
                    coordinates = info['index']['_index'], info['index']['_id']
                    if success:
                        on_success(coordinates)
                    else:
                        if info['index']['status'] == 409:
                            on_conflict(coordinates, info)
                        else:
                            on_error(coordinates, info)

            # Collect the set of documents to be retried
            documents = {k: v for k, v in documents.items() if k in retry_ids}
        if errored_documents or conflict_documents:
            log.warning('Failures: %r', errored_documents)
            log.warning('Conflicts: %r', conflict_documents)
            raise RuntimeError('Failed to index documents. Failures: %i, conflicts: %i.' %
                               (len(errored_documents), len(conflict_documents)))

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
