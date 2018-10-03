from abc import ABC
from collections import defaultdict
import logging
from typing import Callable, List, Mapping, MutableMapping, Sequence, Union

from elasticsearch import ConflictError, ElasticsearchException
from elasticsearch.helpers import parallel_bulk, streaming_bulk
from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata

from azul import config
from azul.base_config import BaseIndexProperties
from azul.es import ESClientFactory
from azul.transformer import ElasticSearchDocument
from azul.types import JSON

log = logging.getLogger(__name__)

DocumentsById = Mapping[str, ElasticSearchDocument]

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

        # FIXME: this seems out of place. Consider creating indices at deploy time and avoid the mostly
        # redundant requests for every notification.
        es_client = ESClientFactory.get()
        for index_name in self.properties.index_names:
            es_client.indices.create(index=index_name,
                                     ignore=[400],
                                     body=dict(settings=self.properties.settings,
                                               mappings=dict(doc=self.properties.mapping)))

        # Collect the documents to be indexed
        indexable_documents = {}
        for transformer in self.properties.transformers:
            es_documents = transformer.create_documents(uuid=bundle_uuid,
                                                        version=bundle_version,
                                                        manifest=manifest,
                                                        metadata_files=metadata_files)
            for es_document in es_documents:
                indexable_documents[es_document.document_id] = es_document

        self.document_handler(indexable_documents)

    def collate(self, documents: Sequence[ElasticSearchDocument]) -> DocumentsById:
        """
        Group the given documents by ID and consolidate the documents within each group into a single document.
        """
        groups_by_id: MutableMapping[str, List[ElasticSearchDocument]] = defaultdict(list)
        for document in documents:
            groups_by_id[document.document_id].append(document)
        documents_by_id = {}
        for documents in groups_by_id.values():
            first, rest = documents[0], documents[1:]
            first.consolidate(rest)
            documents_by_id[first.document_id] = first
        return documents_by_id

    def write(self, documents_by_id: DocumentsById, conflict_retry_limit=None, error_retry_limit=3) -> None:
        errored_documents = defaultdict(int)
        conflict_documents = defaultdict(int)
        es_client = ESClientFactory.get()

        while documents_by_id:
            log.info("Indexing %i document(s).", len(documents_by_id))

            documents_by_id = self._merge_existing_docs(documents_by_id)

            retry_ids = set()

            def on_success(doc_id):
                doc = documents_by_id[doc_id]
                log.debug('Successfully wrote document %s/%s with contributions from %i bundle(s).',
                          doc.entity_type, doc_id, len(doc.bundles))
                conflict_documents.pop(doc_id, None)
                errored_documents.pop(doc_id, None)

            def on_conflict(doc_id, error):
                errored_documents.pop(doc_id, None)  # a conflict resets the error count
                if conflict_retry_limit is None or conflict_documents[doc_id] < conflict_retry_limit:
                    conflict_documents[doc_id] += 1
                    action = 'retrying'
                    retry_ids.add(doc_id)
                else:
                    action = 'giving up'
                log.warning('There was a conflict with document %s: %r. Total # of errors: %i, %s.',
                            doc_id, error, conflict_documents[doc_id], action)

            def on_error(doc_id, error):
                if error_retry_limit is None or errored_documents[doc_id] < error_retry_limit:
                    errored_documents[doc_id] += 1
                    action = 'retrying'
                    retry_ids.add(doc_id)
                else:
                    action = 'giving up'
                log.warning('There was a general error with document %s: %r. Total # of errors: %i, %s.',
                            doc_id, error, errored_documents[doc_id], action)

            if len(documents_by_id) < 128:
                for cur_doc in documents_by_id.values():
                    doc_id = cur_doc.document_id
                    try:
                        es_client.index(index=cur_doc.document_index,
                                        doc_type=cur_doc.document_type,
                                        body=cur_doc.to_source(),
                                        id=doc_id,
                                        refresh=self.refresh,
                                        version=cur_doc.document_version,
                                        version_type='external')
                    except ConflictError as e:
                        on_conflict(doc_id, e)
                    except ElasticsearchException as e:
                        on_error(doc_id, e)
                    else:
                        on_success(doc_id)
            else:
                actions = [dict(_op_type="index",
                                _index=doc.document_index,
                                _type=doc.document_type,
                                _source=doc.to_source(),
                                _id=doc.document_id,
                                version=doc.document_version,
                                version_type="external") for doc in documents_by_id.values()]
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
                    doc_id = info['index']['_id']
                    if success:
                        on_success(doc_id)
                    else:
                        if info['index']['status'] == 409:
                            on_conflict(doc_id, info)
                        else:
                            on_error(doc_id, info)

            # Collect the set of documents to be retried
            documents_by_id = {k: v for k, v in documents_by_id.items() if k in retry_ids}
        if errored_documents or conflict_documents:
            log.warning('Failures: %r', errored_documents)
            log.warning('Conflicts: %r', conflict_documents)
            raise RuntimeError('Failed to index documents. Failures: %i, conflicts: %i.' %
                               (len(errored_documents), len(conflict_documents)))

    def _merge_existing_docs(self, documents_by_id: DocumentsById) -> DocumentsById:
        mget_body = dict(docs=[dict(_index=doc.document_index,
                                    _type=doc.document_type,
                                    _id=doc.document_id) for doc in documents_by_id.values()])
        response = ESClientFactory.get().mget(body=mget_body)
        cur_docs = [doc for doc in response["docs"] if doc['found']]
        if cur_docs:
            log.info("Merging %i existing document(s).", len(cur_docs))
            # Make a mutable copy …
            merged_documents_by_id = dict(documents_by_id)
            # … and update with documents already in index
            for cur_doc in cur_docs:
                cur_doc = ElasticSearchDocument.from_index(cur_doc)
                new_doc = merged_documents_by_id[cur_doc.document_id]
                cur_doc.update_with(new_doc)
                merged_documents_by_id[cur_doc.document_id] = cur_doc
            return merged_documents_by_id
        else:
            log.info("No existing documents to merge with.")
            return documents_by_id

    def delete(self, dss_notification: JSON) -> None:
        bundle_uuid = dss_notification['match']['bundle_uuid']
        bundle_version = dss_notification['match']['bundle_version']
        docs = self._get_docs_by_uuid_and_version(bundle_uuid, bundle_version)

        documents_by_id = {}
        for hit in docs['hits']['hits']:
            doc = ElasticSearchDocument.from_index(hit)
            for bundle in doc.bundles:
                if bundle.version == bundle_version and bundle.uuid == bundle_uuid:
                    bundle.delete()
            documents_by_id[doc.document_id] = doc

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
