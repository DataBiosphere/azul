import logging
from abc import ABC
from collections import defaultdict
from typing import Any, Mapping

from elasticsearch import ConflictError, ElasticsearchException
from elasticsearch.helpers import parallel_bulk, streaming_bulk

from azul.base_config import BaseIndexProperties
from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata
from azul.es import ESClientFactory
from azul.transformer import ElasticSearchDocument

from azul import config

log = logging.getLogger(__name__)


class BaseIndexer(ABC):
    """
    The base indexer class provides the framework to do indexing.
    """
    def __init__(self, properties: BaseIndexProperties) -> None:
        self.properties = properties

    def index(self, dss_notification: Mapping[str, Any]) -> None:
        # Calls extract, transform, merge, and load
        bundle_uuid = dss_notification['match']['bundle_uuid']
        bundle_version = dss_notification['match']['bundle_version']
        _, manifest, metadata_files = download_bundle_metadata(client=config.dss_client(self.properties.dss_url),
                                                               replica='aws',
                                                               uuid=bundle_uuid,
                                                               version=bundle_version,
                                                               num_workers=config.num_dss_workers)

        es_client = ESClientFactory.get()
        # Create indices and populate mappings
        for index_name in self.properties.index_names:
            # 400 is the status code in case the index already exists
            es_client.indices.create(index=index_name, body=self.properties.settings, ignore=[400])
            es_client.indices.put_mapping(index=index_name, doc_type="doc", body=self.properties.mapping)

        errored_documents = defaultdict(int)
        conflict_documents = defaultdict(int)

        # Collect the documents to be indexed
        indexable_documents = {}
        for transformer in self.properties.transformers:
            es_documents = transformer.create_documents(uuid=bundle_uuid,
                                                        version=bundle_version,
                                                        manifest=manifest,
                                                        metadata_files=metadata_files)
            for es_document in es_documents:
                indexable_documents[es_document.document_id] = es_document

        while indexable_documents:
            log.info("%s.%s: Indexing %i document(s).", bundle_uuid, bundle_version, len(indexable_documents))
            mget_body = dict(docs=[dict(_index=doc.document_index,
                                        _type=doc.document_type,
                                        _id=doc.document_id) for doc in indexable_documents.values()])
            response = es_client.mget(body=mget_body)
            cur_docs = [doc for doc in response["docs"] if doc['found']]
            if cur_docs:
                log.info("%s.%s: Merging %i existing document(s).", bundle_uuid, bundle_version, len(cur_docs))
                for cur_doc in cur_docs:
                    cur_doc = ElasticSearchDocument.from_index(cur_doc)
                    new_doc = indexable_documents[cur_doc.document_id]
                    new_doc.merge(cur_doc)
            else:
                log.info("%s.%s: No existing documents to merge with.", bundle_uuid, bundle_version)

            retry_ids = set()

            def on_success(doc_id):
                log.debug('%s.%s: Successfully indexed document %s', bundle_uuid, bundle_version, doc_id)
                conflict_documents.pop(doc_id, None)
                errored_documents.pop(doc_id, None)

            def on_conflict(doc_id, error):
                log.warning('%s.%s: There was a conflict with document %s: %r, retrying.',
                            bundle_uuid, bundle_version, doc_id, error)
                conflict_documents[doc_id] += 1
                errored_documents.pop(doc_id, None)
                retry_ids.add(doc_id)

            def on_error(doc_id, error):
                if errored_documents[doc_id] < 3:
                    errored_documents[doc_id] += 1
                    action = 'retrying'
                    retry_ids.add(doc_id)
                else:
                    action = 'giving up'
                log.warning('%s.%s: There was a general error with document %s: %r. Total # of errors: %i, %s.',
                            bundle_uuid, bundle_version, doc_id, error, errored_documents[doc_id], action)

            if len(indexable_documents) < 32:
                for cur_doc in indexable_documents.values():
                    doc_id = cur_doc.document_id
                    try:
                        es_client.index(index=cur_doc.document_index,
                                        doc_type=cur_doc.document_type,
                                        body=cur_doc.to_source(),
                                        id=doc_id,
                                        refresh="wait_for",
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
                                version_type="external") for doc in indexable_documents.values()]
                if len(actions) < 1024:
                    log.info("%s.%s: Using streaming_bulk().", bundle_uuid, bundle_version)
                    helper = streaming_bulk
                else:
                    log.info("%s.%s: Using parallel_bulk().", bundle_uuid, bundle_version)
                    helper = parallel_bulk
                response = helper(client=es_client,
                                  actions=actions,
                                  refresh="wait_for",
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
            indexable_documents = {k: v for k, v in indexable_documents.items() if k in retry_ids}

        if errored_documents or conflict_documents:
            log.warning('%s.%s: failures: %r', bundle_uuid, bundle_version, errored_documents)
            log.warning('%s.%s: conflicts: %r', bundle_uuid, bundle_version, conflict_documents)
            raise RuntimeError('%s.%s: Failed to index bundle. Failures: %i, conflicts: %i.' %
                               (bundle_uuid, bundle_version, len(errored_documents), len(conflict_documents)))
