import logging
from abc import ABC
from collections import defaultdict
from typing import Any, Mapping

from elasticsearch import ConflictError, ElasticsearchException
from elasticsearch.helpers import parallel_bulk, streaming_bulk

from azul.base_config import BaseIndexProperties
from azul.downloader import MetadataDownloader

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
        metadata_downloader = MetadataDownloader(self.properties.dss_url)
        metadata_files, manifest = metadata_downloader.extract_bundle(dss_notification)
        es_client = self.properties.elastic_search_client

        # Create indices and populate mappings
        for index_name in self.properties.index_names:
            # 400 is the status code in case the index already exists
            es_client.indices.create(index=index_name, body=self.properties.settings, ignore=[400])
            es_client.indices.put_mapping(index=index_name, doc_type="doc", body=self.properties.mapping)

        # Collect the documents to be indexed
        indexable_documents = {}
        for transformer in self.properties.transformers:
            es_documents = transformer.create_documents(uuid=bundle_uuid,
                                                        version=bundle_version,
                                                        manifest=manifest,
                                                        metadata_files=metadata_files)
            for es_document in es_documents:
                indexable_documents[es_document.document_id] = es_document

        self._update_content(bundle_uuid, bundle_version, es_client, indexable_documents, is_indexer=True)

    def _update_content(self, bundle_uuid, bundle_version, es_client, indexable_documents, is_indexer):
        errored_documents = defaultdict(int)
        conflict_documents = defaultdict(int)
        while indexable_documents:
            log.info("%s.%s: Indexing %i document(s).", bundle_uuid, bundle_version, len(indexable_documents))
            mget_body = dict(docs=[dict(_index=doc.document_index if is_indexer else doc['_index'],
                                        _type=doc.document_type if is_indexer else doc['_type'],
                                        _id=doc.document_id if is_indexer else doc['_id']) for doc in (indexable_documents.values() if is_indexer else indexable_documents)])
            response = es_client.mget(body=mget_body)
            cur_docs = [doc for doc in response['docs'] if doc['found']]
            if cur_docs:
                log.info("%s.%s: Merging %i existing document(s).", bundle_uuid, bundle_version, len(cur_docs))
                for cur_doc in cur_docs:
                    doc_id = cur_doc['_id']
                    if is_indexer:
                        new_doc = indexable_documents[doc_id]
                    else:
                        new_doc = None
                        for itr_doc in indexable_documents:
                            if itr_doc['_id'] == doc_id:
                                new_doc = itr_doc
                        if new_doc is None:
                            raise RuntimeError("%s.%s: No existing document to delete", bundle_uuid, bundle_version)
                    cur_doc_source, new_doc_source = cur_doc['_source'], (new_doc.document_content if is_indexer else new_doc['_source'])
                    cur_bundles, new_bundles = cur_doc_source['bundles'], new_doc_source['bundles']
                    merged_bundles = self.merge(cur_bundles, new_bundles)
                    new_doc_source['bundles'] = merged_bundles
                    if is_indexer:
                        new_doc.document_version = cur_doc.get("_version", 0) + 1
                    else:
                        new_doc['_version'] = cur_doc.get("_version", 0) + 1
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
                for cur_doc in (indexable_documents.values() if is_indexer else indexable_documents):
                    doc_id = cur_doc.document_id if is_indexer else cur_doc['_id']
                    try:
                        es_client.index(index=cur_doc.document_index if is_indexer else cur_doc['_index'],
                                        doc_type=cur_doc.document_type if is_indexer else cur_doc['_type'],
                                        body=cur_doc.document_content if is_indexer else cur_doc['_source'],
                                        id=doc_id,
                                        version=cur_doc.document_version if is_indexer else cur_doc['_version'],
                                        version_type='external')
                    except ConflictError as e:
                        on_conflict(doc_id, e)
                    except ElasticsearchException as e:
                        on_error(doc_id, e)
                    else:
                        on_success(doc_id)
            else:
                actions = [dict(_op_type="index",
                                _index=doc.document_index if is_indexer else doc['_index'],
                                _type=doc.document_type if is_indexer else doc['_type'],
                                _source=doc.document_content if is_indexer else doc['_source'],
                                _id=doc.document_id if is_indexer else doc['_id'],
                                version=doc.document_version if is_indexer else doc['_version'],
                                version_type="external") for doc in (
                    indexable_documents.values() if is_indexer else indexable_documents)]
                if len(actions) < 1024:
                    log.info("%s.%s: Using streaming_bulk().", bundle_uuid, bundle_version)
                    helper = streaming_bulk
                else:
                    log.info("%s.%s: Using parallel_bulk().", bundle_uuid, bundle_version)
                    helper = parallel_bulk
                response = helper(client=es_client,
                                  actions=actions,
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
            if is_indexer:
                indexable_documents = {k: v for k, v in indexable_documents.items() if k in retry_ids}
            else:
                temp_docs = []
                for temp_doc in indexable_documents:
                    if temp_doc['_id'] in retry_ids:
                        temp_docs.append(temp_doc)
                indexable_documents = temp_docs
        if errored_documents or conflict_documents:
            log.warning('%s.%s: failures: %r', bundle_uuid, bundle_version, errored_documents)
            log.warning('%s.%s: conflicts: %r', bundle_uuid, bundle_version, conflict_documents)
            raise RuntimeError('%s.%s: Failed to index bundle. Failures: %i, conflicts: %i.' %
                               (bundle_uuid, bundle_version, len(errored_documents), len(conflict_documents)))

    @staticmethod
    def merge(cur_bundles, new_bundles):
        """
        Bundles without a match in the other list are chosen:
        >>> BaseIndexer.merge([dict(uuid=0, version=0),                        ],
        ...                   [                         dict(uuid=2, version=0)])
        [{'uuid': 2, 'version': 0}, {'uuid': 0, 'version': 0}]

        If the UUID matches, the more recent bundle version is chosen:
        >>> BaseIndexer.merge([dict(uuid=0, version=0), dict(uuid=2, version=1)],
        ...                   [dict(uuid=0, version=1), dict(uuid=2, version=0)])
        [{'uuid': 0, 'version': 1}, {'uuid': 2, 'version': 1}]

        Ties (identical UUID and version) are broken by favoring the bundle from the second argument:
        >>> BaseIndexer.merge([dict(uuid=1, version=0, x=1)],
        ...                   [dict(uuid=1, version=0, x=2)])
        [{'uuid': 1, 'version': 0, 'x': 2}]

        A more complicated case:
        >>> BaseIndexer.merge([dict(uuid=0, version=0), dict(uuid=1, version=0, x=1), dict(uuid=2, version=0)],
        ...                   [                         dict(uuid=1, version=0, x=2), dict(uuid=2, version=1)])
        [{'uuid': 1, 'version': 0, 'x': 2}, {'uuid': 2, 'version': 1}, {'uuid': 0, 'version': 0}]
        """
        cur_bundles_by_id = {cur_bundle['uuid']: cur_bundle for cur_bundle in cur_bundles}
        assert len(cur_bundles_by_id) == len(cur_bundles)
        bundles = {}
        for new_bundle in new_bundles:
            bundle_uuid = new_bundle['uuid']
            try:
                cur_bundle = cur_bundles_by_id.pop(bundle_uuid)
            except KeyError:
                bundle = new_bundle
            else:
                bundle = new_bundle if ((new_bundle['version'] >= cur_bundle['version']) or
                                        new_bundle['admin_deleted']) else cur_bundle
            assert bundles.setdefault(bundle_uuid, bundle) is bundle
        for bundle in cur_bundles_by_id.values():
            assert bundles.setdefault(bundle['uuid'], bundle) is bundle
        return list(bundles.values())

    def delete(self, dss_notification: Mapping[str, Any]) -> None:
        bundle_uuid = dss_notification['match']['bundle_uuid']
        bundle_version = dss_notification['match']['bundle_version']

        es_client = self.properties.elastic_search_client

        search_query = {
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

        current_doc = es_client.search(doc_type="doc",
                                       body=search_query)

        hits = current_doc['hits']['hits']

        updated_hits = []

        for hit in hits:
            current_doc_id = hit['_id']
            hit_doc = es_client.get(index=hit['_index'],
                                    doc_type="doc",
                                    id=current_doc_id)

            for bundle in hit_doc['_source']['bundles']:
                if bundle['version'] == bundle_version:
                    bundle['contents'] = None
                    bundle['admin_deleted'] = True

            updated_hits.append(hit_doc)

        self._update_content(bundle_uuid, bundle_version, es_client, updated_hits, is_indexer=False)
