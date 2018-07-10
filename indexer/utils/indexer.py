# -*- coding: utf-8 -*-
"""Base indexer class.

The base indexer class provides the framework to do indexing.

The based class Indexer serves as the basis for additional indexing classes.

"""
from abc import ABC
from collections import defaultdict
import logging
from typing import Mapping, Any, MutableMapping

from elasticsearch import ConflictError, ElasticsearchException
from elasticsearch.helpers import parallel_bulk, streaming_bulk

from utils.base_config import BaseIndexProperties
from utils.downloader import MetadataDownloader

log = logging.getLogger(__name__)


class BaseIndexer(ABC):

    def __init__(self, properties: BaseIndexProperties) -> None:
        self.properties = properties

    def index(self, dss_notification: Mapping[str, Any]) -> None:
        # Calls extract, transform, merge, and load
        bundle_uuid = dss_notification['match']['bundle_uuid']
        bundle_version = dss_notification['match']['bundle_version']
        metadata_downloader = MetadataDownloader(self.properties.dss_url)
        metadata, data = metadata_downloader.extract_bundle(dss_notification)

        es_client = self.properties.elastic_search_client

        # Create indices and populate mappings
        for index_name in self.properties.index_names:
            # FIXME: explain why 400 is ignored
            es_client.indices.create(index=index_name, body=self.properties.settings, ignore=[400])
            es_client.indices.put_mapping(index=index_name, doc_type="doc", body=self.properties.mapping)

        errored_documents = defaultdict(int)
        conflict_documents = defaultdict(int)

        # Collect the initial set of documents to be indexed
        indexable_documents = {}
        for transformer in self.properties.transformers:
            es_documents = transformer.create_documents(metadata,
                                                        data,
                                                        bundle_uuid,
                                                        bundle_version)
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
                for doc in cur_docs:
                    new_doc = indexable_documents[doc["_id"]]
                    self.merge(new_doc.document_content, doc["_source"])
                    new_doc.document_version = doc.get("_version", 0) + 1
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
                for doc in indexable_documents.values():
                    doc_id = doc.document_id
                    try:
                        es_client.index(index=doc.document_index,
                                        doc_type=doc.document_type,
                                        body=doc.document_content,
                                        id=doc_id,
                                        version=doc.document_version,
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
                                _source=doc.document_content,
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

    @staticmethod
    def merge(new_doc: MutableMapping[str, Any], cur_doc: Mapping[str, Any]):
        # FIXME: I think this condition is redundant
        if cur_doc:
            # The new document contains data for one bundle only
            new_bundle = new_doc["bundles"][0]
            updated_bundles = []
            bundle_found = False
            for bundle in cur_doc["bundles"]:
                if bundle["uuid"] == new_bundle["uuid"]:
                    latest_bundle = max(bundle,
                                        new_bundle,
                                        key=lambda x: x["version"])
                    updated_bundles.append(latest_bundle)
                    bundle_found = True
                else:
                    updated_bundles.append(bundle)
            if not bundle_found:
                updated_bundles.append(new_bundle)
            new_doc["bundles"] = updated_bundles
