# -*- coding: utf-8 -*-
"""Base indexer class.

The base indexer class provides the framework to do indexing.

The based class Indexer serves as the basis for additional indexing classes.

"""
from abc import ABC
from collections import defaultdict
import logging
from typing import Mapping, Any, Union, Iterable

from elasticsearch.helpers import parallel_bulk

from utils.base_config import BaseIndexProperties
from utils.downloader import MetadataDownloader
from utils.transformer import ElasticSearchDocument

# create logger
module_logger = logging.getLogger(__name__)


class BaseIndexer(ABC):

    def __init__(self, properties: BaseIndexProperties) -> None:
        self.properties = properties

    def index(self, dss_notification: Mapping[str, Any]) -> None:
        # Calls extract, transform, merge, and load
        bundle_uuid = dss_notification['match']['bundle_uuid']
        bundle_version = dss_notification['match']['bundle_version']
        metadata_downloader = MetadataDownloader(self.properties.dss_url)
        metadata, data = metadata_downloader.extract_bundle(dss_notification)
        transformers = self.properties.transformers
        es_client = self.properties.elastic_search_client
        # Create and populate the Indexes
        for index_name in self.properties.index_names:
            # Create the index and apply the mapping at the same time.
            es_client.indices.create(index=index_name,
                                     body=self.properties.settings,
                                     ignore=[400])
            es_client.indices.put_mapping(index=index_name,
                                          doc_type="doc",
                                          body=self.properties.mapping)
        # Keep track of documents to index and errored out documents
        indexable_documents = {}
        errored_documents = defaultdict(int)
        conflict_documents = defaultdict(int)
        # Collect the documents to be indexed
        for transformer in transformers:
            es_documents = transformer.create_documents(metadata,
                                                        data,
                                                        bundle_uuid,
                                                        bundle_version)
            for es_document in es_documents:
                indexable_documents[es_document.document_id] = es_document

        def create_mget_body(documents_dictionary: Mapping[str, ElasticSearchDocument]) -> \
                Mapping[str, Iterable]:
            docs = []
            for d in documents_dictionary.values():
                _doc = {
                    "_index": d.document_index,
                    "_type": d.document_type,
                    "_id": d.document_id
                }
                docs.append(_doc)
            _mget_body = {"docs": docs}
            return _mget_body

        def create_bulk_body(
                documents_dictionary: Mapping[str, ElasticSearchDocument]) -> Iterable[dict]:
            for d in documents_dictionary.values():
                _action = {
                    "_index": d.document_index,
                    "_type": d.document_type,
                    "_id": d.document_id,
                    "_op_type": "index",
                    "version": d.document_version,
                    "version_type": "external",
                    "_source": d.document_content
                }
                yield _action

        while indexable_documents:
            # First query for any existing documents
            mget_body = create_mget_body(indexable_documents)
            existing_docs = es_client.mget(body=mget_body)
            # Merge any existing document
            for doc in existing_docs["docs"]:
                if doc["found"]:
                    es_document = indexable_documents[doc["_id"]]
                    new_doc = es_document.document_content
                    # This should update 'indexable_documents' by reference
                    updated_document = self.merge(new_doc, doc["_source"])
                    updated_version = doc.get("_version", 0) + 1
                    es_document.document_content = updated_document
                    es_document.document_version = updated_version

            retry_ids = set()
            # Process each result
            for success, info in parallel_bulk(es_client,
                                               create_bulk_body(
                                                   indexable_documents),
                                               raise_on_error=False,
                                               max_chunk_bytes=10485760):
                # We don't care about successes
                if success:
                    continue
                retry_ids.add(info["index"]["_id"])
                if info["index"]['status'] == 409:
                    module_logger.warning(
                        "There was a conflict error on document %s, index %s",
                        info["index"]["_id"], info["index"]["_index"])
                    module_logger.warning("Indexer will try to re-index")
                    conflict_documents[info["index"]["_id"]] += 1

                else:
                    # Limit other errors to 3 times.
                    if errored_documents[info["index"]["_id"]] < 3:
                        errored_documents[info["index"]["_id"]] += 1
                        module_logger.warning(
                            "This isn't a conflict error. %i attempt",
                            errored_documents[
                                info["index"]["_id"]])
                        module_logger.warning(
                            "There was an error on document %s, index %s",
                            info["index"]["_id"], info["index"]["_index"])
                    else:
                        module_logger.error("i% tries on id %s",
                                            errored_documents[
                                                info["index"]["_id"]],
                                            info["index"]["_id"])
                        module_logger.error(
                            "There was an error on document %s, index %s",
                            info["index"]["_id"],
                            info["index"]["_index"])

            # Update the indexable documents
            indexable_documents = {k: v for k, v in indexable_documents.items()
                                   if k in retry_ids}
        module_logger.debug("Errored documents: %s", str(errored_documents))
        module_logger.debug("Conflict documents: %s", str(conflict_documents))

    @staticmethod
    def merge(new_document: Mapping[str, Any],
              stored_document: Union[bool, Any]) -> Mapping[str, Any]:
        # This is a new record
        if not stored_document:
            return new_document
        # new_bundle should be an array of length one since the new document
        # contains data for only one bundle
        new_bundle = new_document["bundles"][0]
        updated_bundles = []
        for bundle in stored_document["bundles"]:
            if bundle["uuid"] == new_bundle["uuid"]:
                latest_bundle = max(bundle,
                                    new_bundle,
                                    key=lambda x: x["version"])
                updated_bundles.append(latest_bundle)
            else:
                updated_bundles.append(bundle)
        # Update the document
        new_document["bundles"] = updated_bundles
        return new_document
