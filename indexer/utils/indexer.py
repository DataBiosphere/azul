# -*- coding: utf-8 -*-
"""Base indexer class.

The base indexer class provides the framework to do indexing.

The based class Indexer serves as the basis for additional indexing classes.

"""
from abc import ABC
from elasticsearch import ConnectionError, ConflictError
import logging
from time import sleep
from typing import Mapping, Any, Union

from elasticsearch import ConnectionError, ConflictError


from utils.base_config import BaseIndexProperties
from utils.downloader import MetadataDownloader

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
            es_client.indices.create(index=index_name,
                                     body=self.properties.settings,
                                     ignore=[400])
            es_client.indices.put_mapping(index=index_name,
                                          doc_type="doc",
                                          body=self.properties.mapping)
        # Call each transformer and populate their respective index
        for transformer in transformers:
            es_documents = transformer.create_documents(metadata, data, bundle_uuid, bundle_version)
            for es_document in es_documents:
                retries = 3
                while True:
                    existing = es_client.get(index=es_document.document_index,
                                             doc_type=es_document.document_type,
                                             id=es_document.document_id,
                                             ignore=[404])
                    # replace with an empty dictionary if no existing doc
                    existing_source = existing.get("_source", {})
                    updated_version = existing.get("_version", 0) + 1
                    new_content = es_document.document_content
                    updated_document = self.merge(new_content, existing_source)
                    es_document.document_content = updated_document
                    try:
                        es_client.index(index=es_document.document_index,
                                        doc_type=es_document.document_type,
                                        id=es_document.document_id,
                                        body=es_document.document_content,
                                        version=updated_version,
                                        version_type="external")
                        break
                    except ConflictError as er:
                        module_logger.info(
                            "There was a version conflict... retrying")
                        module_logger.debug(er.info)
                    except ConnectionError as er:
                        module_logger.info("There was a connection error")
                        module_logger.info("{} retries left".format(retries))
                        if retries > 0:
                            retries -= 1
                            sleep(retries)
                        else:
                            module_logger.error("Out of retries. There is a connection problem.")
                            module_logger.error(er.error)
                            module_logger.error(er.info)
                            raise er

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
