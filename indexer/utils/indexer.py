# -*- coding: utf-8 -*-
"""Base indexer class.

The base indexer class provides the framework to do indexing.

The based class Indexer serves as the basis for additional indexing classes.

"""
import collections
from copy import deepcopy
from abc import ABC, abstractmethod
from utils.downloader import MetadataDownloader
from functools import reduce
import logging
import json
import re
from utils.base_config import IndexProperties
from typing import Type, Mapping, Iterable, Any

# create logger
module_logger = logging.getLogger(__name__)


class Indexer(ABC):

    def __init__(self, metadata_files: dict, data_files: dict, properties: IndexProperties) -> None:
        self.metadata_files = metadata_files
        self.data_files = data_files
        self.properties = properties

    def index(self,
              blue_box_notification: Mapping[str, Any],
              replica: str) -> None:
        # Calls extract, transform, merge, and load
        bundle_uuid = blue_box_notification['match']['bundle_uuid']
        bundle_version = blue_box_notification['match']['bundle_version']
        metadata, data = MetadataDownloader(self.properties.dss_url)
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
        for transformer in transformers:
            es_documents = transformer.create_documents(metadata, data, bundle_uuid, bundle_version)
            for es_document in es_documents:

                # TODO: Implement merging strategy by using versioning
                self.merge(es_document)

                es_client.index(index=es_document.document_index,
                                doc_type=es_document.document_type,
                                id=es_document.document_id,
                                body=es_document.document_content)
            pass

    def extract(self):
        metadata, data = MetadataDownloader(self.properties.dss_url)
        return metadata, data

    def transform(self):
        pass

    def merge(self):
        pass

    def load(self):
        pass










class OLDIndexer(ABC):
    """Indexer class to help indexing operation.

    The base class Indexer serves as the basis to extend other types
    of transformative indexes. It codes a lot of the boiler plate code
    regarding communication with ElasticSearch, so that users of this
    base class only need to focus on their indexing algorithm. Users
    may optionally overwrite all of the methods should they need to use
    other technologies besides ElasticSearch.

    The index() method is the main method for the Indexer class. Users
    should ideally only overwrite index(), special_fields() and merge().
    index() should call special_fields() first and then merge(). Users
    can define their own extra functions to help with indexing as needed.

    """

    def __init__(self, metadata_files, data_files, es_client, index_name,
                 doc_type, index_settings=None, index_mapping_config=None,
                 **kwargs):
        """
        Create an instance of the Indexing class.

        The constructor creates an Indexer instance, with various
        parameters that are needed when performing the indexing operation.
        It requires a dictionary of the metadata_files, the data_files,
        an ElasticSearch(ES) client object to communicate with some
        ES instance, the index_settings, as well as a mapping config
        file that can be used by the indexer for indexing operation and
        actual ElasticSearch mapping. Any extra kwargs are set as
        attributes to the instance.

        :param metadata_files: dictionary of json metadata (dict of dicts)
        :param data_files: dictionary describing non-metadata files
         (dict of dicts)
        :param es_client: The elasticsearch client
        :param index_name: The name of the index to put the documents.
        :param doc_type: The document type to put the document under in ES.
        :param index_settings: Any special settings for the ES index.
        :param index_mapping_config: The indexer config file.
        """
        # Set main arguments
        self.metadata_files = metadata_files
        self.data_files = data_files
        self.es_client = es_client
        self.index_name = index_name
        self.doc_type = doc_type
        self.index_settings = index_settings
        self.index_mapping_config = index_mapping_config
        # Set logger
        self.logger = logging.getLogger('chalicelib.indexer.Indexer')
        # Set all kwargs as instance attributes
        for key, value in kwargs.items():
            setattr(self, key, value)
        super().__init__()

    def index(self, bundle_uuid, bundle_version, *args, **kwargs):
        """
        Indexes the data files.

        Triggers the actual indexing process

        :param bundle_uuid: The bundle_uuid of the bundle that will be indexed.
        :param bundle_version: The bundle of the version.
        """
        raise NotImplementedError(
            'users must define index to use this base class')

    def special_fields(self, *args, **kwargs):
        """
        Add any special fields that may be missing.

        Gets any special field that may not be available directly from the
        metadata.

        :return: a dictionary of all the special fields to be added.
        """
        raise NotImplementedError(
            'users must define special_fields to use this base class')

    def merge(self, doc_contents, **kwargs):
        """
        Merge the document with the contents in ElasticSearch.

        merge() should take the results of get_item() and harmonize it
        with whatever is present in ElasticSearch to avoid blind overwritting
        of documents. Users should implement their own protocol.

        :param doc_contents: Current document to be indexed.
        :return: The harmonized document which can overwrite existing entry.
        """
        raise NotImplementedError(
            'users must define merge to use this base class')

    def load_doc(self, doc_uuid, doc_contents, * args, **kwargs):
        """
        Load a document into ElasticSearch.

        Load 'doc_contents' as a document in ElasticSearch (ES) with a
        uuid 'doc_uuid'.

        :param index_name: The name of the index to load into ElasticSearch.
        :param doc_type: The name of the document type in ElasticSearch.
        :param doc_uuid: The uuid which will root the ElasticSearch document.
        :param doc_contents: The contents that will be loaded into ES.
        :return:
        """
        # Loads the index document file into elasticsearch
        self.es_client.index(index=self.index_name,
                             doc_type=self.doc_type,
                             id=doc_uuid,
                             body=doc_contents)

    def create_mapping(self, *args, **kwargs):
        """
        Create the ElasticSearch mapping.

        To be overwritten by the class inheriting from here.
        This class should return the dictionary corresponding to the mapping
        to be loaded into ElasticSearch. It should use the index_mapping_config
        attribute as appropriate.

        :return: Dictionary describing the ElaticSearch mapping.
        """
        raise NotImplementedError(
            'users must define create_mapping to use this base class')

    def load_mapping(self, *args, **kwargs):
        """
        Load the mapping into Elasticsearch.

        This method is responsible for loading the mapping into Elasticsearch.
        It first creates an index using the instance's attributes, and
        then loads the mapping by also calling create_mapping(), which
        will create the object describing the mapping.
        """
        # Creates the index
        self.es_client.indices.create(index=self.index_name,
                                      body=self.index_settings,
                                      ignore=[400])
        # Loads mappings for the index
        self.es_client.put_mapping(index=self.index_name,
                                   doc_type=self.doc_type,
                                   body=self.create_mapping())

    def get_schema(self, d, path=("core", "schema_version")):
        """
        Obtain the schema version.

        This helper function gets the schema version deep in a dictionary.
        See: stackoverflow.com/questions/40468932/pass-nested-dictionary-
        location-as-parameter-in-python
        :param d: Metadata dictionary.
        :param path: Path to the schema version.
        :return: path contents
        """
        return reduce(dict.get, path, d)

    def get_item(self, c_item, _file):
        """
        Get the c_item in _file or a None string if field is missing.

        This recursive method serves to either get all the formatted
        strings that make the config (c_item). Each leaf on the c_item must
        be a string containing an asterisk, where the left hand side is the
        name under the extraction files, and the right hand side is the keyword
        we want to associate the contents with. If a field is not found, the
        function will continue until it reaches the leaf and return it with a
        "None string"

        :param c_item: config item.
        :param _file: the object to extract contents from. Defaults to 'None'.
        :return: name, item
        """
        if isinstance(c_item, dict):
            # Iterate over the contents of the dictionary
            for key, value in c_item.items():
                # Extract the contents of the _file list
                if isinstance(_file, list):
                    # Iterate over each object in the list
                    for el in _file:
                        yield from self.get_item(c_item, el)
                # If the _file is not a list
                else:
                    # Iterate over each requested object in the current config
                    # path
                    for item in value:
                        # If requested item present, continue to next level
                        if key in _file:
                            child = _file[key]
                            yield from self.get_item(item, child)
                        # Otherwise, just continue with empty dictionary;
                        # This will return None on all missing fields
                        else:
                            yield from self.get_item(item, {})
        else:
            # Parse the requested field into actual name and desired name
            content, name = tuple(c_item.split('*'))
            # If it is a list, please handle
            if isinstance(_file, list):
                # Iterate over each field in _file
                for el in _file:
                    yield from self.get_item(c_item, el)
            # If requested field is present return it
            elif content in _file:
                yield name, _file[content]
            # Return None string if there is no field present
            else:
                yield name, "None"
