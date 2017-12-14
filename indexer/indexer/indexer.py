# -*- coding: utf-8 -*-
"""Indexer module to help with indexing.

The indexer module contains classes to create different types of Indexes.

The based class Indexer serves as the basis for additional indexing classes
in this module.

"""
import json
import re


class Indexer(object):
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

        :param metadata_files: dictionary of json metadata (list of dicts)
        :param data_files: dictionary describing non-metadata files
         (list of dicts)
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
        # Set all kwargs as instance attributes
        for key, value in kwargs.items():
            setattr(self, key, value)

    def index(self, bundle_uuid, bundle_version, *args, **kwargs):
        """Indexes the data files.

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


class FileIndexer(Indexer):
    """Create a file oriented index.

    FileIndexer makes use of its index() function to perform indexing
    of the data files that are presented to the class when creating an
    instance.

    End for now.

    """

    def index(self, bundle_uuid, bundle_version, **kwargs):
        """Indexes the data files.

        Triggers the actual indexing process

        :param bundle_uuid: The bundle_uuid of the bundle that will be indexed.
        :param bundle_version: The bundle of the version.
        """
        # Get the config driving indexing (e.g. the required entries)
        req_entries = self.index_mapping_config['requested_entries']
        # Iterate over each file
        for _file in self.data_files.values():
            # List the arguments for clarity
            args = [req_entries, "", self.metadata_files]
            # Get all the contents from the entries requested in the config
            contents = {key: value for key, value in self.__get_item(*args)}
            # Get the elasticsearch uuid for this particular data file
            es_uuid = "{}:{}".format(bundle_uuid, _file['uuid'])
            # Get the special fields added to the contents
            special_ = self.special_fields(_file,
                                           contents,
                                           bundle_uuid=bundle_uuid,
                                           bundle_version=bundle_version,
                                           es_uuid=es_uuid)
            contents = {**contents, **special_}
            # Load the current file in question
            # Ideally merge() should be called at this point
            self.load_doc(doc_contents=contents, doc_uuid=es_uuid)

    def __get_item(self, c_item, name, _file=None):
        """
        Get the c_item in _file or all the strings in the c_item.

        This recursive method serves to either get all the formatted
        strings that make the config (c_item). If '_file' is not None,
        then you get a tuple containing the string representing the path
        in the metadata and the value of the metadata at that path.
        This is a generator function.

        :param c_item: config item.
        :param name: name representing the path on the metadata
        :param _file: the object to extract contents from. Defaults to None.
        :return: name or name, item
        """
        if isinstance(c_item, dict):
            # Iterate over the contents of the dictionary
            for key, value in c_item.items():
                # Create the new name
                new_name = "{}|{}".format(name, key) if name != "" else key
                for item in value:
                    # Recursive call on each level
                    if _file is None or key in _file:
                        son = None if _file is None else _file[key]
                        yield from self.__get_item(item, new_name, _file=son)
        else:
            # Return name concatenated with config key
            name = "{}|{}".format(name, c_item).replace(".", ",")
            if _file is not None and c_item in _file:
                # If the file exists and contains the item in question
                yield name, _file[c_item]
            elif _file is None:
                # If we only want the string of the name
                yield name

    def merge(self, doc_contents):
        """
        Merge the document with the contents in ElasticSearch.

        merge() should take the results of get_item() and harmonize it
        with whatever is present in ElasticSearch to avoid blind overwritting
        of documents. Users should implement their own protocol.

        :param doc_contents: Current document to be indexed.
        :return: The harmonized document which can overwrite existing entry.
        """
        # Assuming a file is never really changed, there is no reason
        # for merge here.
        pass

    def create_mapping(self, **kwargs):
        """
        Return the mapping as a string.

        Pulls the mapping from the index_mapping_config.
        """
        # Return the es_mapping from the index_mapping_config
        mapping_config = self.index_mapping_config['es_mapping']
        return json.dumps(mapping_config)

    def __get_format(self, file_name):
        """
        HACK This is to get the file format while we get a file format.

        We need to get the file format from the Blue Box team somehow.
        This is a small parsing hack while we get a response.

        :param file_name: A string containing the the file name with extension
        """
        # Get everything after the period
        file_format = '.'.join(file_name.split('.')[1:])
        file_format = file_format if file_format != '' else 'Unknown'
        return file_format

    def __get_bundle_type(self, file_extension):
        """
        HACK This is to get the bundle type while we wait for Blue Box team.

        We need to get the bundle type from the Blue Box team somehow.
        This is a small parsing hack while we get a response from them.

        :param file_name: A string containing the the file extension
        """
        # A series of if else statements to figure out the bundle type
        # HACK
        if 'analysis.json' in self.metadata_files:
            bundle_type = 'Analysis'
        elif re.search(r'(tiff)', str(file_extension)):
            bundle_type = 'Imaging'
        elif re.search(r'(fastq.gz)', str(file_extension)):
            bundle_type = 'scRNA-Seq Upload'
        else:
            bundle_type = 'Unknown'
        return bundle_type

    def special_fields(self, data_file, present_fields, **kwargs):
        """
        Add any special fields that may be missing.

        Gets any special field that may not be available directly from the
        metadata.

        :param data_file: a dictionary describing the file in question.
        :param present_fields: dictionary with available fields.
        :param kwargs: any additional entries you want to include.
        :return: a dictionary of all the special fields to be added.
        """
        # Get all the fields from a single file into a dictionary
        file_data = {'file_{}'.format(key): value
                     for key, value in data_file.items()}
        # Add extra field that should go in here (e.g. es_uuid, bundle_uuid)
        extra_fields = {key: value for key, value in kwargs.items()}
        # Get the file format
        file_format = self.__get_format(file_data['file_name'])
        # Create a dictionary with the file fomrat and the bundle type
        computed_fields = {"file_format": file_format,
                           "bundle_type": self.__get_bundle_type(file_format)}
        # Get all the requested entries that should go in ElasticSearch
        req_entries = self.index_mapping_config['requested_entries']
        all_fields = {entry for entry in self.__get_item(req_entries, "")}
        # Make a set out of the fields present in the data
        present_fields = set(present_fields.keys())
        # Add empty fields as the string 'None'
        empty = {field: "None" for field in all_fields - present_fields}
        # Merge the four dictionaries
        all_data = {**file_data, **extra_fields, **computed_fields, **empty}
        return all_data


class DonorIndexer(Indexer):
    """index method calls:.

    - special_fields
    - merge
    - load_mapping
        - create_mapping
    - load_doc

    """

    pass
