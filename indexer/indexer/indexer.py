from abc import abstractmethod, ABCMeta
from collections import ChainMap
import json
from pprint import pprint
import re


class AbstractIndexer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def index(self, **kwargs):
        """
        this is where different indexes can be indexed
        calls extract_item, merge, load_doc
        :return: none
        """
        raise NotImplementedError(
            'users must define index to use this base class')

    @abstractmethod
    def special_fields(self, **kwargs):
        """
        special fields (bundle_uuid, bundle_type)
        :return:
        """
        raise NotImplementedError(
            'users must define special_fields to use this base class')

    @abstractmethod
    def merge(self, **kwargs):
        """
        take results of extract_item and query ES to merge into one ES file
        calls special_fields
        :return:
        """
        raise NotImplementedError(
            'users must define merge to use this base class')

    @abstractmethod
    def load_doc(self, **kwargs):
        """
        makes put request to ES
        :return:
        """
        raise NotImplementedError(
            'users must define load_doc to use this base class')

    @abstractmethod
    def create_mapping(self, **kwargs):
        """
        Creates the mapping to be used for the index in ElasticSearch
        """
        raise NotImplementedError(
            'users must define create_mapping to use this base class')

    @abstractmethod
    def load_mapping(self, **kwargs):
        """
        load mappings into ES
        :return:
        """
        raise NotImplementedError(
            'users must define load_mapping to use this base class')


class Indexer(AbstractIndexer):
    def __init__(self, metadata_files, data_files, es_client, index_name,
                 doc_type, index_settings=None, index_mapping_config=None,
                 **kwargs):
        """
        :param metadata_files: list of json metadata (list of dicts)
        :param data_files: dictionary describing non-metadata files
         (list of dicts)
        :param es_client: The elasticsearch client
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

    def index(self, *args, **kwargs):
        """
        this is where different indexes can be indexed
        calls special_fields, merge, load_doc
        :return: none
        """
        raise NotImplementedError(
            'users must define index to use this base class')

    def special_fields(self, *args, **kwargs):
        """
        special fields handler (bundle_uuid, bundle_type)
        :return:
        """
        raise NotImplementedError(
            'users must define special_fields to use this base class')

    def merge(self, **kwargs):
        """
        take results of extract_item and query ES to merge into one ES file
        calls special_fields
        :return:
        """
        raise NotImplementedError(
            'users must define merge to use this base class')

    def load_doc(self, doc_uuid, doc_contents, **kwargs):
        """
        makes put request to ES (ElasticSearch)
        :param index_name: The name of the index to load into ElasticSearch
        :param doc_type: The name of the document type in ElasticSearch
        :param doc_uuid: The uuid which will root the ElasticSearch document
        :param doc_contents: The contents that will be loaded into ES
        :return:
        """
        # Loads the index document file into elasticsearch
        self.es_client.index(index=self.index_name,
                             doc_type=self.doc_type,
                             id=doc_uuid,
                             body=doc_contents)

    def create_mapping(self, **kwargs):
        """
        To be overwritten by the class inheriting from here.
        This class should return the dictionary corresponding to the mapping
        to be loaded into ElasticSearch. It should use the index_mapping_config
        attribute as appropriate.
        """
        raise NotImplementedError(
            'users must define create_mapping to use this base class')

    def load_mapping(self, **kwargs):
        """
        create index and load mappings into ES (ElasticSearch)
        :return:
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
        # for dfile in self.data_files:
        #     file_uuid = dfiles[file_uuid]
        #     es_json = self.special_fields(dfile=dfile, metadata=metadata_files)
        #     for field in es_json:
        #         for fkey, fvalue in field.items():
        #             if fkey == 'es_uuid':
        #                 es_uuid = fvalue
        #                 es_json.remove(field)
        #     for c_key, c_value in kwargs['config']:
        #         for mfile in metadata_files:
        #             if c_key in mfile:
        #                 for c_item in c_value:
        #                     to_append = self.__extract_item(c_key, mfile)
        #                     if to_append is not None:
        #                         if isinstance(to_append, list):
        #                             # makes lists of lists into a single list
        #                             to_append = self.merge(to_append)
        #                             for item in to_append:
        #                                 # add file item to list of items to append to ES
        #                                 es_json.append(item)
        #                         else:
        #                             # add file item to list of items to append to ES
        #                             es_json.append(to_append)
        #
        #     to_append = self.__extract_item(config, metadata_files)
        #     if to_append is not None:
        #         if isinstance(to_append, list):
        #             # makes lists of lists into a single list
        #             to_append = self.merge(to_append)
        # self.load_doc(doc_contents=to_append, doc_uuid=es_uuid)

        ### CARLOS REFACTOR
        # Get the config driving indexing
        requested_entries = self.index_mapping_config['requested_entries']
        # Iterate over each file
        for _file in self.data_files.values():
            # Set the contents to an empty dictionary
            contents = {}
            # For each file iterate over the contents of the config
            for c_key, c_value in requested_entries.items():
                if c_key in self.metadata_files:
                    # Contents is a dictionary with all the present fields
                    current = self.__extract_item(c_value,
                                                  self.metadata_files[c_key],
                                                  c_key)
                # Merge two dictionaries
                print("##############   PRINTING current     #############")
                pprint(current)
                contents = {**contents, **current}
                print("##############   PRINTING contents     #############")
                pprint(contents)
            # Get the special fields added to the contents
            es_uuid = "{}:{}".format(bundle_uuid, _file['uuid'])
            print("##############   PRINTING Before Special fields     #############")
            pprint(contents)
            special_ = self.special_fields(_file,
                                           contents,
                                           bundle_uuid=bundle_uuid,
                                           bundle_version=bundle_version,
                                           es_uuid=es_uuid)
            contents = {**contents, **special_}
            # Load the current file in question
            # Ideally merge() should be called at this point
            print("##############   PRINTING TOLOAD FILE       #############")
            pprint(contents)
            self.load_doc(doc_contents=contents, doc_uuid=es_uuid)

    def __extract_item(self, c_item, _file, name, **kwargs):
        print("##############   PRINTING FILE       #############")
        pprint(_file)
        print("##############   PRINTING CONFIG ITEM   #############")
        print(c_item)
        print("##############   PRINTING NAME #############")
        print(name)
        if isinstance(c_item, dict):
            # if the config is a dictionary,
            # then need to look deeper into the file and config for the key
            es_array = []
            for key, value in c_item.items():
                # Check if the key is in the _file in question
                if key in _file:
                    # making the name that shows path taken to get to value
                    name = "{}|{}".format(name, key) if name != "" else key
                    # resursive call. Get a list of key:value pairs
                    current = [self.__extract_item(item, _file[key], name)
                               for item in value]
                    es_array.extend(current)
                    # Make the list of dictionaries into a single dictionary
            fields_dictionary = dict(ChainMap(*filter(None, es_array)))
            return fields_dictionary
        elif isinstance(c_item, list):
            es_array = [self.__extract_item(item, _file, name)
                        for item in c_item]
            fields_dictionary = dict(ChainMap(*filter(None, es_array)))
            return fields_dictionary
        elif c_item in _file:
            # if config item is in the file
            file_value = _file[c_item]
            # need to be able to handle lists
            if not isinstance(file_value, list):
                name = "{}|{}".format(name, c_item) if name != "" else c_item
                # ES does not like periods(.) use commas(,) instead
                n_replace = name.replace(".", ",")
                # return the value of key (given by config)
                return {n_replace: file_value}

    def merge(self, l):
        for el in l:
            if isinstance(el, collections.Sequence) and not isinstance(el, (
                    str, bytes)):
                yield from self.merge(el)
            else:
                yield el

    def create_mapping(self, **kwargs):
        """
        Return the mapping as a string.

        Pulls the mapping from the index_mapping_config.
        """
        # Return the es_mapping from the index_mapping_config
        mapping_config = self.index_mapping_config['es_mapping']
        return json.dumps(mapping_config)

    def __es_config(self, c_item, name):
        """
        This function is a simpler version of look_file
        The name is recursively found by going through
        the nested levels of the config file
        :param c_item: config item
        :param name: used for key in the key, value pair
        :return: name
        """
        # TODO: THE LOGIC OF THIS IS WRONG. MIGHT HAVE TO REWRITE IT
        if isinstance(c_item, dict):
            # name concatenated with config key
            for key, value in c_item.items():
                # Assign the name
                name = "{}|{}".format(name, key) if name != "" else key
                # recursively call on each item in this level of config values
                es_array = [self.__es_config(item, name) for item in value]
                # Union on all of the sets in the list
                fields_set = set.union(*es_array)
                yield fields_set # THIS RETURNS WITHOUT GOING THOROUGH ALL THE FILES
        else:
            # return name concatenated with config key
            name = "{}|{}".format(name, c_item)
            n_replace = name.replace(".", ",")
            return {n_replace}

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
        :return: a dictionary with all the special fields added.
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
        # Add empty fields as the string 'None'
        # Get all the entries that should go in ElasticSearch
        requested_entries = self.index_mapping_config['requested_entries']
        all_fields = self.__es_config(requested_entries, "")
        print("##############   PRINTING ALL_FIELDS       #############")
        pprint(all_fields)
        # Make a set out of the fields present in the data
        present_fields = set(present_fields.keys())
        empty = {field: "None" for field in all_fields - present_fields}
        # Merge the four dictionaries
        all_data = {**file_data, **extra_fields, **computed_fields, **empty}
        return all_data


class DonorIndexer(Indexer):
    pass


'''
index method calls:
    - special_fields
    - merge
    - load_mapping
        - create_mapping
    - load_doc
'''
