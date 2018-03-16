# -*- coding: utf-8 -*-
"""Indexer module to help with indexing.

The indexer module contains classes to create different types of Indexes.

The based class Indexer serves as the basis for additional indexing classes
in this module.

"""
import collections
from copy import deepcopy
from functools import reduce
import logging
import json
import re

# create logger
module_logger = logging.getLogger('chalicelib.indexer')


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
        # Set logger
        self.logger = logging.getLogger('chalicelib.indexer.Indexer')
        # Set all kwargs as instance attributes
        for key, value in kwargs.items():
            setattr(self, key, value)

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
        versions = self.index_mapping_config['requested_entries']
        # Iterate over each file
        for _file in self.data_files.values():
            # Create a new dictionary
            d = collections.defaultdict(list)
            # Get schema version for one of the metadata files
            metadata_file = next(iter(self.metadata_files.values()))
            schema_v = self.get_schema(metadata_file)
            # Get the config for the current schema version
            req_entries = versions[schema_v]
            # Put together the list of arguments
            args = [req_entries,  self.metadata_files]
            # For each tuple returned from the metadata file, update
            # the dictionary
            for key, value in self.get_item(*args):
                d[key].append(value)
            # Get the elasticsearch uuid for this particular data file
            es_uuid = "{}:{}".format(bundle_uuid, _file['uuid'])
            # Get the special fields added to the contents
            special_ = self.special_fields(_file,
                                           bundle_uuid=bundle_uuid,
                                           bundle_version=bundle_version,
                                           es_uuid=es_uuid)
            d['bundles'] = [
                {
                    "uuid": special_.pop('bundle_uuid', None),
                    "version": special_.pop('bundle_version', None),
                    "type": special_.pop('bundle_type', None)
                }
            ]
            samples_list = []
            for i, sample_id in enumerate(d['sampleIds']):
                sample = {
                    "sampleId": sample_id,
                    "sampleBodyPart": d["sampleBodyPart"][i],
                    "sampleSpecies": d["sampleSpecies"][i],
                    "sampleNcbiTaxonIds": d["sampleNcbiTaxonIds"][i]
                }
                samples_list.append(sample)
            # Remove superfluous keywords
            d.pop('sampleIds', None)
            d.pop("sampleBodyPart", None)
            d.pop("sampleSpecies", None)
            d.pop("sampleNcbiTaxonIds", None)
            samples = {"samples": samples_list}
            contents = {**d, **special_, **samples}
            self.logger.debug("PRINTING FILE INDEX DOCUMENT:\n")
            self.logger.debug(contents)
            # Load the current file in question
            # Ideally merge() should be called at this point
            self.load_doc(doc_contents=contents, doc_uuid=es_uuid)

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

    def special_fields(self, data_file, **kwargs):
        """
        Add any special fields that may be missing.

        Gets any special field that may not be available directly from the
        metadata.

        :param data_file: a dictionary describing the file in question.
        :param kwargs: any additional entries you want to include.
        :return: a dictionary of all the special fields to be added.
        """
        # Create a list of files
        file_data = {"files": data_file}
        # Add extra field that should go in here (e.g. es_uuid, bundle_uuid)
        extra_fields = {key: value for key, value in kwargs.items()}
        # Get the file format
        file_format = self.__get_format(file_data['files']['name'])
        file_data["files"]["format"] = file_format
        # Create a dictionary with the file fomrat and the bundle type
        computed_fields = {"bundle_type": self.__get_bundle_type(file_format)}
        # Merge the four dictionaries
        all_data = {**file_data, **extra_fields, **computed_fields}
        return all_data

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

        :param file_extension: A string containing the the file extension
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


class BundleOrientedIndexer(Indexer):
    """Create a Project oriented index.

    ProjectOrientedIndexer makes use of its index() function to perform
    indexing of the bundle files that are presented to the class when creating
    an instance.

    End for now.

    """

    def index(self, bundle_uuid, bundle_version, **kwargs):
        """Indexes the bundle into a sample oriented indexer.

        Triggers the actual indexing process

        :param bundle_uuid: The bundle_uuid of the bundle that will be indexed.
        :param bundle_version: The bundle of the version.
        """
        # Get the config driving indexing (e.g. the required entries)
        versions = self.index_mapping_config['requested_entries']
        # Get schema version for one of the metadata files
        metadata_file = next(iter(self.metadata_files.values()))
        schema_v = self.get_schema(metadata_file)
        # Get the config for the current schema version
        req_entries = versions[schema_v]
        # Put together the list of arguments
        args = [req_entries,  self.metadata_files]
        # Create a new dictionary
        d = collections.defaultdict(list)
        # For each tuple returned from the metadata file, update
        # the dictionary
        for key, value in self.get_item(*args):
            d[key].append(value)
        # Add the format and bundle type to each file
        bundle_type = None
        for file_name, description in self.data_files.items():
            self.data_files[file_name]['format'] = self.__get_format(file_name)
            # Get the bundle type
            bundle_type = self.__get_bundle_type(file_name)
        # Assign the bundle
        d['bundles'] = [
            {
                "uuid": bundle_uuid,
                "version": bundle_version,
                "type": bundle_type
            }
        ]
        # Assign the files
        d["files"] = list(self.data_files.values())
        # Rearrange samples
        samples_list = []
        for i, sample_id in enumerate(d['sampleIds']):
            sample = {
                "sampleId": sample_id,
                "sampleBodyPart": d["sampleBodyPart"][i],
                "sampleSpecies": d["sampleSpecies"][i],
                "sampleNcbiTaxonIds": d["sampleNcbiTaxonIds"][i]
            }
            samples_list.append(sample)
        # Remove superfluous keywords
        d.pop('sampleIds', None)
        d.pop("sampleBodyPart", None)
        d.pop("sampleSpecies", None)
        d.pop("sampleNcbiTaxonIds", None)
        # Assign the sample list
        d["samples"] = samples_list
        # Iterate over each sample
        for bundle in d['bundles']:
            new_bundle = deepcopy(d)
            new_bundle['bundles'] = bundle
            es_uuid = bundle['uuid']
            new_bundle['es_uuid'] = es_uuid
            contents = self.merge(new_bundle, es_uuid)
            self.logger.debug("PRINTING BUNDLE INDEX DOCUMENT:\n")
            self.logger.debug(contents)
            self.load_doc(doc_contents=contents, doc_uuid=es_uuid)

    def __get_value(self, d, path):
        """
        Obtain a value deep in a Dictionary.

        This helper function serves to get a value deep in a dictionary.
        See: stackoverflow.com/questions/40468932/pass-nested-dictionary-
        location-as-parameter-in-python
        """
        return reduce(dict.get, path, d)

    def __merge_lists(self, new, current, path):
        """
        Merge two lists with unique ids.

        This function helps by merging two litst of dictionaries and making
        sure that the field in question only appears once in the list.
        """
        merged_dict = {}
        both_lists = current + new
        for item in both_lists:
            _id = self.__get_value(item, path)
            # TODO: I don't like this approach, but this is a first pass.
            # We need to redo so that it is less than O(n)
            # This first pass approach ensures new entries overwrite old ones.
            merged_dict[_id] = item
        merged = list(merged_dict.values())
        return merged

    def merge(self, doc_contents, _id):
        """
        Merge the current doc_contents.

        This method calls elasticsearch and merges the documents present
        in there with the ones from doc_contents
        """
        existing = self.es_client.get(index=self.index_name,
                                      doc_type=self.doc_type,
                                      id=_id,
                                      ignore=[404])
        if '_source' in existing:
            # Pop out the samples field
            bundle = existing['_source'].pop('bundles')
            # Merge all the fields:
            # do for loop, use the collections thing to update a new dict
            d = collections.defaultdict(list)
            d.update(existing['_source'])
            for key, value in doc_contents.items():
                if isinstance(d[key], list):
                    d[key].extend(value)
                else:
                    d[key] = value
            d['bundles'] = bundle
            return d
        else:
            return doc_contents

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

        :param file_extension: A string containing the the file extension
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


class AssayOrientedIndexer(Indexer):
    """Create a Project oriented index.

    ProjectOrientedIndexer makes use of its index() function to perform
    indexing of the bundle files that are presented to the class when creating
    an instance.

    End for now.

    """

    def index(self, bundle_uuid, bundle_version, **kwargs):
        """Indexes the bundle into a sample oriented indexer.

        Triggers the actual indexing process

        :param bundle_uuid: The bundle_uuid of the bundle that will be indexed.
        :param bundle_version: The bundle of the version.
        """
        # Get the config driving indexing (e.g. the required entries)
        versions = self.index_mapping_config['requested_entries']
        # Get schema version for one of the metadata files
        metadata_file = next(iter(self.metadata_files.values()))
        schema_v = self.get_schema(metadata_file)
        # Get the config for the current schema version
        req_entries = versions[schema_v]
        # Put together the list of arguments
        args = [req_entries,  self.metadata_files]
        # Create a new dictionary
        d = collections.defaultdict(list)
        # For each tuple returned from the metadata file, update
        # the dictionary
        for key, value in self.get_item(*args):
            d[key].append(value)
        # Add the format and bundle type to each file
        bundle_type = None
        for file_name, description in self.data_files.items():
            self.data_files[file_name]['format'] = self.__get_format(file_name)
            # Get the bundle type
            bundle_type = self.__get_bundle_type(file_name)
        # Assign the bundles
        d['bundles'] = [
            {
                "uuid": bundle_uuid,
                "version": bundle_version,
                "type": bundle_type
            }
        ]
        # Assign the files
        d["files"] = list(self.data_files.values())
        # Rearrange samples
        samples_list = []
        for i, sample_id in enumerate(d['sampleIds']):
            sample = {
                "sampleId": sample_id,
                "sampleBodyPart": d["sampleBodyPart"][i],
                "sampleSpecies": d["sampleSpecies"][i],
                "sampleNcbiTaxonIds": d["sampleNcbiTaxonIds"][i]
            }
            samples_list.append(sample)
        # Remove superfluous keywords
        d.pop('sampleIds', None)
        d.pop("sampleBodyPart", None)
        d.pop("sampleSpecies", None)
        d.pop("sampleNcbiTaxonIds", None)
        # Assign the sample list
        d["samples"] = samples_list
        # Iterate over each sample
        for assay in d['assayId']:
            new_assay = deepcopy(d)
            new_assay['assayId'] = assay
            es_uuid = assay
            new_assay['es_uuid'] = es_uuid
            contents = self.merge(new_assay, es_uuid)
            self.logger.debug("PRINTING ASSAY INDEX DOCUMENT:\n")
            self.logger.debug(contents)
            self.load_doc(doc_contents=contents, doc_uuid=es_uuid)

    def __get_value(self, d, path):
        """
        Obtain a value deep in a Dictionary.

        This helper function serves to get a value deep in a dictionary.
        See: stackoverflow.com/questions/40468932/pass-nested-dictionary-
        location-as-parameter-in-python
        """
        return reduce(dict.get, path, d)

    def __merge_lists(self, new, current, path):
        """
        Merge two lists with unique ids.

        This function helps by merging two litst of dictionaries and making
        sure that the field in question only appears once in the list.
        """
        merged_dict = {}
        both_lists = current + new
        for item in both_lists:
            _id = self.__get_value(item, path)
            # TODO: I don't like this approach, but this is a first pass.
            # We need to redo so that it is less than O(n)
            # This first pass approach ensures new entries overwrite old ones.
            merged_dict[_id] = item
        merged = list(merged_dict.values())
        return merged

    def merge(self, doc_contents, _id):
        """
        Merge the current doc_contents.

        This method calls elasticsearch and merges the documents present
        in there with the ones from doc_contents
        """
        existing = self.es_client.get(index=self.index_name,
                                      doc_type=self.doc_type,
                                      id=_id,
                                      ignore=[404])
        if '_source' in existing:
            # Pop out the samples field
            assay = existing['_source'].pop('assayId')
            # Merge all the fields:
            # do for loop, use the collections thing to update a new dict
            d = collections.defaultdict(list)
            d.update(existing['_source'])
            for key, value in doc_contents.items():
                if isinstance(d[key], list):
                    d[key].extend(value)
                else:
                    d[key] = value
            d['assayId'] = assay
            return d
        else:
            return doc_contents

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

        :param file_extension: A string containing the the file extension
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


class SampleOrientedIndexer(Indexer):
    """Create a Sample oriented index.

    SampleOrientedIndexer makes use of its index() function to perform
    indexing of the bundle files that are presented to the class when creating
    an instance.

    End for now.

    """

    def index(self, bundle_uuid, bundle_version, **kwargs):
        """Indexes the bundle into a sample oriented indexer.

        Triggers the actual indexing process

        :param bundle_uuid: The bundle_uuid of the bundle that will be indexed.
        :param bundle_version: The bundle of the version.
        """
        # Get the config driving indexing (e.g. the required entries)
        versions = self.index_mapping_config['requested_entries']
        # Get schema version for one of the metadata files
        metadata_file = next(iter(self.metadata_files.values()))
        schema_v = self.get_schema(metadata_file)
        # Get the config for the current schema version
        req_entries = versions[schema_v]
        # Put together the list of arguments
        args = [req_entries,  self.metadata_files]
        # Create a new dictionary
        d = collections.defaultdict(list)
        # For each tuple returned from the metadata file, update
        # the dictionary
        for key, value in self.get_item(*args):
            d[key].append(value)
        # Add the format and bundle type to each file
        bundle_type = None
        for file_name, description in self.data_files.items():
            self.data_files[file_name]['format'] = self.__get_format(file_name)
            # Get the bundle type
            bundle_type = self.__get_bundle_type(file_name)
        # Assign the bundle
        d['bundles'] = [
            {
                "uuid": bundle_uuid,
                "version": bundle_version,
                "type": bundle_type
            }
        ]
        # Assign the files
        d["files"] = list(self.data_files.values())
        # Rearrange samples
        samples_list = []
        for i, sample_id in enumerate(d['sampleIds']):
            sample = {
                "sampleId": sample_id,
                "sampleBodyPart": d["sampleBodyPart"][i],
                "sampleSpecies": d["sampleSpecies"][i],
                "sampleNcbiTaxonIds": d["sampleNcbiTaxonIds"][i]
            }
            samples_list.append(sample)
        # Remove superfluous keywords
        d.pop('sampleIds', None)
        d.pop("sampleBodyPart", None)
        d.pop("sampleSpecies", None)
        d.pop("sampleNcbiTaxonIds", None)
        # Assign the sample list
        d["samples"] = samples_list
        # Iterate over each sample
        for sample in d['samples']:
            new_sample = deepcopy(d)
            new_sample['samples'] = sample
            es_uuid = sample['sampleId']
            new_sample['es_uuid'] = es_uuid
            # Load the current file in question
            contents = self.merge(new_sample, es_uuid)
            self.logger.debug("PRINTING SAMPLE INDEX DOCUMENT:\n")
            self.logger.debug(contents)
            self.load_doc(doc_contents=contents, doc_uuid=es_uuid)

    def __get_value(self, d, path):
        """
        Obtain a value deep in a Dictionary.

        This helper function serves to get a value deep in a dictionary.
        See: stackoverflow.com/questions/40468932/pass-nested-dictionary-
        location-as-parameter-in-python
        """
        return reduce(dict.get, path, d)

    def __merge_lists(self, new, current, path):
        """
        Merge two lists with unique ids.

        This function helps by merging two litst of dictionaries and making
        sure that the field in question only appears once in the list.
        """
        merged_dict = {}
        both_lists = current + new
        for item in both_lists:
            _id = self.__get_value(item, path)
            # TODO: I don't like this approach, but this is a first pass.
            # We need to redo so that it is less than O(n)
            # This first pass approach ensures new entries overwrite old ones.
            merged_dict[_id] = item
        merged = list(merged_dict.values())
        return merged

    def merge(self, doc_contents, _id):
        """
        Merge the current doc_contents.

        This method calls elasticsearch and merges the documents present
        in there with the ones from doc_contents
        """
        existing = self.es_client.get(index=self.index_name,
                                      doc_type=self.doc_type,
                                      id=_id,
                                      ignore=[404])
        if '_source' in existing:
            # Pop out the samples field
            sample = existing['_source'].pop('samples')
            # Merge all the fields:
            # do for loop, use the collections thing to update a new dict
            d = collections.defaultdict(list)
            d.update(existing['_source'])
            for key, value in doc_contents.items():
                if isinstance(d[key], list):
                    d[key].extend(value)
                else:
                    d[key] = value
            d['samples'] = sample
            return d
        else:
            return doc_contents

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

        :param file_extension: A string containing the the file extension
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


class ProjectOrientedIndexer(Indexer):
    """Create a Project oriented index.

    ProjectOrientedIndexer makes use of its index() function to perform
    indexing of the bundle files that are presented to the class when creating
    an instance.

    End for now.

    """

    def index(self, bundle_uuid, bundle_version, **kwargs):
        """Indexes the bundle into a sample oriented indexer.

        Triggers the actual indexing process

        :param bundle_uuid: The bundle_uuid of the bundle that will be indexed.
        :param bundle_version: The bundle of the version.
        """
        # Get the config driving indexing (e.g. the required entries)
        versions = self.index_mapping_config['requested_entries']
        # Get schema version for one of the metadata files
        metadata_file = next(iter(self.metadata_files.values()))
        schema_v = self.get_schema(metadata_file)
        # Get the config for the current schema version
        req_entries = versions[schema_v]
        # Put together the list of arguments
        args = [req_entries,  self.metadata_files]
        # Create a new dictionary
        d = collections.defaultdict(list)
        # For each tuple returned from the metadata file, update
        # the dictionary
        for key, value in self.get_item(*args):
            d[key].append(value)
        # Add the format and bundle type to each file
        bundle_type = None
        for file_name, description in self.data_files.items():
            self.data_files[file_name]['format'] = self.__get_format(file_name)
            # Get the bundle type
            bundle_type = self.__get_bundle_type(file_name)
        # Assign the bundle
        d['bundles'] = [
            {
                "uuid": bundle_uuid,
                "version": bundle_version,
                "type": bundle_type
            }
        ]
        # Assign the files
        d["files"] = list(self.data_files.values())
        # Rearrange samples
        samples_list = []
        for i, sample_id in enumerate(d['sampleIds']):
            sample = {
                "sampleId": sample_id,
                "sampleBodyPart": d["sampleBodyPart"][i],
                "sampleSpecies": d["sampleSpecies"][i],
                "sampleNcbiTaxonIds": d["sampleNcbiTaxonIds"][i]
            }
            samples_list.append(sample)
        # Remove superfluous keywords
        d.pop('sampleIds', None)
        d.pop("sampleBodyPart", None)
        d.pop("sampleSpecies", None)
        d.pop("sampleNcbiTaxonIds", None)
        # Assign the sample list
        d["samples"] = samples_list
        # Iterate over each sample
        for project in d['projectId']:
            new_project = deepcopy(d)
            new_project['projectId'] = project
            es_uuid = project
            new_project['es_uuid'] = es_uuid
            contents = self.merge(new_project, es_uuid)
            self.logger.debug("PRINTING PROJECT INDEX DOCUMENT:\n")
            self.logger.debug(contents)
            self.load_doc(doc_contents=contents, doc_uuid=es_uuid)

    def __get_value(self, d, path):
        """
        Obtain a value deep in a Dictionary.

        This helper function serves to get a value deep in a dictionary.
        See: stackoverflow.com/questions/40468932/pass-nested-dictionary-
        location-as-parameter-in-python
        """
        return reduce(dict.get, path, d)

    def __merge_lists(self, new, current, path):
        """
        Merge two lists with unique ids.

        This function helps by merging two litst of dictionaries and making
        sure that the field in question only appears once in the list.
        """
        merged_dict = {}
        both_lists = current + new
        for item in both_lists:
            _id = self.__get_value(item, path)
            # TODO: I don't like this approach, but this is a first pass.
            # We need to redo so that it is less than O(n)
            # This first pass approach ensures new entries overwrite old ones.
            merged_dict[_id] = item
        merged = list(merged_dict.values())
        return merged

    def merge(self, doc_contents, _id):
        """
        Merge the current doc_contents.

        This method calls elasticsearch and merges the documents present
        in there with the ones from doc_contents
        """
        existing = self.es_client.get(index=self.index_name,
                                      doc_type=self.doc_type,
                                      id=_id,
                                      ignore=[404])
        if '_source' in existing:
            # Pop out the samples field
            project = existing['_source'].pop('projectId')
            # Merge all the fields:
            # do for loop, use the collections thing to update a new dict
            d = collections.defaultdict(list)
            d.update(existing['_source'])
            for key, value in doc_contents.items():
                if isinstance(d[key], list):
                    d[key].extend(value)
                else:
                    d[key] = value
            d['projectId'] = project
            return d
        else:
            return doc_contents

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

        :param file_extension: A string containing the the file extension
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


class DonorIndexer(Indexer):
    """index method calls:.

    - special_fields
    - merge
    - load_mapping
        - create_mapping
    - load_doc

    """

    pass
