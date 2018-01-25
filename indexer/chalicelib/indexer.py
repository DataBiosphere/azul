# -*- coding: utf-8 -*-
"""Indexer module to help with indexing.

The indexer module contains classes to create different types of Indexes.

The based class Indexer serves as the basis for additional indexing classes
in this module.

"""
import collections
from functools import reduce
import json
from pprint import pprint
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
        req_entries = self.index_mapping_config['requested_entries']["v4.6.1"]
        req_entries2 = self.index_mapping_config['requested_entries']["vTEST"]#TEST
        # Iterate over each file
        for _file in self.data_files.values():
            # List the arguments for clarity
            args = [req_entries, "", self.metadata_files]
            # Get all the contents from the entries requested in the config
            contents = {key: value for key, value in self.__get_item(*args)}
            #TEST
            args2 = [req_entries2, self.metadata_files]
            d = collections.defaultdict(list)
            for key, value in self.__get_item2(*args2):
                d[key].append(value)
                print("JUST APPENDED ENTRY, dictionary right now:\n")
                pprint(d)
            print("#######TESTING NEW get_item2 METHOD##########")
            pprint(d)
            d = None
            ###FINISH TEST###
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
        req_entries = self.index_mapping_config['requested_entries']["v4.6.1"]
        all_fields = {entry for entry in self.__get_item(req_entries, "")}
        # Make a set out of the fields present in the data
        present_keys = set(present_fields.keys())
        # Add empty fields as the string 'None'
        empty = {field: "None" for field in all_fields - present_keys}
        # Merge the four dictionaries
        all_data = {**file_data, **extra_fields, **computed_fields, **empty}
        return all_data

    def flatten(self, l):
        for el in l:
            if isinstance(el, collections.Sequence) and not isinstance(el, (
                    str, bytes)):
                yield from self.flatten(el)
            else:
                yield el

    def __get_item2(self, c_item, _file):
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
                # for item in value:
                #     # Handle if _file is a list
                #     if isinstance(_file, list):
                #         print("PRINTING _file:")
                #         pprint(_file)
                #         for el in _file:
                #             print("PRINTING el")
                #             pprint(el)
                #             yield from self.__get_item2(c_item, el)
                #     # Recursive call on each level
                #     elif key in _file:
                #         child = _file[key]
                #         yield from self.__get_item2(item, child)
                if isinstance(_file, list):
                    print("PRINTING _file:")
                    pprint(_file)
                    for el in _file:
                        print("PRINTING el")
                        pprint(el)
                        yield from self.__get_item2(c_item, el)
                else:
                    for item in value:
                        # Handle if _file is a list
                        if key in _file:
                            child = _file[key]
                            yield from self.__get_item2(item, child)
        else:
            # Return the tuple containing the key and value
            content, name = tuple(c_item.split('*'))
            print("Content: {} ; Name: {} \n".format(content, name))
            pprint("THE LEAF _file is: {}\n ".format(_file))
            if isinstance(_file, list):
                for el in _file:
                    print("PRINTING LEAF el")
                    pprint(el)
                    yield from self.__get_item2(c_item, el)
            elif content in _file:
                print("the file content is: {}\n".format(_file[content]))
                yield name, _file[content]

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
                    # Handle if _file is a list
                    if isinstance(_file, list):
                        facets_list = [term for el in _file for term in self.__get_item(c_item, name, el)]
                        #facets_list = list(self.flatten(facets_list))
                        yield new_name, facets_list
                    # Recursive call on each level
                    elif _file is None or key in _file:
                        child = None if _file is None else _file[key]
                        yield from self.__get_item(item, new_name, _file=child)
        else:
            # Return name concatenated with config key
            name = "{}|{}".format(name, c_item).replace(".", ",")
            if _file is not None and c_item in _file:
                # If the file exists and contains the item in question
                yield name, _file[c_item]
            elif _file is None:
                # If we only want the string of the name
                yield name

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
    """Create an Analysis oriented index.

    AssayOrientedIndexer makes use of its index() function to perform
    indexing of the bundle files that are presented to the class when creating
    an instance.

    End for now.

    """

    def index(self, bundle_uuid, bundle_version, **kwargs):
        """Indexes the bundle into an assay oriented indexer.

        Triggers the actual indexing process

        :param bundle_uuid: The bundle_uuid of the bundle that will be indexed.
        :param bundle_version: The bundle of the version.
        """
        # Assign the ES uuid
        es_uuid = self.metadata_files['assay.json']['content']['assay_id']
        # Get required entries
        req_entries = self.index_mapping_config['requested_entries']["v4.6.1"]
        args = [req_entries, "", self.metadata_files]
        facets = {key: value for key, value in self.__get_item(*args)}
        for file_name, description in self.data_files.items():
            self.data_files[file_name]['format'] = self.__get_format(file_name)
            # Get the bundle type
            bundle_type = self.__get_bundle_type(file_name)
            self.data_files[file_name]["bundle_type"] = bundle_type
        # Get extra fields
        if "analysis.json" in self.metadata_files:
            analysis_type = "analysis"
            analysis_json = self.metadata_files['analysis.json']
        else:
            analysis_type = "upload"
            analysis_json = {"content": {"analysis_id": es_uuid}}
        # Get samples
        samples = facets['sample,json|samples']

        # Construct the ES doc to be loaded
        contents = {
            **self.metadata_files['assay.json'],
            "analysis": [
                {**analysis_json,
                 'analysis_type': analysis_type,
                 'data_bundles': [
                     {"bundle_uuid": bundle_uuid,
                      "files": list(self.data_files.values())}
                 ]}
            ],
            "samples": samples,
            "projects": [
                {**self.metadata_files['project.json']}
            ],
            "es_uuid": es_uuid
        }
        # Load the current file in question
        contents = self.merge(contents, es_uuid)
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
            # Get the samples
            samples_es = existing['_source']['samples']
            samples_doc = doc_contents['samples']
            # Merge samples
            samples_merge = self.__merge_lists(samples_doc,
                                               samples_es,
                                               ('content', 'sample_id'))
            # Get analysis
            analysis_es = existing['_source']['analysis']
            analysis_doc = doc_contents['analysis']
            # Merge analysis
            analysis_merge = self.__merge_lists(analysis_doc,
                                                analysis_es,
                                                ('content', 'analysis_id'))
            # Assign only unique analysis
            for analysis in analysis_merge:
                analysis_id = self.__get_value(analysis, ('content',
                                                          'analysis_id'))
                for old_analysis in analysis_es:
                    old_analysis_id = self.__get_value(old_analysis,
                                                       ('content',
                                                        'analysis_id'))
                    if analysis_id == old_analysis_id:
                        new_bundles = analysis['data_bundles']
                        old_bundles = old_analysis['data_bundles']
                        merged_bundles = self.__merge_lists(new_bundles,
                                                            old_bundles,
                                                            ('bundle_uuid',))
                        analysis['data_bundles'] = merged_bundles
                        break
            # Get projects
            projects_es = existing['_source']['projects']
            projects_doc = doc_contents['projects']
            projects_merge = self.__merge_lists(projects_doc,
                                                projects_es,
                                                ('content', 'project_id'))
            # Assign new top fields
            doc_contents['projects'] = projects_merge
            doc_contents['samples'] = samples_merge
            doc_contents['analysis'] = analysis_merge
            return doc_contents

        else:
            return doc_contents

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
                        child = None if _file is None else _file[key]
                        yield from self.__get_item(item, new_name, _file=child)
        else:
            # Return name concatenated with config key
            name = "{}|{}".format(name, c_item).replace(".", ",")
            if _file is not None and c_item in _file:
                # If the file exists and contains the item in question
                yield name, _file[c_item]
            elif _file is None:
                # If we only want the string of the name
                yield name

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
        # Get required entries
        req_entries = self.index_mapping_config['requested_entries']["v4.6.1"]
        args = [req_entries, "", self.metadata_files]
        facets = {key: value for key, value in self.__get_item(*args)}
        for file_name, description in self.data_files.items():
            self.data_files[file_name]['format'] = self.__get_format(file_name)
            # Get the bundle type
            bundle_type = self.__get_bundle_type(file_name)
            self.data_files[file_name]["bundle_type"] = bundle_type
        # Get extra fields
        if "analysis.json" in self.metadata_files:
            analysis_type = "analysis"
            analysis_json = self.metadata_files['analysis.json']
        else:
            analysis_type = "upload"
            assay_id = self.metadata_files['assay.json']['content']['assay_id']
            analysis_json = {"content": {"analysis_id": assay_id}}
        # Get samples
        samples = facets['sample,json|samples']
        # Iterate over each sample
        for _sample in samples:
            # Get the elasticsearch uuid for this particular sample
            es_uuid = _sample['content']['sample_id']
            contents = {
                **_sample,
                "assays": [
                    {
                        **self.metadata_files['assay.json'],
                        "analysis": [
                            {**analysis_json,
                             'analysis_type': analysis_type,
                             'data_bundles': [
                                 {"bundle_uuid": bundle_uuid,
                                  "files": list(self.data_files.values())}
                             ]}
                        ]
                    }
                ],
                "projects": [
                    {**self.metadata_files['project.json']}
                ],
                "es_uuid": es_uuid
            }
            # Load the current file in question
            contents = self.merge(contents, es_uuid)
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
            # Get the assays
            assays_es = existing['_source']['assays']
            assays_doc = doc_contents['assays']
            # Merge assays
            assay_merge = self.__merge_lists(assays_doc,
                                             assays_es,
                                             ('content', 'assay_id'))
            # Assign only unique assays
            for assay in assay_merge:
                # Get the samples
                samples_es = existing['_source']['assays']['samples']
                samples_doc = assay['samples']
                # Merge samples
                samples_merge = self.__merge_lists(samples_doc,
                                                   samples_es,
                                                   ('content', 'sample_id'))
                # Get analysis
                analysis_es = existing['_source']['analysis']
                analysis_doc = doc_contents['analysis']
                # Merge analysis
                analysis_merge = self.__merge_lists(analysis_doc,
                                                    analysis_es,
                                                    ('content', 'analysis_id'))
                for analysis in analysis_merge:
                    analysis_id = self.__get_value(analysis, ('content',
                                                              'analysis_id'))
                    for old_analysis in analysis_es:
                        old_analysis_id = self.__get_value(old_analysis,
                                                           ('content',
                                                            'analysis_id'))
                        if analysis_id == old_analysis_id:
                            new_bundles = analysis['data_bundles']
                            old_bundles = old_analysis['data_bundles']
                            merged_bundles = self.__merge_lists(
                                new_bundles,
                                old_bundles,
                                ('bundle_uuid',))
                            analysis['data_bundles'] = merged_bundles
                            break
            for analysis in analysis_merge:
                analysis_id = self.__get_value(analysis, ('content',
                                                          'analysis_id'))
                for old_analysis in analysis_es:
                    old_analysis_id = self.__get_value(old_analysis,
                                                       ('content',
                                                        'analysis_id'))
                    if analysis_id == old_analysis_id:
                        new_bundles = analysis['data_bundles']
                        old_bundles = old_analysis['data_bundles']
                        merged_bundles = self.__merge_lists(new_bundles,
                                                            old_bundles,
                                                            ('bundle_uuid',))
                        analysis['data_bundles'] = merged_bundles
                        break
            # Get projects
            projects_es = existing['_source']['projects']
            projects_doc = doc_contents['projects']
            projects_merge = self.__merge_lists(projects_doc,
                                                projects_es,
                                                ('content', 'project_id'))
            # Assign new top fields
            doc_contents['projects'] = projects_merge
            doc_contents['samples'] = samples_merge
            doc_contents['analysis'] = analysis_merge
            return doc_contents

        else:
            return doc_contents

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
                        child = None if _file is None else _file[key]
                        yield from self.__get_item(item, new_name, _file=child)
        else:
            # Return name concatenated with config key
            name = "{}|{}".format(name, c_item).replace(".", ",")
            if _file is not None and c_item in _file:
                # If the file exists and contains the item in question
                yield name, _file[c_item]
            elif _file is None:
                # If we only want the string of the name
                yield name

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
