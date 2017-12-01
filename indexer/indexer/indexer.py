from abc import abstractmethod, ABCMeta


class AbstractIndexer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def extract_item(self, **kwargs):
        """
        looks for a items from the config in the metadata
        :return: none
        """
        raise NotImplementedError(
            'users must define extract_item to use this base class')

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
        :return: none
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
    def load_mapping(self, **kwargs):
        """
        load mappings into ES
        :return:
        """
        raise NotImplementedError(
            'users must define load_mapping to use this base class')


class Indexer(AbstractIndexer):
    def __init__(self, metadata_files, data_files, bundle_uuid, es_client, config="hello"):
        """

        :param metadata_files: list of json metadata (list of dicts)
        :param data_files: list describing non-metadata files (list of dicts)
        :param bundle_uuid: (string)
        :param es_client: localhost or not (string)
        :param config: basic config for indexer and elasticsearch index
        """
        self.metadata_files = metadata_files
        self.metadata_files = data_files
        self.bundle_uuid = bundle_uuid
        self.es_client = es_client
        self.config = config


    def extract_item(self, metadata_files, config):
        """
        looks for a items from the config in the metadata
        :return: none
        """
        if isinstance(metadata_files, dict):
            for key, value in config:
                if key in metadata_files:
                    if isinstance(value, dict):
                        self.extract_item(metadata_files[key], value)



    def index(self):
        """
        this is where different indexes can be indexed
        calls extract_item, merge, load_doc
        :return: none
        """


    def special_fields(self):
        """
        special fields (bundle_uuid, bundle_type)
        :return: none
        """


    def merge(self):
        """
        take results of extract_item and query ES to merge into one ES file
        calls special_fields
        :return:
        """


    def load_doc(self):
        """
        makes put request to ES
        :return:
        """


    def load_mapping(self):
        """
        load mappings into ES
        :return:
        """


    # so this is an Abstract Base Class? and file(Indexer) or project(Indexer) inherits?
    # and overwrites these functions?

    # goal of this class: take metadata files and reindex them into different
    # types of indexes (file, project, donor, etc)
    # maybe have an arrays of dicts per each index, then ES module can deal?
    # how does a donor metadata index work? what goes in?
    # what about mapping and set theory (adding none to keys that don't exist?)


    # What if we find all the keys values in all metadata files, then matched to keys as found in mapping?