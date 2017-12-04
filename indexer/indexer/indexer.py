from abc import abstractmethod, ABCMeta


class AbstractIndexer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, metadata_files, data_files, es_client, **kwargs):
        """
        :param metadata_files: list of json metadata (list of dicts)
        :param data_files: list describing non-metadata files (list of dicts)
        :param es_client: localhost or not (string)
        """
        self.metadata_files = metadata_files
        self.metadata_files = data_files
        self.es_client = es_client

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
    def __init__(self, metadata_files, data_files, es_client, **kwargs):
        """
        :param metadata_files: list of json metadata (list of dicts)
        :param data_files: list describing non-metadata files (list of dicts)
        :param es_client: The elasticsearch client
        """
        # Call the parent __init__ method
        super(AbstractIndexer, self).__init__()
        # Set all kwargs as instance attributes
        for key, value in kwargs.items():
            setattr(self, key, value)
        # Ensure this keys got passed on to kwargs
        index_config = ['index_name',
                        'index_settings',
                        'index_mapping_config',
                        'doc_type']
        # Confirm fields in index_config are present
        if not all(attr in kwargs for attr in index_config):
            missing_fields = index_config - kwargs.keys()
            raise ValueError("You are missing: {}".format(missing_fields))
        # Loads the mapping into ElasticSearch
        self.load_mapping()

    def index(self, **kwargs):
        """
        this is where different indexes can be indexed
        calls merge, load_doc
        :return: none
        """
        raise NotImplementedError(
            'users must define index to use this base class')

    def special_fields(self, **kwargs):
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

    def load_doc(self, **kwargs):
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
                             id=kwargs['doc_uuid'],
                             body=kwargs['doc_contents'])

    def create_mapping(self, **kwargs):
        """
        To be overwritten by the class inheriting from here.
        This class should return the dictionary corresponding to the mapping
        to be loaded into ElasticSearch. It should use the index_mapping_config
        attribute as appropriate.
        """
        return {}

    def load_mapping(self, **kwargs):
        """
        create index and load mappings into ES (ElasticSearch)
        :return:
        """
        # Creates the index
        self.es_client.indices.create(index=self.index_name,
                                      doc_type=self.index_settings,
                                      ignore=[400])
        # Loads mappings for the index
        self.es_client.put_mapping(index=self.index_name,
                                   doc_type=self.doc_type,
                                   body=self.create_mapping())


class FileIndexer(Indexer):
    def index(self, **kwargs):
        for dfile in data_files:
            file_uuid = dfiles[file_uuid]
            es_json = self.special_fields(dfile=dfile, metadata=metadata_files)
            for field in es_json:
                for fkey, fvalue in field.items():
                    if fkey == 'es_uuid':
                        es_uuid = fvalue
                        es_json.remove(field)
            for c_key, c_value in kwargs['config']:
                for mfile in metadata_files:
                    if c_key in mfile:
                        for c_item in c_value:
                            to_append = self.__extract_item(c_key, mfile)
                            if to_append is not None:
                                if isinstance(to_append, list):
                                    # makes lists of lists into a single list
                                    to_append = self.merge(to_append)
                                    for item in to_append:
                                        # add file item to list of items to append to ES
                                        es_json.append(item)
                                else:
                                    # add file item to list of items to append to ES
                                    es_json.append(to_append)

            to_append = self.__extract_item(config, metadata_files)
            if to_append is not None:
                if isinstance(to_append, list):
                    # makes lists of lists into a single list
                    to_append = self.merge(to_append)
            self.load_doc(doc_contents=to_append, doc_uuid=es_uuid)

    def __extract_item(self, **kwargs):
        if isinstance(c_item, dict):
            # if the config is a dictionary,
            # then need to look deeper into the file and config for the key
            es_array = []
            for key, value in c_item.items():
                # removing mapping param
                key_split = key.split("*")
                if key in file:
                    # making the name that shows path taken to get to value
                    if len(name) > 0:
                        name = str(name) + "|" + str(key_split[0])
                    else:
                        name = str(key)
                    for item in value:
                        # resursive call, one nested item deeper
                        es_array.append(
                            self.__extract_item(item, file[key_split[0]],
                                                name))
                    return es_array
        elif c_item.split("*")[0] in file:
            # if config item is in the file
            c_item_split = c_item.split("*")
            file_value = file[c_item_split[0]]
            # need to be able to handle lists
            if not isinstance(file_value, list):
                if len(name) > 0:
                    name = str(name) + "|" + str(c_item_split[0])
                else:
                    name = str(c_item_split[0])
                # ES does not like periods(.) use commas(,) instead
                n_replace = name.replace(".", ",")
                # return the value of key (given by config)
                return ({n_replace: file_value})
        # Carlos's test
        elif c_item.split("*")[0] not in file:
            # all config items that cannot be found in file are given value "None"
            c_item_split = c_item.split("*")
            # Putting an empty string.
            # If None (instead of "None") it could break things downstream
            file_value = "None"
            name = str(name) + "|" + str(c_item_split[0])
            # ES does not like periods(.) use commas(,) instead
            n_replace = name.replace(".", ",")
            return ({n_replace: file_value})

    def merge(l):
        for el in l:
            if isinstance(el, collections.Sequence) and not isinstance(el, (
                    str, bytes)):
                yield from self.merge(el)
            else:
                yield el

    def create_mapping():
        # ES mapping
        try:
            with open('chalicelib/config.json') as f:
                config = json.loads(f.read())
        except Exception as e:
            print(e)
            raise NotFoundError("chalicelib/config.json file does not exist")
        # key_names = ['bundle_uuid', 'dirpath', 'file_name']
        key_names = ['bundle_uuid', 'file_name', 'file_uuid',
                     'file_version', 'file_format', 'bundle_type',
                     "file_size*long"]
        for c_key, c_value in config.items():
            for c_item in c_value:
                # get the names for each of the items in the config
                # the key_names array still has the mapping attached to each name
                key_names.append(self.__es_config(c_item, c_key))
        key_names = __merge(key_names)
        es_mappings = []

        # i_split splits at "*" (i for item)
        # u_split splits i_split[1] at "_" (u for underscore)
        # banana_split splits i_split[2] at "_"
        for item in key_names:
            # this takes in names with mappings still attached, separates it
            # name and mappings separated by *
            # analyzer is separated by mapping by _
            # ex: name*mapping1*mapping2_analyzer
            i_replace = item.replace(".", ",")
            i_split = i_replace.split("*")
            if len(i_split) == 1:
                # ex: name
                # default behavior: main field: keyword, raw field: text with analyzer
                es_mappings.append(
                    {i_split[0]: {"type": "keyword",
                                  "fields": {"raw": {"type": "text",
                                                     "analyzer": config_analyzer,
                                                     "search_analyzer": "standard"}}}})
            elif len(i_split) == 2:
                u_split = i_split[1].split("_")
                if len(u_split) == 1:
                    # ex: name*mapping1
                    es_mappings.append({i_split[0]: {"type": i_split[1]}})
                else:
                    # ex: name*mapping1_analyzer
                    es_mappings.append(
                        {i_split[0]: {"type": u_split[0],
                                      "analyzer": u_split[1],
                                      "search_analyzer": "standard"}})
            else:
                u_split = i_split[1].split("_")
                banana_split = i_split[2].split("_")
                if len(u_split) == 1 and len(banana_split) == 1:
                    # ex: name*mapping1*mapping2
                    es_mappings.append(
                        {i_split[0]: {"type": i_split[1], "fields": {
                            "raw": {"type": i_split[2]}}}})
                elif len(u_split) == 2 and len(banana_split) == 1:
                    # ex: name*mapping1_analyzer*mapping2
                    es_mappings.append(
                        {i_split[0]: {"type": u_split[0],
                                      "analyzer": u_split[1],
                                      "search_analyzer": "standard",
                                      "fields": {
                                          "raw": {"type": i_split[2]}}}})
                elif len(u_split) == 1 and len(banana_split) == 2:
                    # ex: name*mapping1*mapping2_analyzer
                    es_mappings.append(
                        {i_split[0]: {"type": i_split[1], "fields": {
                            "raw": {"type": banana_split[0],
                                    "analyzer": banana_split[1],
                                    "search_analyzer": "standard"}}}})
                elif len(u_split) == 2 and len(banana_split) == 2:
                    # ex: name*mapping1_analyzer*mapping2_analyzer
                    es_mappings.append(
                        {i_split[0]: {"type": u_split[0], "fields": {
                            "raw": {"type": banana_split[0],
                                    "analyzer": banana_split[1],
                                    "search_analyzer": "standard"}},
                                      "analyzer": u_split[1],
                                      "search_analyzer": "standard"}})
                else:
                    app.log.info("mapping formatting problem %s", i_split)

        es_keys = []
        es_values = []
        # format mappings
        for item in es_mappings:
            if item is not None:
                for key, value in item.items():
                    es_keys.append(key)
                    es_values.append(value)
            es_file = dict(zip(es_keys, es_values))
        return final_mapping = '{"properties":' + json.dumps(es_file) + '}'

    def __es_config(c_item, name):
        """
        This function is a simpler version of look_file
        The name is recursively found by going through
        the nested levels of the config file
        :param c_item: config item
        :param name: used for key in the key, value pair
        :return: name
        """
        if isinstance(c_item, dict):
            es_array = []
            # name concatenated with config key
            for key, value in c_item.items():
                if len(name) > 0:
                    name = str(name) + "|" + str(key)
                else:
                    name = str(key)
                # recursively call on each item in this level of config values
                for item in value:
                    es_array.append(self.__es_config(item, name))
                return es_array
        else:
            # return name concatenated with config key
            name = str(name) + "|" + str(c_item)
            return (name)

    def special_fields(self, **kwargs):
        es_json = []
        file_name, values = list(dfile.items())[0]
        file_uuid = values['uuid']
        file_version = values['version']
        file_size = values['size']
        # Make the ES uuid be a concatenation of:
        # bundle_uuid:file_uuid:file_version
        es_uuid = "{}:{}:{}".format(bundle_uuid, file_uuid, file_version)
        es_json.append({'es_uuid': es_uuid})
        # add special fields (ones that aren't in config)
        es_json.append({'bundle_uuid': bundle_uuid})
        # Carlos adding extra fields
        es_json.append({'file_name': file_name})
        es_json.append({'file_uuid': file_uuid})
        es_json.append({'file_version': file_version})
        # Add the file format
        file_format = '.'.join(file_name.split('.')[1:])
        file_format = file_format if file_format != '' else 'None'
        es_json.append({'file_format': file_format})
        # Emily adding bundle_type
        if 'analysis.json' in [list(x.keys())[0] for x in metadata]:
            es_json.append({'bundle_type': 'Analysis'})
        elif re.search(r'(tiff)', str(file_extensions)):
            es_json.append({'bundle_type': 'Imaging'})
        elif re.search(r'(fastq.gz)', str(file_extensions)):
            es_json.append({'bundle_type': 'scRNA-Seq Upload'})
        else:
            es_json.append({'bundle_type': 'Unknown'})
        # adding size of the file
        es_json.append({'file_size': file_size})
        return es_json


class DonorIndexer(Indexer):
    pass


'''
index method calls:
    - merge
    -load_doc
'''