import json
import pprint
import logging
#TODO Check if APACHE LICENSE is OK to Use
import jmespath

log = logging.getLogger(__name__)


TRANSFORMER_SETTINGS_KEY = 'transformer_settings'

INDEXER_NAME_KEY = 'indexer_name'
MAPPING_KEY = 'mappings'
INDEX_FIELD_KEY = 'index_field'
DSS_FIELD_KEY = 'dss_field'

CONSTANT_KEY = 'constant'
SOURCE_KEY = 'source'
FILE_DATA_KEY = 'from_file_data'

FILTER_KEY = 'filter'
FILTER_TARGET_FIELD_KEY = 'metadata_field'
FILTER_ORIGIN_FIELD_KEY = 'data_file_field'

MULTI_DSS_FIELD_KEY = 'field_names'
FORMATTERS_KEY = 'formatters'
#SEPERATOR = 'seperator'

#
# class FilterFieldError(Exception):
#     pass


class Formatter:
    name = "base_formatter"

    def __init__(self):
        pass

    def format(self, value):
        raise NotImplementedError


class ConvertToListFormatter(Formatter):
    name = "convert_to_list"

    def __init__(self):
        super().__init__()

    def format(self, value):
        if not isinstance(value, list):
            return [value]
        else:
            return value


class ExtractFileExtension(Formatter):
    name = "extract_file_extension"

    def __init__(self):
        super().__init__()

    def format(self, value):
        val_lst = value.split('.')

        if val_lst:
            return val_lst[-1]
        else:
            return "N/A"


class DCCJSONTransformer:
    def __init__(self, filename):
        with open(filename, 'r') as csf:
            self.settings = json.load(csf)
            self.filename = filename

        self._formatters = {}
        self.add_formatter(ConvertToListFormatter())
        self.add_formatter(ExtractFileExtension())

    def add_formatter(self, formatter):
        self._formatters[formatter.name] = formatter

    def update_settings(self):
        try:
            with open(self.filename, 'r') as csf:
                self.settings = json.load(csf)
        #TODO replace Generic Exception
        except Exception as e:
            log.error("Something's wrong with the settings:\n{}".format(e), exc_info=True)

    def transform(self, data_file, metadata_files):
        output_json = {}
        for index_setting in self.settings[TRANSFORMER_SETTINGS_KEY]:
            indexer_name = index_setting[INDEXER_NAME_KEY]
            index_output = output_json[indexer_name] = {}
            for mapping in index_setting[MAPPING_KEY]:
                index_field = mapping[INDEX_FIELD_KEY]

                #Selects what data from the metadata will be written
                if SOURCE_KEY in mapping.keys():
                    source = mapping[SOURCE_KEY]
                    metadata = metadata_files[source]
                    if FILTER_KEY in mapping.keys():
                        dss_filter_field = mapping[FILTER_KEY][0][FILTER_ORIGIN_FIELD_KEY]
                        index_filter_field = mapping[FILTER_KEY][0][FILTER_TARGET_FIELD_KEY]
                        data_file_value = data_file[dss_filter_field]
                        raw_value = self._get_matching_subitem(data_file_value, index_filter_field, index_field,
                                                               metadata)
                    else:
                        try:
                            dss_field_key = mapping[DSS_FIELD_KEY]
                        except KeyError:
                            log.error("Missing Required field from mapping:\n{}".format(pprint.pformat(mapping)),
                                      exc_info=True)
                            raise
                        else:
                            raw_value = self._find_value_with_fieldname(dss_field_key, metadata)
                elif mapping.get(FILE_DATA_KEY, False):
                    metadata = data_file
                    dss_field_key = mapping[DSS_FIELD_KEY]
                    raw_value = self._find_value_with_fieldname(dss_field_key, metadata)
                elif CONSTANT_KEY in mapping.keys():
                    raw_value = mapping[CONSTANT_KEY]
                else:
                    log.error("No source field given from settings file:\n{}".format(pprint.pformat(data_file)))
                    #TODO replace with custom exception
                    raise Exception

                formatted_value = raw_value
                if FORMATTERS_KEY in mapping.keys():
                    for fmtr_name in mapping[FORMATTERS_KEY]:
                        fmtr = self._formatters[fmtr_name]
                        formatted_value = fmtr.format(formatted_value)

                index_output[index_field] = formatted_value
        return output_json

    def _find_value_with_fieldname(self, dss_field, metadata):
        def parse_metadata(field):
            try:
                result = jmespath.search(dss_field, metadata)
            except IndexError as e:
                log.error("The '{}' field doesn't exist in selected metadata".format(field))
                exit()
            return result

        if isinstance(dss_field, dict):
            #TODO Implement seperator settings
            return ":".join(map(parse_metadata, dss_field[MULTI_DSS_FIELD_KEY]))
        else:
            return parse_metadata(dss_field)

    def _get_matching_subitem(self, dss_value, query_index_field, index_field, metadata):
        result = jmespath.search(index_field, metadata)
        matching_subitems = [res.context.value for res in result]
        for subitem in matching_subitems:
            query_index_value = self._find_value_with_fieldname(query_index_field, subitem)
            if dss_value == query_index_value:
                return self._find_value_with_fieldname(index_field, subitem)
