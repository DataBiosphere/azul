import json
import pprint
#TODO Check if APACHE LICENSE is OK to Use
from jsonpath_rw import parse as jsonpath_parse

import operator

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
#SEPERATOR = 'seperator'

#
# class FilterFieldError(Exception):
#     pass


class DCCJSONTransformer:
    def __init__(self, filename):
        with open(filename, 'r') as csf:
            self.settings = json.load(csf)
            self.filename = filename

    def update_settings(self):
        try:
            self.settings = json.load(self.filename)
        except Exception:
            print("Something wrong with the settings.")

    def transform(self, data_file, metadata_files):
        output_json = {}
        for index_setting in self.settings[TRANSFORMER_SETTINGS_KEY]:
            indexer_name = index_setting[INDEXER_NAME_KEY]
            index_output = output_json[indexer_name] = {}
            for mapping in index_setting[MAPPING_KEY]:
                index_field = mapping[INDEX_FIELD_KEY]
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
                        dss_field_key = mapping[DSS_FIELD_KEY]
                        raw_value = self._find_value_with_fieldname(dss_field_key, metadata)
                elif bool(mapping.get(FILE_DATA_KEY, False)):
                    metadata = data_file
                    dss_field_key = mapping[DSS_FIELD_KEY]
                    raw_value = self._find_value_with_fieldname(dss_field_key, metadata)
                elif CONSTANT_KEY in mapping.keys():
                    raw_value = mapping[CONSTANT_KEY]
                else:
                    # TODO add better exception
                    # pprint.pprint(data_file)
                    raise Exception

                index_output[index_field] = raw_value
        return output_json

    def _find_value_with_fieldname(self, dss_field, metadata):
        def parse_metadata(field):
            parser = jsonpath_parse("$..{}".format(field))
            result = parser.find(metadata)
            return result[0].value
        if isinstance(dss_field, dict):
            return ":".join(map(parse_metadata, dss_field[MULTI_DSS_FIELD_KEY]))
        else:
            return parse_metadata(dss_field)

    def _get_matching_subitem(self, dss_value, query_index_field, index_field, metadata):
        parser = jsonpath_parse("$..{}".format(index_field))
        result = parser.find(metadata)
        matching_subitems = [res.context.value for res in result]
        for subitem in matching_subitems:
            query_index_value = self._find_value_with_fieldname(query_index_field, subitem)
            if dss_value == query_index_value:
                return self._find_value_with_fieldname(index_field, subitem)




