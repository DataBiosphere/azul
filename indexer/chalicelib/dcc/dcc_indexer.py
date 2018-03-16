from chalicelib.indexer import Indexer, FileIndexer
from chalicelib.dcc.transformer import DCCJSONTransformer
import os
import json
import logging

log = logging.getLogger(__name__)


CONFIG_JSON_FILENAME = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "transformer_mapping.json")
REQUESTED_ENTRIES_KEY = 'requested_entries'

ES_DOC_TYPE = 'meta'


class DCCIndexer(Indexer):
    transformer = DCCJSONTransformer(CONFIG_JSON_FILENAME)

    def __init__(self, metadata_files, data_files, es_client):
        super(DCCIndexer, self).__init__(metadata_files, data_files, es_client, None, ES_DOC_TYPE)
        self.transformer.update_settings()

    def index(self, bundle_uuid, bundle_version, *args, **kwargs):
        self.transformer.update_settings()
        for _file in self.data_files.values():
            indexer_doc_list = self.transformer.transform(_file, self.metadata_files)
            for indexer_name, contents in indexer_doc_list.items():
                doc_id = f"{bundle_uuid}.{bundle_version}"
                self.load_doc_by_index(doc_id, contents, indexer_name)

    #TODO Add duplication check from like from FileIndexer in develop branch
    def merge(self, doc_contents, **kwargs):
        return doc_contents

    def create_mapping(self, *args, **kwargs):
        mapping_config = self.get_mapping_config()
        return json.dumps(mapping_config)

    # TODO Figure out if more extra fields are needed and if certain fields need to be formatted
    def special_fields(self, data_file, present_fields, **kwargs):
        extra_fields = {key: value for key, value in kwargs.items()}
        req_entries = self.get_required_entries()
        all_data = {**data_file, **extra_fields}
        return all_data

    def get_id(self):
        return

    def get_mapping_config(self):
        return self.index_mapping_config['es_mapping']

    def load_doc_by_index(self, doc_uuid, doc_contents, index_name, *args, **kwargs):
        self.es_client.index(index=index_name,
                             doc_type=self.doc_type,
                             id=doc_uuid,
                             body=doc_contents)
