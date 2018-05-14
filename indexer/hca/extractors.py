import jmespath
import re
from typing import Mapping, Sequence

class ElasticSearchDocument:
    def __init__(self, elastic_search_id: str, document_json: dict) -> None:
        self.elastic_search_id = elastic_search_id
        self.document_json = document_json

    @property
    def document_id(self) -> str:
        return self.elastic_search_id

    @property
    def document_content(self) -> dict:
        return self.document_json


class Extractor:
    def __init__(self):
        pass

    @classmethod
    def get_version(cls, metadata_json: dict) -> str:
        schema_url = metadata_json["describedBy"]
        version_match = re.search(r'\d\.\d\.\d', schema_url)
        version = version_match.group()
        return version

    def _create_files(self, ):
        pass

    def _create_biomaterials(self, ):
        pass

    def _create_processes(self, ):
        pass

    def _create_protocols(self, ):
        pass

    def _create_projects(self, ):
        pass

    def create_documents(
            self,
            metadata_files: Mapping[str, dict],
            data_files: Mapping[str, dict]
    ) -> Sequence[ElasticSearchDocument]:
        pass
