from abc import ABC, abstractmethod
from project.hca.extractors import * # <- TODO: CHANGE MY FRIEND
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


class Transformer(ABC):
    def __init__(self):
        pass

    @classmethod
    def get_version(cls, metadata_json: dict) -> str:
        schema_url = metadata_json["describedBy"]
        version_match = re.search(r'\d\.\d\.\d', schema_url)
        version = version_match.group()
        version = version.replace('.', '_')
        return version

    @abstractmethod
    def _create_files(
            self,
            files_dictionary: dict,
            metadata_dictionary: dict=None) -> Sequence[dict]:
        pass

    @abstractmethod
    def _create_samples(self, metadata_dictionary: dict) -> Sequence[dict]:
        pass

    @abstractmethod
    def _create_projects(self, metadata_dictionary: dict) -> Sequence[dict]:
        pass

    @abstractmethod
    def create_documents(
            self,
            metadata_files: Mapping[str, dict],
            data_files: Mapping[str, dict]
    ) -> Sequence[ElasticSearchDocument]:
        pass


class FileTransformer(Transformer):
    def __init__(self):
        super().__init__()

    def _create_files(
            self,
            files_dictionary: dict,
            metadata_dictionary: dict=None
    ) -> Sequence[dict]:
        if metadata_dictionary is None:
            return [files_dictionary]
        else:
            metadata_version = self.get_version(metadata_dictionary)
            extractor = getattr()

    def _create_samples(self, metadata_dictionary: dict) -> Sequence[dict]:
        pass

    def _create_projects(self, metadata_dictionary: dict) -> Sequence[dict]:
        pass

    def create_documents(
            self,
            metadata_files: Mapping[str, dict],
            data_files: Mapping[str, dict]
    ) -> Sequence[ElasticSearchDocument]:
        pass
