from abc import ABC, abstractmethod
from itertools import filterfalse, tee
import re
from typing import Mapping, Sequence, Iterable


class Document:
    def __init__(self, entity_id: str, bundle_uuid: str, bundle_version: str, content: dict) -> None:
        self.entity_id = entity_id
        self.bundle_uuid = bundle_uuid
        self.bundle_version = bundle_version
        self.content = content

    @property
    def document(self) -> dict:
        constructed_dict = {
            "entity_id": self.entity_id,
            "bundles": [
                {
                    "uuid": self.bundle_uuid,
                    "version": self.bundle_version,
                    "contents": self.content
                }
            ]
        }
        return constructed_dict


class ElasticSearchDocument:
    def __init__(self, elastic_search_id: str, content: Document) -> None:
        self.elastic_search_id = elastic_search_id
        self.content = content

    @property
    def document_id(self) -> str:
        return self.elastic_search_id

    @property
    def document_content(self) -> dict:
        return self.content.document


class Transformer(ABC):
    def __init__(self):
        pass

    @staticmethod
    def partition(predicate, iterable):
        """
        Use a predicate to partition entries into false entries and
        true entries
        """
        t1, t2 = tee(iterable)
        return filterfalse(predicate, t1), filter(predicate, t2)

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
    def _create_specimens(self, metadata_dictionary: dict) -> Sequence[dict]:
        pass

    @abstractmethod
    def _create_project(self, metadata_dictionary: dict) -> Sequence[dict]:
        pass

    @abstractmethod
    def create_documents(
            self,
            metadata_files: Mapping[str, dict],
            data_files: Mapping[str, dict],
            bundle_uuid: str,
            bundle_version: str,
    ) -> Sequence[ElasticSearchDocument]:
        pass
