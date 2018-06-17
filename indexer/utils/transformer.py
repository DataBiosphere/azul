from abc import ABC, abstractmethod
from itertools import filterfalse, tee
import logging
import re
from typing import Mapping, Sequence
import os

module_logger = logging.getLogger(__name__)


class Document:
    def __init__(self, entity_id: str, bundle_uuid: str, bundle_version: str, content: dict) -> None:
        self.entity_id = entity_id
        self.bundle_uuid = bundle_uuid
        self.bundle_version = bundle_version
        self._document = {
            "entity_id": self.entity_id,
            "bundles": [
                {
                    "uuid": self.bundle_uuid,
                    "version": self.bundle_version,
                    "contents": content
                }
            ]
        }

    @property
    def document(self) -> dict:
        return self._document

    @document.setter
    def document(self, new_content: dict) -> None:
        self._document = new_content


class ElasticSearchDocument:
    def __init__(self, elastic_search_id: str, content: Document, entity_name: str, _type: str="doc") -> None:
        self.elastic_search_id = elastic_search_id
        self._content = content
        self.index = "browser_{}_{}".format(entity_name, os.getenv("STAGE_ENVIRONMENT", "dev"))
        self._type = _type
        self._version = 1

    @property
    def document_id(self) -> str:
        return self.elastic_search_id

    @property
    def document_content(self) -> dict:
        return self._content.document

    @property
    def document_index(self) -> str:
        return self.index

    @property
    def document_type(self) -> str:
        return self._type

    @property
    def document_version(self) -> int:
        return self._version

    @document_content.setter
    def document_content(self, new_content: dict) -> None:
        self._content.document = new_content

    @document_version.setter
    def document_version(self, new_version) -> None:
        self._version = new_version


class Transformer(ABC):

    def __init__(self):
        super().__init__()

    @property
    def entity_name(self) -> str:
        return ""

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
        simple_version = version.rsplit(".", 1)
        simple_version = simple_version[0].replace('.', '_')
        return simple_version

    @abstractmethod
    def create_documents(
            self,
            metadata_files: Mapping[str, dict],
            data_files: Mapping[str, dict],
            bundle_uuid: str,
            bundle_version: str,
    ) -> Sequence[ElasticSearchDocument]:
        pass

