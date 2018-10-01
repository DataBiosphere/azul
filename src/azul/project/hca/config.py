from typing import Any, Iterable, Mapping, Tuple

from azul.base_config import BaseIndexProperties
from azul.transformer import Transformer
from .transformers import FileTransformer, SpecimenTransformer


class IndexProperties(BaseIndexProperties):
    """Index properties for HCA"""

    def __init__(self, dss_url: str, es_endpoint: Tuple[str, int]) -> None:
        self._dss_url = dss_url
        self._es_endpoint = es_endpoint

        self._es_mapping = {
            "dynamic_templates": [
                {
                    "strings_as_text": {
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                {
                    "longs_with_keyword": {
                        "match_mapping_type": "long",
                        "mapping": {
                            "type": "long",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    },
                }
            ]
        }
        self._es_settings = {
            }
        }

    @property
    def dss_url(self) -> str:
        return self._dss_url

    @property
    def mapping(self) -> Mapping[str, Any]:
        return self._es_mapping

    @property
    def settings(self) -> Mapping[str, Any]:
        return self._es_settings

    @property
    def transformers(self) -> Iterable[Transformer]:
        transformers = [FileTransformer(), SpecimenTransformer()]
        return transformers

    @property
    def entities(self) -> Iterable[str]:
        return ["files", "specimens", "projects"]
