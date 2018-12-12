from typing import Iterable

from azul import config
from azul.indexer import BaseIndexer
from azul.project.hca.transformers import FileTransformer, ProjectTransformer, SpecimenTransformer
from azul.transformer import Transformer
from azul.types import JSON


class Indexer(BaseIndexer):

    def mapping(self) -> JSON:
        return {
            "dynamic_templates": [
                {
                    "project_nested_contributors": {
                        "match_pattern": "regex",
                        "path_match": r".*projects?\.contributors",
                        "mapping": {}
                    }
                },
                {
                    "project_nested_publications": {
                        "match_pattern": "regex",
                        "path_match": r".*projects?\.publications",
                        "mapping": {}
                    }
                },
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

    def settings(self) -> JSON:
        return {
            "index": {
                # This is important. It may slow down searches but it does increase concurrency during indexing,
                # currently our biggest performance bottleneck.
                "number_of_shards": config.indexer_concurrency,
                "number_of_replicas": 1
            }
        }

    def transformers(self) -> Iterable[Transformer]:
        return FileTransformer(), SpecimenTransformer(), ProjectTransformer()

    def entities(self) -> Iterable[str]:
        return ['files', 'specimens', 'projects']
