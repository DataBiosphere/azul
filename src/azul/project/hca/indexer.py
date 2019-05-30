from typing import Iterable

from azul.indexer import BaseIndexer
from azul.project.hca.transformers import FileTransformer, CellSuspensionTransformer, SampleTransformer, ProjectTransformer, BundleTransformer
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
                    "other_types_with_keyword": {
                        "match_mapping_type": "*",
                        "mapping": {
                            "type": "{dynamic_type}",
                            "fields": {
                                "keyword": {
                                    "type": "{dynamic_type}"
                                }
                            }
                        }
                    }
                }
            ]
        }

    def transformers(self) -> Iterable[Transformer]:
        return FileTransformer(), CellSuspensionTransformer(), SampleTransformer(), ProjectTransformer(), BundleTransformer()

    def entities(self) -> Iterable[str]:
        return ['files', 'cell_suspensions', 'samples', 'projects', 'bundles']
