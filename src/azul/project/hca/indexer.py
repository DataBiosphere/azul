from typing import Iterable

from azul.indexer import BaseIndexer
from azul.project.hca.transformers import FileTransformer, SampleTransformer, ProjectTransformer, BundleTransformer
from azul.transformer import Transformer
from azul.types import JSON


class Indexer(BaseIndexer):

    def mapping(self) -> JSON:
        return {
            "dynamic_templates": [
                {
                    "exclude_metadata_field": {
                        "path_match": "metadata",
                        "mapping": {
                            "enabled": False
                        }
                    }
                },
                {
                    "project_nested_contributors": {
                        "path_match": "contents.projects.contributors",
                        "mapping": {
                            "enabled": False
                        }
                    }
                },
                {
                    "project_nested_publications": {
                        "path_match": "contents.projects.publications",
                        "mapping": {
                            "enabled": False
                        }
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
        return FileTransformer(), SampleTransformer(), ProjectTransformer(), BundleTransformer()

    def entities(self) -> Iterable[str]:
        return ['files', 'samples', 'projects', 'bundles']
