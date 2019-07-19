from typing import Iterable

from azul.indexer import BaseIndexer
from azul.project.hca.transformers import (FileTransformer,
                                           CellSuspensionTransformer,
                                           SampleTransformer,
                                           ProjectTransformer,
                                           BundleTransformer)
from azul.transformer import Transformer
from azul.types import JSON


class Indexer(BaseIndexer):

    def mapping(self) -> JSON:
        return {
            "dynamic_templates": [
                {
                    "donor_age_range": {
                        "path_match": "contents.donors.organism_age_range",
                        "mapping": {
                            # This field has to be a double because the `donor_age_range` needs to be precise at values
                            # slightly larger than the human lifespan. A float with 2 decimal precision starts losing
                            # its accuracy at 16,777,216.00. That value in seconds, is only about 0.53 in years.
                            # Doubles lose their accuracy at 9,007,199,254,740,993.00 seconds which is
                            # 285,616,414.72 years.
                            "type": "double_range"
                        }
                    }
                },
                {
                    "exclude_metadata_field": {
                        "path_match": "contents.metadata.*",
                        "mapping": {
                            "type": "{dynamic_type}",
                            "index": False
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
        return (FileTransformer(),
                CellSuspensionTransformer(),
                SampleTransformer(),
                ProjectTransformer(),
                BundleTransformer())

    def entities(self) -> Iterable[str]:
        return ['files', 'cell_suspensions', 'samples', 'projects', 'bundles']
