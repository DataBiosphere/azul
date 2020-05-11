from typing import Iterable

from azul.indexer import BaseIndexer
from azul.plugins.metadata.hca.transformers import (
    FileTransformer,
    CellSuspensionTransformer,
    SampleTransformer,
    ProjectTransformer,
    BundleTransformer,
)
from azul.indexer.transformer import Transformer
from azul.types import JSON


class Indexer(BaseIndexer):

    def mapping(self) -> JSON:
        return {
            "numeric_detection": False,
            "dynamic_templates": [
                {
                    "donor_age_range": {
                        "path_match": "contents.donors.organism_age_range",
                        "mapping": {
                            # A float (single precision IEEE-754) can represent all integers up to 16,777,216. If we
                            # used float values for organism ages in seconds, we would not be able to accurately
                            # represent an organism age of 16,777,217 seconds. That is 194 days and 15617 seconds.
                            # A double precision IEEE-754 representation loses accuracy at 9,007,199,254,740,993 which
                            # is more than 285616415 years.

                            # Note that Python's float uses double precision IEEE-754.
                            # (https://docs.python.org/3/tutorial/floatingpoint.html#representation-error)
                            "type": "double_range"
                        }
                    }
                },
                {
                    "exclude_metadata_field": {
                        "path_match": "contents.metadata",
                        "mapping": {
                            "enabled": False
                        }
                    }
                },
                {
                    "exclude_metadata_field": {
                        "path_match": "contents.files.related_files",
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

    @classmethod
    def transformers(cls) -> Iterable[Transformer]:
        return (FileTransformer(),
                CellSuspensionTransformer(),
                SampleTransformer(),
                ProjectTransformer(),
                BundleTransformer())

    def entities(self) -> Iterable[str]:
        return ['files', 'cell_suspensions', 'samples', 'projects', 'bundles']
