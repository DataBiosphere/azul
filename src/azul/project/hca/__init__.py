from typing import Type

from azul import config
from azul.indexer import BaseIndexer
import azul.plugin
from azul.project.hca.indexer import Indexer
from azul.types import JSON


class Plugin(azul.plugin.Plugin):

    def indexer_class(self) -> Type[BaseIndexer]:
        return Indexer

    def dss_subscription_query(self) -> JSON:
        return {
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "term": {
                                "admin_deleted": True
                            }
                        }
                    ],
                    "must": [
                        {
                            "exists": {
                                # Remove conditional when prod bundles are converted to vx structure
                                "field": "files.project_json"
                            }
                        }, *(
                            [
                                {
                                    "range": {
                                        "manifest.version": {
                                            "gte": "2018-11-27"
                                        }
                                    }
                                }
                            ] if config.dss_endpoint == "https://dss.integration.data.humancellatlas.org/v1" else [
                                {
                                    "bool": {
                                        "should": [
                                            {
                                                "terms": {
                                                    "files.project_json.provenance.document_id": [
                                                        # CBeta Release spreadsheet as of 11/13/2018
                                                        "2c4724a4-7252-409e-b008-ff5c127c7e89",  # treutlein
                                                        "08e7b6ba-5825-47e9-be2d-7978533c5f8c",  # pancreas6decades
                                                        "019a935b-ea35-4d83-be75-e1a688179328",  # neuron_diff
                                                        "a5ae0428-476c-46d2-a9f2-aad955b149aa",  # EMTAB5061
                                                        "adabd2bd-3968-4e77-b0df-f200f7351661",  # Regev-ICA
                                                        "67bc798b-a34a-4104-8cab-cad648471f69",  # Teichmann-mouse-melanoma
                                                        "81b5f43d-3c20-4575-9efa-bfb0b070a6e3",  # Meyer
                                                        "519b58ef-6462-4ed3-8c0d-375b54f53c31",  # EGEOD106540
                                                        "1f9a699a-262c-40a0-8b2c-7ba960ca388c",  # ido_amit
                                                        "62aa3211-bf52-4873-9029-0bcc1d09e553",  # humphreys
                                                        "a71dee10-9a4d-4ea2-a6f3-ae7314112cf1",  # peer
                                                        "adb384b2-cd5e-4cf5-9205-5f066474005f",  # basu
                                                        "46c58e08-4518-4e45-acfe-bdab2434975d",  # 10x-mouse-brain
                                                        "3eaad325-3666-4b65-a4ed-e23ff71222c1",  # rsatija
                                                    ]
                                                }
                                            },
                                            {
                                                "bool": {
                                                    "must": [
                                                        {
                                                            "prefix": {
                                                                "files.project_json.project_core.project_short_name": "staging/"
                                                            }
                                                        },
                                                        {
                                                            "range": {
                                                                "manifest.version": {
                                                                    "gte": "2018-11-27"
                                                                }
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            ] if config.dss_endpoint == "https://dss.staging.data.humancellatlas.org/v1" else [
                                {
                                    "bool": {
                                        "must": [
                                            {
                                                "terms": {
                                                    "files.project_json.provenance.document_id": [
                                                        # CBeta Release spreadsheet as of 12/03/2018
                                                        "2a0faf83-e342-4b1c-bb9b-cf1d1147f3bb",  # treutlein
                                                        "e8642221-4c2c-4fd7-b926-a68bce363c88",  # pancreas6decades
                                                        "cf8439db-fcc9-44a8-b66f-8ffbf729bffa",  # meyer
                                                        "f396fa53-2a2d-4b8a-ad18-03bf4bd46833",  # Teichmann-mouse-melanoma
                                                        "fd1d163d-d6a7-41cd-b3bc-9d77ba9a36fe",  # peer
                                                        "f8880be0-210c-4aa3-9348-f5a423e07421",  # neuron_diff
                                                        "0c7bbbce-3c70-4d6b-a443-1b92c1f205c8",  # ido_amit
                                                        "1630e3dc-5501-4faf-9726-2e2c0b4da6d7",  # humphreys
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            ] if config.dss_endpoint == "https://dss.data.humancellatlas.org/v1" else [
                            ]
                        )
                    ]
                }
            }
        }

    def dss_deletion_subscription_query(self) -> JSON:
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "admin_deleted": True
                            }
                        }
                    ]
                }
            }
        }
