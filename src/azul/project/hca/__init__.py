from azul.project.hca.config import IndexProperties
from azul.project.hca.indexer import Indexer
from azul import config

dss_subscription_query = {
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
                                    "gte": "2018-11-13"
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
                                                "1f9a699a-262c-40a0-8b2c-7ba960ca388c",  # tabulamuris
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
                                                        "files.project_json.project_core.project_short_name": "integration/"
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "manifest.version": {
                                                            "gte": "2018-11-07"
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
                    ]
                )
            ]
        }
    }
}

dss_deletion_subscription_query = {
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
