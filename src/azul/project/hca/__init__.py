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
                                                            "gte": "2018-11-13"
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
                                "must_not": [
                                    {
                                        "terms": {
                                            "files.project_json.provenance.document_id": [
                                                # CBeta Release spreadsheet (Production Obsolete Datasets) as of 12/17/2018
                                                "1630e3dc-5501-4faf-9726-2e2c0b4da6d7",  # ???
                                                "fd1d163d-d6a7-41cd-b3bc-9d77ba9a36fe",  # peer
                                                "2a0faf83-e342-4b1c-bb9b-cf1d1147f3bb",  # treutlein
                                                "cf8439db-fcc9-44a8-b66f-8ffbf729bffa",  # meyer
                                                "179bf9e6-5b33-4c5b-ae26-96c7270976b8",  # Regev-ICA
                                                "6b9f514d-d738-403f-a9c2-62580bbe5c83",  # Q4_DEMO-…
                                                "311d013c-01e4-42c0-9c2d-25472afa9cbc",  # Q4_DEMO-…
                                                "d237ed6a-3a7f-4a91-b300-b070888a8542",  # DCP_Infrastructure_Test_
                                                # remove once https://github.com/DataBiosphere/azul/issues/86 is fixed
                                                "32eb86db-6842-480f-a49a-a2b0161ed35a",  # tabulamuris
                                            ]
                                        }
                                    },
                                    {
                                        "terms": {
                                            "files.project_json.hca_ingest.document_id": [
                                                # CBeta Release spreadsheet (Production Obsolete Datasets) as of 12/17/2018
                                                "bae45747-546a-4aed-9377-08e9115a8fb8",  # Q4_DEMO-…
                                                "7cb4940d-7c85-43d1-b2f5-1d99813e65df",  # Q4_DEMO-…
                                                "6ec8e247-2eb0-42d1-823f-75facd03988d",  # meyer
                                                "93f6a42f-1790-4af4-b5d1-8c436cb6feae",  # Teichmann-mouse-melanoma
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
