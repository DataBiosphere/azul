from typing import Type

from azul import config
from azul.indexer import BaseIndexer
import azul.plugin
from azul.project.hca.indexer import Indexer
from azul.types import JSON


class Plugin(azul.plugin.Plugin):

    def indexer_class(self) -> Type[BaseIndexer]:
        return Indexer

    def dss_subscription_query(self, prefix: str) -> JSON:
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
                                "field": "files.project_json"
                            }
                        },
                        *self._prefix_clause(prefix),
                        *(
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
                                                        "2cd14cf5-f8e0-4c97-91a2-9e8957f41ea8",  # tabulamuris
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
                                                "range": {
                                                    "manifest.version": {
                                                        "gte": "2019-04-03"
                                                    }
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
                                                        "1630e3dc-5501-4faf-9726-2e2c0b4da6d7",  # ???
                                                        "fd1d163d-d6a7-41cd-b3bc-9d77ba9a36fe",  # peer
                                                        "2a0faf83-e342-4b1c-bb9b-cf1d1147f3bb",  # treutlein
                                                        "cf8439db-fcc9-44a8-b66f-8ffbf729bffa",  # meyer
                                                        "6b9f514d-d738-403f-a9c2-62580bbe5c83",  # Q4_DEMO-…
                                                        "311d013c-01e4-42c0-9c2d-25472afa9cbc",  # Q4_DEMO-…
                                                        "d237ed6a-3a7f-4a91-b300-b070888a8542",  # DCP_Infrastructure_Test_
                                                        "e6cc0b02-2125-4faa-9903-a9025a62efec",  # Q4_DEMO-…
                                                        "e4dbcb98-0562-4071-8bea-5e8de5f3c147",  # Q4_DEMO-…
                                                        "e79e9284-c337-4dfd-853d-66fa3facfbbd",  # 10x_prod_test_01_08_2019
                                                        "560cd061-9165-4699-bc6e-8253e164c079",  # ss2_prod_test_01_08_2019
                                                        "e83fda0e-6515-4f13-82cb-a5860ecfc2d4",  # prod/10x/2019-01-22T20:35:32Z
                                                        "9a60e8c2-32ea-4586-bc1f-7ee58f462b07",  # prod/Smart-seq2/2019-01-22T20:35:33Z
                                                        "71a6e049-4846-4c2a-8823-cc193c573efc",  # prod/Smart-seq2/2019-01-22T18:13:48Z
                                                        "4b5a2268-507c-46e6-bab0-3efb30145e85",  # prod/10x/2019-01-22T18:44:02Z
                                                        "364ebb73-652e-4d32-8938-1c922d0b2584",  # prod/Smart-seq2/2019-01-22T19:15:02Z
                                                        "11f5d59b-0e2c-4f01-85ac-8d8dd3db53be",  # prod/Smart-seq2/2019-01-22T18:44:03Z
                                                        "c1996526-6466-40ff-820f-dad4d63492ec",  # prod/10x/2019-01-29T22:06:07Z
                                                        "c281dedc-e838-4464-bf51-1cc4efae3fb9",  # prod/10x/2019-01-29T23:14:07Z
                                                        "40afcf6b-422a-47ba-ba7a-33678c949b5c",  # prod/10x/2019-01-30T05:22:07Z
                                                        "71a6e049-4846-4c2a-8823-cc193c573efc",  # prod/Smart-seq2/2019-01-22T18:13:48Z
                                                        "9a60e8c2-32ea-4586-bc1f-7ee58f462b07",  # prod/Smart-seq2/2019-01-22T20:35:33Z
                                                        "0facfacd-5b0c-4228-8be5-37aa1f3a269d",  # prod/Smart-seq2/2019-01-29T22:06:07Z
                                                        "76c209df-42bf-41dc-a5f5-3d27193ca7a6",  # prod/Smart-seq2/2019-01-29T23:14:07Z
                                                        "bb409c34-bb87-4ed2-adaf-6d1ef10610b5",  # prod/Smart-seq2/2019-01-30T05:22:07Z
                                                        "1a6b5e5d-914f-4dd6-8817-a1f9b7f364d5",  # prod/10x/2019-02-12T18:43:49Z
                                                        "dd401943-1059-4b2d-b187-7a9e11822f95",  # prod/Smart-seq2/2019-02-12T18:43:49Z
                                                        "209e6402-d854-49ea-815f-421dae5e3f4d",  # Tissue stability, https://github.com/HumanCellAtlas/data-store/issues/1976
                                                        "6ac9d7b5-f86d-4c23-82a9-485a6642b278",  # Tissue Sensitivity, https://github.com/DataBiosphere/azul/issues/870
                                                        "f396fa53-2a2d-4b8a-ad18-03bf4bd46833",  # Mouse Melanoma
                                                        "f8880be0-210c-4aa3-9348-f5a423e07421",  # Old neuron_diff
                                                        "0c7bbbce-3c70-4d6b-a443-1b92c1f205c8",  # BM_PC
                                                        "e8642221-4c2c-4fd7-b926-a68bce363c88",  # Old pancreas6decades
                                                        "5e6c0ede-5648-4b0e-83ce-4644c437b4c0",  # Old pancreas6decades
                                                        "dadbdb31-5f69-485b-85f3-b244b74123f1",  # Old pancreas6decades
                                                        "1a0f98b8-746a-489d-8af9-d5c657482aab",  # Old EMTAB5061
                                                        "d96c2451-6e22-441f-a3e6-70fd0878bb1b",  # "cerebral organoid", superseded by 005d611a-14d5-4fbf-846e-571a1f874f70
                                                        "5dfe932f-159d-4cab-8039-d32f22ffbbc2",  # Tissue Sensitivity "meyer", superseded by c4077b3c-5c98-4d26-a614-246d12c2e5d7
                                                        "ccd8370a-84b8-464d-a87e-e688ac3e4f62",  # Mouse Melanoma, superseded by 8c3c290d-dfff-4553-8868-54ce45f4ba7f
                                                        "34ec62a2-9643-430d-b41a-1e342bd615fc",  # kidney_biopsy_scRNA-seq "humphreys", superseded by 027c51c6-0719-469f-a7f5-640fe57cbece
                                                        "c765e3f9-7cfc-4501-8832-79e5f7abd321",  # cardiomyocytes_basu "basu", superseded by a9c022b4-c771-4468-b769-cabcf9738de3
                                                        "e1f2a0e4-1ec8-431e-a6df-c975b3a1131f",  # bone marrow "ido_amit", superseded by a29952d9-925e-40f4-8a1c-274f118f1f51
                                                        "b6dc9b93-929a-45d0-beb2-5cf8e64872fe",  # neuron_diff, superseded by 2043c65a-1cf8-4828-a656-9e247d4e64f1
                                                        "0ec2b05f-ddbe-4e5a-b30f-e81f4b1e330c",  # CD4+_lymphocytes "EGEOD106540", superseded by 90bd6933-40c0-48d4-8d76-778c103bf545
                                                        "ff481f29-3d0b-4533-9de2-a760c61c162d",  # "10x-mouse-brain", superseded by 74b6d569-3b11-42ef-b6b1-a0454522b4a0
                                                    ]
                                                }
                                            },
                                            {
                                                "terms": {
                                                    "files.project_json.hca_ingest.document_id": [
                                                        "bae45747-546a-4aed-9377-08e9115a8fb8",  # Q4_DEMO-…
                                                        "7cb4940d-7c85-43d1-b2f5-1d99813e65df",  # Q4_DEMO-…
                                                        "6ec8e247-2eb0-42d1-823f-75facd03988d",  # meyer
                                                        "93f6a42f-1790-4af4-b5d1-8c436cb6feae",  # Teichmann-mouse-melanoma
                                                        "6504d48c-1610-43aa-8cf8-214a960e110c",  # duplicate of Regev-ICA
                                                    ]
                                                }
                                            },
                                            {
                                                "bool": {
                                                    "must": [
                                                        {
                                                            "exists": {
                                                                "field": "files.analysis_process_json"  # exclude secondary bundles from …
                                                            }
                                                        },
                                                        {
                                                            "terms": {
                                                                "files.project_json.provenance.document_id": [
                                                                    "179bf9e6-5b33-4c5b-ae26-96c7270976b8",  # 1m immune cells aka "Regev-ICA"
                                                                    "29f53b7e-071b-44b5-998a-0ae70d0229a4",  # human hematopoietic profiling "peer"
                                                                ]
                                                            }
                                                        }
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

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "admin_deleted": True
                            }
                        },
                        *self._prefix_clause(prefix)
                    ]
                }
            }
        }

    def _prefix_clause(self, prefix):
        return [
            {
                'prefix': {
                    'uuid': prefix
                }
            }
        ] if prefix else []
