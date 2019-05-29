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
                                        "must": [
                                            {
                                                "terms": {
                                                    "files.project_json.provenance.document_id": [
                                                        "179bf9e6-5b33-4c5b-ae26-96c7270976b8",  # 1m immune cells
                                                        "ff481f29-3d0b-4533-9de2-a760c61c162d",  # 1m neurons
                                                        "f8880be0-210c-4aa3-9348-f5a423e07421",  # an in vitro model of human inhibitory interneuron differentiation produced over time
                                                        "0c7bbbce-3c70-4d6b-a443-1b92c1f205c8",  # bm_pc
                                                        "0ec2b05f-ddbe-4e5a-b30f-e81f4b1e330c",  # cd4+ cytotoxic t lymphocytes
                                                        "c765e3f9-7cfc-4501-8832-79e5f7abd321",  # drop-seq, dronc-seq, fluidigm c1 comparison
                                                        "aabbec1a-1215-43e1-8e42-6489af25c12c",  # fetal/maternal interface
                                                        "1a0f98b8-746a-489d-8af9-d5c657482aab",  # healthy and type 2 diabetes pancreas
                                                        "d96c2451-6e22-441f-a3e6-70fd0878bb1b",  # hpsi human cerebral organoids
                                                        "29f53b7e-071b-44b5-998a-0ae70d0229a4",  # human hematopoietic profiling
                                                        "34ec62a2-9643-430d-b41a-1e342bd615fc",  # kidney biopsy scrna-seq
                                                        "f396fa53-2a2d-4b8a-ad18-03bf4bd46833",  # mouse melanoma
                                                        "5f256182-5dfc-4070-8404-f6fa71d37c73",  # multiplexed scrna-seq with barcoded antibodies
                                                        "b6dc9b93-929a-45d0-beb2-5cf8e64872fe",  # single cell rnaseq characterization of cell types produced over time in an in vitro model of human inhibitory interneuron differentiation.
                                                        "e8642221-4c2c-4fd7-b926-a68bce363c88",  # single cell transcriptome analysis of human pancreas
                                                        "f306435b-4e60-4a79-83a1-159bda5a2c79",  # tabula muris
                                                        "5dfe932f-159d-4cab-8039-d32f22ffbbc2",  # tissue stability
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
