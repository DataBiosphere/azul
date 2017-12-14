from flask import Flask, jsonify

app = Flask(__name__)


# this is a flask mock-up of the blue box and used for testing purposes

@app.route('/')
def hello_world():
    return 'Hello, World!'


@app.route('/v1/bundles/<bundle_uuid>')
def give_bundle(bundle_uuid):
    return jsonify(
        {
            "bundle": {
                "creator_uid": 1,
                "files": [
                    {
                        "name": "assay.json",
                        "uuid": "c1fb1206-7c6a-408c-b056-91eedb3f7a19",
                        "version": "2017-09-22T001550.273020Z",
                        "content-type": "application/json",
                        "indexed": True,
                        "crc32c": "bb52aea3",
                        "s3-etag": "345f3c5065d77925d2aceff03de276b3",
                        "sha1": "b7132ca4a56bcee6469266f6ef612c3b3bbbc52c",
                        "sha256": "3d7d562856984cb1d8d378618baaa94b3cc44988b89"
                                  "9d2838018bf947b8d7cb8",
                        "size": 1137
                    },
                    {
                        "name": "ERR580157_1.fastq.gz",
                        "uuid": "52d4f049-2c9a-4a75-8dd4-9559902e67bd",
                        "version": "2017-09-22T001551.542119Z",
                        "content-type": "gzip",
                        "indexed": True,
                        "crc32c": "e68855a7",
                        "s3-etag": "e771bab4b85e09b9a714f53b2fca366f",
                        "sha1": "99c3ea974678720f1159fe61f77d958bb533bd7d",
                        "sha256": "c4d20c2d5e6d8276f96d7a9dc4a8df1650a2b34d70d"
                                  "0f775f35a174035fa4141",
                        "size": 11
                    },
                    {
                        "name": "ERR580157_2.fastq.gz",
                        "uuid": "cd6c128b-cf1f-49dc-b3d8-1eb39115f90e",
                        "version": "2017-09-22T001552.608139Z",
                        "content-type": "gzip",
                        "indexed": True,
                        "crc32c": "ee751fc7",
                        "s3-etag": "15d80faa6b78463c2bd9e8789b0a3d25",
                        "sha1": "b143f054b564c31f72f51b66d28bb922a0c0317d",
                        "sha256": "91f33631ff0b30c0cd1e06489436a0e6944eec20533"
                                  "8f10bf979b179b3fbb919",
                        "size": 12
                    },
                    {
                        "name": "project.json",
                        "uuid": "d1bf1d60-7aaf-44c4-b8be-52180ac98535",
                        "version": "2017-09-22T001553.697403Z",
                        "content-type": "application/json",
                        "indexed": True,
                        "crc32c": "c155a99c",
                        "s3-etag": "0cefbf7e03412a031ece2e337716eb49",
                        "sha1": "ff010c9c38c6db711c8534db11587a98a95ef5cf",
                        "sha256": "fc13c24cd7b9c3f3bb378f874c81573a5edb617078c"
                                  "6c1ab533fc9724d5c57ee",
                        "size": 5487
                    },
                    {
                        "name": "sample.json",
                        "uuid": "328229b7-5a5a-43fc-84d4-c3071d6e2d57",
                        "version": "2017-09-22T001554.790410Z",
                        "content-type": "application/json",
                        "indexed": True,
                        "crc32c": "91bb5ad5",
                        "s3-etag": "6778129b17287d80d2bd85695cdf5bd0",
                        "sha1": "0af94780add752c632f6e8531b6c661d8eec5ef0",
                        "sha256": "6135e1f93b8b6536f4893c31419d2640b0fd0d1eea9"
                                  "27b5e1c8690739f00646e",
                        "size": 1700
                    }
                ],
                "uuid": "b1db2bf9-855a-4961-ae39-be2a8d6aa864",
                "version": "2017-09-22T001555.857860Z"
            }
        }
    )


@app.route('/v1/files/c1fb1206-7c6a-408c-b056-91eedb3f7a19')
def give_file_assay():
    return jsonify(
        {
            "core": {
                "type": "assay",
                "schema_version": "3.0.0",
                "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                              "schema/assay.json"
            },
            "rna": {
                "core": {
                    "type": "rna",
                    "schema_version": "3.0.0",
                    "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                  "schema/rna.json"
                },
                "end_bias": "full_transcript",
                "primer": "random",
                "library_construction": "SMARTer Ultra Low RNA Kit",
                "spike_in": "ERCC"
            },
            "seq": {
                "core": {
                    "type": "seq",
                    "schema_version": "3.0.0",
                    "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                  "schema/seq.json"
                },
                "instrument_model": "Illumina HiSeq 2000",
                "instrument_platform": "Illumina",
                "library_construction": "Nextera XT",
                "molecule": "RNA",
                "paired_ends": "yes",
                "lanes": [
                    {
                        "number": 1,
                        "r1": "ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR580/"
                              "ERR580157/ERR580157_1.fastq.gz",
                        "r2": "ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR580/"
                              "ERR580157/ERR580157_2.fastq.gz"
                    }
                ],
                "ena_experiment": "ERX538284",
                "ena_run": "ERR580157"
            },
            "single_cell": {
                "core": {
                    "type": "single_cell",
                    "schema_version": "3.0.0",
                    "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                  "schema/single_cell.json"
                },
                "cell_handling": "Fluidigm C1"
            },
            "sample_id": "d3abdd56-8d52-44d9-938b-f349e827e06e",
            "id": "c8899599-4d25-416a-96bd-2c22c54a0c25"
        }
    )


@app.route('/v1/files/d1bf1d60-7aaf-44c4-b8be-52180ac98535')
def give_file_project():
    return jsonify(
        {
            "core": {
                "type": "project",
                "schema_version": "3.0.0",
                "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                              "schema/project.json"
            },
            "title": "Gene expression pattern of single mES cells during "
                     "cell cycle stages",
            "id": "E-MTAB-2805",
            "array_express_investigation": "E-MTAB-2805",
            "contributors": [
                {
                    "core": {
                        "type": "contact",
                        "schema_version": "3.0.0",
                        "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                      "schema/contact.json"
                    },
                    "name": "Kedar,N,Natarajan"
                }
            ],
            "ddjb_trace": "ERP006670",
            "description": "In this study, we aimed to study the gene "
                           "expression patterns at single cell level across "
                           "the different cell cycle stages in mESC. "
                           "We performed single cell RNA-Seq experiment on "
                           "mESC that were stained with Hoechst 33342 and "
                           "Flow cytometry sorted for G1, S and G2M stages of "
                           "cell cycle. Single cell RNA-Seq was performed "
                           "using Fluidigm C1 system and libraries were "
                           "generated using Nextera XT (Illumina) kit.",
            "experimental_design": [
                {
                    "text": "cell type comparison design"
                },
                {
                    "text": "in_vitro_design"
                },
                {
                    "text": "co-expression_design"
                }
            ],
            "experimental_factor_name": [
                "cell cycle stage"
            ],
            "publications": [
                {
                    "core": {
                        "type": "publication",
                        "schema_version": "3.0.0",
                        "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                      "schema/publication.json"
                    },
                    "authors": [
                        "Natarajan, K"
                    ]
                }
            ],
            "submitter": {
                "core": {
                    "type": "contact",
                    "schema_version": "3.0.0",
                    "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                  "schema/contact.json"
                },
                "city": "Hinxton",
                "country": "UK",
                "email": "kedarnat@ebi.ac.uk",
                "institution": "EMBL-EBI, Wellcome Trust Genome Campus, "
                               "Cambridge",
                "name": "Kedar,N,Natarajan"
            },
            "supplementary_files": [
                "ERCC_Sequences.txt"
            ],
            "protocols": [
                {
                    "core": {
                        "type": "protocol",
                        "schema_version": "3.0.0",
                        "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                      "schema/protocol.json"
                    },
                    "description": "mESC were grown in standard 2i media. "
                                   "The media composition was N2B27 basal "
                                   "media (NDiff 227, StemCells), 100 U/ml "
                                   "recombinant human leukemia inhibitory "
                                   "factor (Millipore), 1M PD0325901 "
                                   "(Stemgent), 3M CHIR99021 (Stemgent).",
                    "type": {
                        "text": "growth protocol"
                    },
                    "id": "e9fa2f85-5958-4db1-93e2-f8a6b4f066ca"
                },
                {
                    "core": {
                        "type": "protocol",
                        "schema_version": "3.0.0",
                        "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                      "schema/protocol.json"
                    },
                    "description": "Sorted cells corresponding to cycle "
                                   "fractions (G1, S and G2M) were collected "
                                   "and loaded on the 10-17 micron Fluidigm "
                                   "C1 Single-Cell Auto Prep IFC.",
                    "type": {
                        "text": "sample collection protocol"
                    },
                    "id": "427c4ca2-c4ec-45ce-9bfb-aa753ae5594f"
                },
                {
                    "core": {
                        "type": "protocol",
                        "schema_version": "3.0.0",
                        "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                      "schema/protocol.json"
                    },
                    "description": "For sorting of cell cycle fractions "
                                   "(G1, S and G2M), cells were trypisinized "
                                   "and stained with Hoechst 33342 (5_M) for "
                                   "30min at 37C. Cells were sorted based on "
                                   "Hoechst staining for G1, S and G2M stages "
                                   "of cell cycle.",
                    "type": {
                        "text": "treatment protocol"
                    },
                    "id": "80427428-5260-4acd-aed8-fdf3e393a034"
                },
                {
                    "core": {
                        "type": "protocol",
                        "schema_version": "3.0.0",
                        "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                      "schema/protocol.json"
                    },
                    "description": "For each cell cycle fraction, 3000 cells "
                                   "were loaded onto a 10-17 micron Fluidigm "
                                   "C1 Single-Cell Auto Prep IFC and cell "
                                   "capture was performed as per "
                                   "manufacturers protocol. Single cell "
                                   "capture, lysis, Reverse transcription and "
                                   "cDNA amplification are performed using "
                                   "the SMARTer cDNA synthesis kit (Clonetech)"
                                   " and Advantage2PCR kit (Clonetech) with "
                                   "1:100 ratio of spike in (Ambion), within "
                                   "the IFC inside the C1-Autoprep system as "
                                   "per manufacturers protocol.",
                    "type": {
                        "text": "nucleic acid extraction protocol"
                    },
                    "id": "55d09648-6607-4302-bc06-b8cd9e7630a1"
                },
                {
                    "core": {
                        "type": "protocol",
                        "schema_version": "3.0.0",
                        "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                      "schema/protocol.json"
                    },
                    "description": "Harvested cDNA from single cells after "
                                   "C1-Autoprep system run were used to make "
                                   "single cell libraries using Nextera XT "
                                   "DNA sample preparation kit (Illumina) "
                                   "utilising 96 dual barcoded indices. "
                                   "Library poolup was performed using AMPure "
                                   "XP beads (Agencourt Biosciences) and "
                                   "single cell libraries were sent for "
                                   "paired end sequencing at the Wellcome "
                                   "Trust Sequencing facility",
                    "type": {
                        "text": "nucleic acid library construction protocol"
                    },
                    "id": "b19fe364-75da-4136-9eaf-9af7426efa5d"
                },
                {
                    "core": {
                        "type": "protocol",
                        "schema_version": "3.0.0",
                        "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                      "schema/protocol.json"
                    },
                    "description": "Single cell libraries were multiplexed "
                                   "and sequenced across 4 lanes of HiSeq "
                                   "2000 (Illumina) using 100bp "
                                   "paired-end sequencing.",
                    "type": {
                        "text": "nucleic acid sequencing protocol"
                    },
                    "id": "6d2db841-811a-48bf-9638-5889b830daa7"
                },
                {
                    "core": {
                        "type": "protocol",
                        "schema_version": "3.0.0",
                        "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                      "schema/protocol.json"
                    },
                    "description": "As per Wellcome Trust Sequencing "
                                   "pipeline guidelines",
                    "type": {
                        "text": "high throughput sequence alignment protocol"
                    },
                    "id": "48743d35-375b-4211-abb7-07ccde5e90a4"
                },
                {
                    "core": {
                        "type": "protocol",
                        "schema_version": "3.0.0",
                        "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                      "schema/protocol.json"
                    },
                    "description": "Mapping for each single cell dataset was "
                                   "done using GSNAP/GMAP "
                                   "(released on 2014-01-21) to a custom "
                                   "mouse genome (mm10; Ensembl GRCm38.p1) "
                                   "including ERCC sequences. Mapped reads "
                                   "were counted using HTSeq (0.6.1)",
                    "type": {
                        "text": "normalization data transformation protocol"
                    },
                    "id": "322d3d98-5707-4199-914c-c99ca6ec8050"
                },
                {
                    "core": {
                        "type": "protocol",
                        "schema_version": "3.0.0",
                        "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                      "schema/protocol.json"
                    },
                    "description": "fresh",
                    "type": {
                        "text": "storage protocol"
                    },
                    "id": "P-LOCL-9"
                }
            ]
        }
    )


@app.route('/v1/files/328229b7-5a5a-43fc-84d4-c3071d6e2d57')
def give_file_sample():
    return jsonify(
        {
            "core": {
                "type": "sample",
                "schema_version": "3.0.0",
                "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                              "schema/sample.json"
            },
            "body_part": {
                "text": "embryonic stem cell"
            },
            "cell_line": {
                "core": {
                    "type": "cell_line",
                    "schema_version": "3.0.0",
                    "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                  "schema/cell_line.json"
                },
                "name": {
                    "text": "AB2.2"
                }
            },
            "culture_type": "cell line",
            "donor": {
                "core": {
                    "type": "donor",
                    "schema_version": "3.0.0",
                    "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                  "schema/donor.json"
                },
                "species": {
                    "ontology": "10090",
                    "text": "Mus musculus"
                },
                "is_living": "no"
            },
            "organ": {
                "ontology": "UBERON_0000922",
                "text": "embryo"
            },
            "preservation": {
                "core": {
                    "type": "preservation",
                    "schema_version": "3.0.0",
                    "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                  "schema/preservation.json"
                },
                "storage_protocol": "P-LOCL-9"
            },
            "supplementary_files": [
                "ftp://ftp.ebi.ac.uk/pub/databases/microarray/data/"
                "experiment/MTAB/E-MTAB-2805/E-MTAB-2805.processed.1.zip"
            ],
            "total_estimated_cells": 1,
            "well": {
                "core": {
                    "type": "well",
                    "schema_version": "3.0.0",
                    "schema_url": "http://hgwdev.soe.ucsc.edu/~kent/hca/"
                                  "schema/well.json"
                },
                "cell_type": {
                    "text": "embyronic stem cell"
                },
                "name": "G1_cell1"
            },
            "protocol_ids": [
                "e9fa2f85-5958-4db1-93e2-f8a6b4f066ca",
                "427c4ca2-c4ec-45ce-9bfb-aa753ae5594f",
                "80427428-5260-4acd-aed8-fdf3e393a034",
                "55d09648-6607-4302-bc06-b8cd9e7630a1",
                "b19fe364-75da-4136-9eaf-9af7426efa5d",
                "6d2db841-811a-48bf-9638-5889b830daa7",
                "48743d35-375b-4211-abb7-07ccde5e90a4",
                "322d3d98-5707-4199-914c-c99ca6ec8050"
            ],
            "project_id": "E-MTAB-2805",
            "cell_cycle": {
                "text": "G1 phase"
            },
            "name": "G1 phase mESCs",
            "title": "G1 phase mouse embryonic stem cell line AB2.2",
            "id": "d3abdd56-8d52-44d9-938b-f349e827e06e",
            "ena_sample": "ERS527385",
            "submitter_id": "G1_cell1"
        }
    )


@app.route('/v1/files/<file_uuid>')
def give_file():
    return give_file_assay()

