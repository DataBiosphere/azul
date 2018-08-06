import unittest
import json

# use this file to unittest app.py
# requirements: app.py, chalicelib/, running test_flask.py,
# declared env var: AZUL_ES_INDEX = test-index

# bb_host = "https://"+os.environ['BLUE_BOX_ENDPOINT']
# in_host = "https://"+os.environ['INDEXER_ENDPOINT']

# Input
bundle_uuid = 'b1db2bf9-855a-4961-ae39-be2a8d6aa864'
file_uuid = 'c1fb1206-7c6a-408c-b056-91eedb3f7a19'
json_file = json.loads(
    '{"core":{"type":"assay","schema_version":"3.0.0",'
    '"schema_url":"http://hgwdev.soe.ucsc.edu/~kent/hca/schema/assay.json"},'
    '"rna":{"core":{"type":"rna","schema_version":"3.0.0",'
    '"schema_url":"http://hgwdev.soe.ucsc.edu/~kent/hca/schema/rna.json"},'
    '"end_bias":"full_transcript","primer":"random",'
    '"library_construction":"SMARTer Ultra Low RNA Kit","spike_in":"ERCC"},'
    '"seq":{"core":{"type":"seq","schema_version":"3.0.0",'
    '"schema_url":"http://hgwdev.soe.ucsc.edu/~kent/hca/schema/seq.json"},'
    '"instrument_model":"Illumina HiSeq 2000",'
    '"instrument_platform":"Illumina",'
    '"library_construction":"Nextera XT","molecule":"RNA","paired_ends":"yes",'
    '"lanes":[{"number":1,"r1":"ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR580/'
    'ERR580157/ERR580157_1.fastq.gz",'
    '"r2":"ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR580/ERR580157/'
    'ERR580157_2.fastq.gz"}],"ena_experiment":"ERX538284",'
    '"ena_run":"ERR580157"},"single_cell":{"core":{"type":"single_cell",'
    '"schema_version":"3.0.0","schema_url":"http://hgwdev.soe.ucsc.edu/~kent/'
    'hca/schema/single_cell.json"},"cell_handling":"Fluidigm C1"},'
    '"sample_id":"d3abdd56-8d52-44d9-938b-f349e827e06e",'
    '"id":"c8899599-4d25-416a-96bd-2c22c54a0c25"}')
c_item1 = 'id*text_autocomplete*keyword'
c_item2 = {"rna": {"primer": "random*keyword*text", "spike_in": "ERCC"}}
to_flatten = [1, [2], [3, [4], [5, [6]], 7], 8]

# Expected Output
expected_bundle = '{"json_files": ' \
                  '[{"assay.json": ' \
                  '"c1fb1206-7c6a-408c-b056-91eedb3f7a19"},{"project.json": ' \
                  '"d1bf1d60-7aaf-44c4-b8be-52180ac98535"}, ' \
                  '{"sample.json": ' \
                  '"328229b7-5a5a-43fc-84d4-c3071d6e2d57"}], "data_files": ' \
                  '[{"ERR580157_1.fastq.gz": {"content-type": "gzip", ' \
                  '"crc32c": "e68855a7", "indexed": true, ' \
                  '"name": "ERR580157_1.fastq.gz", "s3-etag": ' \
                  '"e771bab4b85e09b9a714f53b2fca366f", "sha1": ' \
                  '"99c3ea974678720f1159fe61f77d958bb533bd7d", "sha256": ' \
                  '"c4d20c2d5e6d8276f96d7a9dc4a8df1650a2b34d70d0f775f35a174' \
                  '035fa4141", "size": 11, "uuid": ' \
                  '"52d4f049-2c9a-4a75-8dd4-9559902e67bd", "version": ' \
                  '"2017-09-22T001551.542119Z"}}, {"ERR580157_2.fastq.gz": ' \
                  '{"content-type": "gzip", "crc32c": "ee751fc7", ' \
                  '"indexed": true, "name": "ERR580157_2.fastq.gz", ' \
                  '"s3-etag": "15d80faa6b78463c2bd9e8789b0a3d25", ' \
                  '"sha1": "b143f054b564c31f72f51b66d28bb922a0c0317d", ' \
                  '"sha256": "91f33631ff0b30c0cd1e06489436a0e6944eec205338f1' \
                  '0bf979b179b3fbb919", "size": 12, ' \
                  '"uuid": "cd6c128b-cf1f-49dc-b3d8-1eb39115f90e", ' \
                  '"version": "2017-09-22T001552.608139Z"}}]}'
expected_file = '{"core":{"type":"assay","schema_version":"3.0.0",' \
                '"schema_url":"http://hgwdev.soe.ucsc.edu/~kent/hca/schema' \
                '/assay.json"},"rna":{"core":{"type":"rna",' \
                '"schema_version":"3.0.0","schema_url":' \
                '"http://hgwdev.soe.ucsc.edu/~kent/hca/schema/rna.json"},' \
                '"end_bias":"full_transcript","primer":"random",' \
                '"library_construction":"SMARTer Ultra Low RNA Kit",' \
                '"spike_in":"ERCC"},"seq":{"core":{"type":"seq",' \
                '"schema_version":"3.0.0",' \
                '"schema_url":"http://hgwdev.soe.ucsc.edu/~kent/hca/schema' \
                '/seq.json"},"instrument_model":"Illumina HiSeq 2000",' \
                '"instrument_platform":"Illumina",' \
                '"library_construction":"Nextera XT","molecule":"RNA",' \
                '"paired_ends":"yes",' \
                '"lanes":[{"number":1,"r1":"ftp://ftp.sra.ebi.ac.uk/vol1/' \
                'fastq/ERR580/ERR580157/ERR580157_1.fastq.gz",' \
                '"r2":"ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR580/ERR580157/' \
                'ERR580157_2.fastq.gz"}],"ena_experiment":"ERX538284",' \
                '"ena_run":"ERR580157"},"single_cell":{"core":' \
                '{"type":"single_cell","schema_version":"3.0.0",' \
                '"schema_url":"http://hgwdev.soe.ucsc.edu/~kent/hca/schema/' \
                'single_cell.json"},"cell_handling":"Fluidigm C1"},' \
                '"sample_id":"d3abdd56-8d52-44d9-938b-f349e827e06e",' \
                '"id":"c8899599-4d25-416a-96bd-2c22c54a0c25"}'
expected_look1 = json.loads('{"id": "c8899599-4d25-416a-96bd-2c22c54a0c25"}')
expected_look2 = [{'rna|primer': 'random'}, {'rna|spike_in': 'ERCC'}]
expected_flatten = [1, 2, 3, 4, 5, 6, 7, 8]
expected_es = "You Know, for Search"
expected_index = [{"assay,json|single_cell|cell_handling": "Fluidigm C1",
                   "assay,json|rna|library_construction":
                       "SMARTer Ultra Low RNA Kit",
                   "assay,json|rna|spike_in": "ERCC",
                   "assay,json|seq|paired_ends": "yes",
                   "assay,json|seq|instrument_platform": "Illumina",
                   "assay,json|seq|library_construction": "Nextera XT",
                   "project,json|id": "E-MTAB-2805",
                   "project,json|submitter|name": "Kedar,N,Natarajan",
                   "project,json|submitter|institution":
                       "EMBL-EBI, Wellcome Trust Genome Campus, Cambridge",
                   "project,json|submitter|country": "UK",
                   "sample,json|body_part|text": "embryonic stem cell",
                   "sample,json|donor|species|text": "Mus musculus",
                   "sample,json|donor|species|ontology": "10090",
                   "sample,json|donor|is_living": "no",
                   "sample,json|donor|core|uuid": "None",
                   "sample,json|name": "G1 phase mESCs",
                   "sample,json|organ|text": "embryo",
                   "sample,json|project_id": "E-MTAB-2805",
                   "sample,json|cell_cycle|text": "G1 phase",
                   "sample,json|culture_type": "cell line",
                   "sample,json|donor_id": "None",
                   "bundle_uuid": "b1db2bf9-855a-4961-ae39-be2a8d6aa864",
                   "file_name": "ERR580157_1.fastq.gz",
                   "file_uuid": "52d4f049-2c9a-4a75-8dd4-9559902e67bd",
                   "file_version": "2017-09-22T001551.542119Z",
                   "file_format": "fastq.gz",
                   "bundle_type": "scRNA-Seq Upload", "file_size": 12,
                   "analysis,json|computational_method": "None"},
                  {"assay,json|single_cell|cell_handling": "Fluidigm C1",
                   "assay,json|rna|library_construction":
                       "SMARTer Ultra Low RNA Kit",
                   "assay,json|rna|spike_in": "ERCC",
                   "assay,json|seq|paired_ends": "yes",
                   "assay,json|seq|instrument_platform": "Illumina",
                   "assay,json|seq|library_construction": "Nextera XT",
                   "project,json|id": "E-MTAB-2805",
                   "project,json|submitter|name": "Kedar,N,Natarajan",
                   "project,json|submitter|institution":
                       "EMBL-EBI, Wellcome Trust Genome Campus, Cambridge",
                   "project,json|submitter|country": "UK",
                   "sample,json|body_part|text": "embryonic stem cell",
                   "sample,json|donor|species|text": "Mus musculus",
                   "sample,json|donor|species|ontology": "10090",
                   "sample,json|donor|is_living": "no",
                   "sample,json|donor|core|uuid": "None",
                   "sample,json|name": "G1 phase mESCs",
                   "sample,json|organ|text": "embryo",
                   "sample,json|project_id": "E-MTAB-2805",
                   "sample,json|cell_cycle|text": "G1 phase",
                   "sample,json|culture_type": "cell line",
                   "sample,json|donor_id": "None",
                   "bundle_uuid": "b1db2bf9-855a-4961-ae39-be2a8d6aa864",
                   "file_name": "ERR580157_2.fastq.gz",
                   "file_uuid": "cd6c128b-cf1f-49dc-b3d8-1eb39115f90e",
                   "file_version": "2017-09-22T001552.608139Z",
                   "file_format": "fastq.gz",
                   "bundle_type": "scRNA-Seq Upload", "file_size": 12,
                   "analysis,json|computational_method": "None"}]


@unittest.skip('https://github.com/DataBiosphere/azul/issues/177')
class TestIndexer(unittest.TestCase):
    def test_bundles(self):
        tbundle = (str(get_bundles(bundle_uuid)))
        self.assertEqual(json.loads(tbundle), json.loads(expected_bundle))

    def test_file(self):
        tfile = (str(get_file(file_uuid)))
        self.assertEqual(json.loads(tfile), json.loads(expected_file))

    def test_flatten(self):
        tflatten = flatten(to_flatten)
        lflatten = (list(tflatten))
        self.assertEqual(lflatten, expected_flatten)

    def test_look(self):
        name = ""
        print(len(name))
        # test look 1 (simple)
        tlook = look_file(c_item1, json_file, name)
        self.assertEqual(tlook, expected_look1)
        # test look 2 (nested)
        tlook = look_file(c_item2, json_file, name)
        self.assertEqual(tlook, expected_look2)

    def test_es(self):
        tes = json.loads(es_check())
        tag = tes['tagline']
        self.assertEqual(tag, expected_es)

    def test_write_es(self):
        es.indices.create(index='test-index', ignore=[400])
        es_json = [{"test": "hello world"}]
        write_es(es_json, '123')
        twrite = es.get(id='123', index='test-index')
        source = twrite['_source']
        self.assertEqual(source, {"test": "hello world"})
        es.delete(id='123', index='test-index', doc_type='document')

    def test_write_index(self):
        write_index(bundle_uuid)
        file_ids = [
            'b1db2bf9-855a-4961-ae39-be2a8d6aa864:52d4f049-2c9a-4a75-8dd4-'
            '9559902e67bd:2017-09-22T001551.542119Z',
            'b1db2bf9-855a-4961-ae39-be2a8d6aa864:cd6c128b-cf1f-49dc-b3d8-'
            '1eb39115f90e:2017-09-22T001552.608139Z']
        for i in range(1, len(file_ids)):
            twrite = es.get(id=file_ids[i], index='test-index')
            source = twrite['_source']
            self.assertEqual(source, expected_index[i])
            es.delete(id=file_ids[i], index='test-index', doc_type='document')

            # need to clean up code on app.py first
            # def test_post_notification(self):
            #     every_day()
            #
            #     post_notification()
            #     twrite = es.get(id='123', index='test-index')
            #     source = twrite['_source']
            #     self.assertEqual(source, expected_index)
            #     es.delete(id='123', index='test-index', doc_type='document')

if __name__ == '__main__':
    unittest.main()

