#!/usr/bin/env python

import unittest
import os
import pandas as pd
from StringIO import StringIO
import zipfile
# import numpy as np
from bagitutils import BagHandler
from pandas.util.testing import assert_frame_equal


class TestBagHandlerMethods(unittest.TestCase):

    def setUp(self):
        """Load normalized test data into Pandas dataframe"""
        fpath = 'test/test_normalize_df_mock.tsv'
        try:
            df = pd.read_csv(
                fpath,
                sep='\t')
        except IOError:
            print('Cannot open file')
        self.normalized = df

    def test_normalize(self):
        """ """
        # fpath = 'test/test_normalize_df_mock.tsv'
        # df = pd.read_csv(fpath, sep='\t')
        # args = dict([('data', df),
        #              ('bag_info', 'test'),
        #              ('bag_path', '~/dev/manifest-handover')])
        # bag = BagHandler(**args)
        # df_test = bag._BagHandler__normalize()
        # assert_frame_equal(self.normalized, df)

    def test_worksWithString(self):
        s = StringIO("Program	Project	Center Name	Submitter Donor ID	Donor UUID	Submitter Donor Primary Site	Submitter Specimen ID	Specimen UUID	Submitter Specimen Type	Submitter Experimental Design	Submitter Sample ID	Sample UUID	Analysis Type	Workflow Name	Workflow Version	File Type	File Path	Upload File ID	Data Bundle UUID	Metadata.json	File URLs	File DOS URL\n\
        NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study	Framingham	Broad	20428	09df7aef-246a-57eb-9685-e1d4d18b55ab	BLOOD	SRS1353998	2923638f-0784-5704-8d93-5b97b4ca3092	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	NWD692354	dd8337dd-f731-5c3b-9a03-bdae77ca47a9	alignment	topmed-spinnaker	Alpha Build 1	crai	NWD692354.b38.irc.v1.cram.crai	5a00cc38-2f8d-4d34-98e0-0a847579b988	50cfaf90-0998-5ef5-aa0b-cfaea71d5a7d		[u'gs://topmed-irc-share/genomes/NWD692354.b38.irc.v1.cram.crai', u's3://nih-nhlbi-datacommons/NWD692354.b38.irc.v1.cram.crai']	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/5a00cc38-2f8d-4d34-98e0-0a847579b988?version=2018-02-28T160411.061319Z\n\
        NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study	Framingham	Broad	20428	09df7aef-246a-57eb-9685-e1d4d18b55ab	BLOOD	SRS1353998	2923638f-0784-5704-8d93-5b97b4ca3092	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	NWD692354	dd8337dd-f731-5c3b-9a03-bdae77ca47a9	alignment	topmed-spinnaker	Alpha Build 1	cram	NWD692354.b38.irc.v1.cram	b4cf8998-34a1-4e00-aa23-bcdf8d6b23b5	50cfaf90-0998-5ef5-aa0b-cfaea71d5a7d		[u's3://nih-nhlbi-datacommons/NWD692354.b38.irc.v1.cram', u'gs://topmed-irc-share/genomes/NWD692354.b38.irc.v1.cram']	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/b4cf8998-34a1-4e00-aa23-bcdf8d6b23b5?version=2018-02-28T160408.957538Z\n")
        pd.read_csv(s, sep='\t')

    def test_bagHandler(self):
        s = StringIO("Program	Project	Center Name	Submitter Donor ID	Donor UUID	Submitter Donor Primary Site	Submitter Specimen ID	Specimen UUID	Submitter Specimen Type	Submitter Experimental Design	Submitter Sample ID	Sample UUID	Analysis Type	Workflow Name	Workflow Version	File Type	File Path	Upload File ID	Data Bundle UUID	Metadata.json	File URLs	File DOS URL\n\
        NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study	Framingham	Broad	20428	09df7aef-246a-57eb-9685-e1d4d18b55ab	BLOOD	SRS1353998	2923638f-0784-5704-8d93-5b97b4ca3092	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	NWD692354	dd8337dd-f731-5c3b-9a03-bdae77ca47a9	alignment	topmed-spinnaker	Alpha Build 1	crai	NWD692354.b38.irc.v1.cram.crai	5a00cc38-2f8d-4d34-98e0-0a847579b988	50cfaf90-0998-5ef5-aa0b-cfaea71d5a7d		[u'gs://topmed-irc-share/genomes/NWD692354.b38.irc.v1.cram.crai', u's3://nih-nhlbi-datacommons/NWD692354.b38.irc.v1.cram.crai']	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/5a00cc38-2f8d-4d34-98e0-0a847579b988?version=2018-02-28T160411.061319Z\n\
        NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study	Framingham	Broad	20428	09df7aef-246a-57eb-9685-e1d4d18b55ab	BLOOD	SRS1353998	2923638f-0784-5704-8d93-5b97b4ca3092	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	NWD692354	dd8337dd-f731-5c3b-9a03-bdae77ca47a9	alignment	topmed-spinnaker	Alpha Build 1	cram	NWD692354.b38.irc.v1.cram	b4cf8998-34a1-4e00-aa23-bcdf8d6b23b5	50cfaf90-0998-5ef5-aa0b-cfaea71d5a7d		[u's3://nih-nhlbi-datacommons/NWD692354.b38.irc.v1.cram', u'gs://topmed-irc-share/genomes/NWD692354.b38.irc.v1.cram']	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/b4cf8998-34a1-4e00-aa23-bcdf8d6b23b5?version=2018-02-28T160408.957538Z\n")
        bag = BagHandler(data=s, bag_info={}, bag_name='manifest')
        zip_name = bag.create_bag()
        os.remove(zip_name)

    def test_zipRootIsManifest(self):
        s = StringIO("Program	Project	Center Name	Submitter Donor ID	Donor UUID	Submitter Donor Primary Site	Submitter Specimen ID	Specimen UUID	Submitter Specimen Type	Submitter Experimental Design	Submitter Sample ID	Sample UUID	Analysis Type	Workflow Name	Workflow Version	File Type	File Path	Upload File ID	Data Bundle UUID	Metadata.json	File URLs	File DOS URL\n\
        NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study	Framingham	Broad	20428	09df7aef-246a-57eb-9685-e1d4d18b55ab	BLOOD	SRS1353998	2923638f-0784-5704-8d93-5b97b4ca3092	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	NWD692354	dd8337dd-f731-5c3b-9a03-bdae77ca47a9	alignment	topmed-spinnaker	Alpha Build 1	crai	NWD692354.b38.irc.v1.cram.crai	5a00cc38-2f8d-4d34-98e0-0a847579b988	50cfaf90-0998-5ef5-aa0b-cfaea71d5a7d		[u'gs://topmed-irc-share/genomes/NWD692354.b38.irc.v1.cram.crai', u's3://nih-nhlbi-datacommons/NWD692354.b38.irc.v1.cram.crai']	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/5a00cc38-2f8d-4d34-98e0-0a847579b988?version=2018-02-28T160411.061319Z\n\
        NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study	Framingham	Broad	20428	09df7aef-246a-57eb-9685-e1d4d18b55ab	BLOOD	SRS1353998	2923638f-0784-5704-8d93-5b97b4ca3092	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	NWD692354	dd8337dd-f731-5c3b-9a03-bdae77ca47a9	alignment	topmed-spinnaker	Alpha Build 1	cram	NWD692354.b38.irc.v1.cram	b4cf8998-34a1-4e00-aa23-bcdf8d6b23b5	50cfaf90-0998-5ef5-aa0b-cfaea71d5a7d		[u's3://nih-nhlbi-datacommons/NWD692354.b38.irc.v1.cram', u'gs://topmed-irc-share/genomes/NWD692354.b38.irc.v1.cram']	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/b4cf8998-34a1-4e00-aa23-bcdf8d6b23b5?version=2018-02-28T160408.957538Z\n")
        bag = BagHandler(data=s, bag_info={}, bag_name='manifest')
        zip_name = bag.create_bag()
        with zipfile.ZipFile(zip_name) as myzip:
            for name in myzip.namelist():
                self.assertTrue(name.startswith('manifest'))
                if 'participant' in name:
                    self.assertIn('data/', name)
        os.remove(zip_name)

    def testDemoData(self):
        data = ("Program	Project	Center Name	Submitter Donor ID	Donor UUID	Submitter Donor Primary Site	Submitter Specimen ID	Specimen UUID	Submitter Specimen Type	Submitter Experimental Design	Submitter Sample ID	Sample UUID	Analysis Type	Workflow Name	Workflow Version	File Type	File Path	Upload File ID	Data Bundle UUID	Metadata.json	File URLs	File DOS URI\n\
NIH Data Commons	NIH Data Commons Pilot	Broad Public Datasets	ABC123456	c2b4c298-4d80-4aaa-bddf-20c15d184af3	Blood	NA12878_2	bfcc3266-340a-5751-8db1-d661163ac8e5	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	H06JUADXX130110_1	c774934f-4100-44bf-8df9-8d4e509c088d	none	test workflow	Development	bam	H06JUADXX130110.1.ATCACGAT.20k_reads.bam	60936d97-6358-4ce3-8136-d5776186ee21	dd04fbf3-2a51-4c72-8038-da7094b8da55		gs://broad-public-datasets/NA12878_downsampled_for_testing/unmapped/H06JUADXX130110.1.ATCACGAT.20k_reads.bam	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/60936d97-6358-4ce3-8136-d5776186ee21?version=2018-03-23T123738.145535Z")
        bag = BagHandler(data=data, bag_info={}, bag_name='manifest')
        (participants, sample) = bag.transform_to_participant_and_sample()
        self.assertListEqual(
            sorted(['c2b4c298-4d80-4aaa-bddf-20c15d184af3']),
            sorted(participants))
        self.assertEquals(len(sample), 1)
        row = sample[0]
        self.assertEquals(row['participant'], 'c2b4c298-4d80-4aaa-bddf-20c15d184af3')
        self.assertEquals(row['gs_url1'], 'gs://broad-public-datasets/NA12878_downsampled_for_testing/unmapped/H06JUADXX130110.1.ATCACGAT.20k_reads.bam')
        self.assertFalse('s3_url1' in row)

    def testWriteCsv(self):
        data = ("Program	Project	Center Name	Submitter Donor ID	Donor UUID	Submitter Donor Primary Site	Submitter Specimen ID	Specimen UUID	Submitter Specimen Type	Submitter Experimental Design	Submitter Sample ID	Sample UUID	Analysis Type	Workflow Name	Workflow Version	File Type	File Path	Upload File ID	Data Bundle UUID	Metadata.json	File URLs	File DOS URI\n\
        NIH Data Commons	NIH Data Commons Pilot	Broad Public Datasets	ABC123456	c2b4c298-4d80-4aaa-bddf-20c15d184af3	Blood	NA12878_2	bfcc3266-340a-5751-8db1-d661163ac8e5	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	H06JUADXX130110_1	c774934f-4100-44bf-8df9-8d4e509c088d	none	test workflow	Development	bam	H06JUADXX130110.1.ATCACGAT.20k_reads.bam	60936d97-6358-4ce3-8136-d5776186ee21	dd04fbf3-2a51-4c72-8038-da7094b8da55		gs://broad-public-datasets/NA12878_downsampled_for_testing/unmapped/H06JUADXX130110.1.ATCACGAT.20k_reads.bam	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/60936d97-6358-4ce3-8136-d5776186ee21?version=2018-03-23T123738.145535Z")
        bag = BagHandler(data=data, bag_info={}, bag_name='manifest')
        zip_name = bag.create_bag()
        with zipfile.ZipFile(zip_name) as myzip:
            for name in myzip.namelist():
                if 'sample' in name:
                    sample = myzip.open(name)
                    row = sample.read()
                    sampleid = 'entity:sample_id'
                    self.assertEqual(sampleid, row[:len(sampleid)])

        os.remove(zip_name)

    def testWriteCsv2(self):
        with open('test/manifest_with_crai_cram_bai.tsv', 'r') as tsv:
            lines = tsv.readlines()
        data = "\n".join(lines)
        bag = BagHandler(data=data, bag_info={}, bag_name='manifest')
        zip_name = bag.create_bag()
        os.remove(zip_name)

if __name__ == '__main__':
    unittest.main()
