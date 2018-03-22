#!/usr/bin/env python

import unittest
import pandas as pd
from StringIO import StringIO
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
        fpath = 'test/test_normalize_df_mock.tsv'
        df = pd.read_csv(fpath, sep='\t')
        # args = dict([('data', df),
        #              ('bag_name', 'test_df'),
        #              ('bag_info', 'test'),
        #              ('bag_path', '~/dev/manifest-handover')])
        # bag = BagHandler(**args)
        # df_test = bag._BagHandler__normalize(df)
        assert_frame_equal(self.normalized, df)

    def test_worksWithString(self):
        s = StringIO("Program	Project	Center Name	Submitter Donor ID	Donor UUID	Submitter Donor Primary Site	Submitter Specimen ID	Specimen UUID	Submitter Specimen Type	Submitter Experimental Design	Submitter Sample ID	Sample UUID	Analysis Type	Workflow Name	Workflow Version	File Type	File Path	Upload File ID	Data Bundle UUID	Metadata.json	File URLs	File DOS URL\n\
        NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study	Framingham	Broad	20428	09df7aef-246a-57eb-9685-e1d4d18b55ab	BLOOD	SRS1353998	2923638f-0784-5704-8d93-5b97b4ca3092	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	NWD692354	dd8337dd-f731-5c3b-9a03-bdae77ca47a9	alignment	topmed-spinnaker	Alpha Build 1	crai	NWD692354.b38.irc.v1.cram.crai	5a00cc38-2f8d-4d34-98e0-0a847579b988	50cfaf90-0998-5ef5-aa0b-cfaea71d5a7d		[u'gs://topmed-irc-share/genomes/NWD692354.b38.irc.v1.cram.crai', u's3://nih-nhlbi-datacommons/NWD692354.b38.irc.v1.cram.crai']	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/5a00cc38-2f8d-4d34-98e0-0a847579b988?version=2018-02-28T160411.061319Z\n\
        NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study	Framingham	Broad	20428	09df7aef-246a-57eb-9685-e1d4d18b55ab	BLOOD	SRS1353998	2923638f-0784-5704-8d93-5b97b4ca3092	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	NWD692354	dd8337dd-f731-5c3b-9a03-bdae77ca47a9	alignment	topmed-spinnaker	Alpha Build 1	cram	NWD692354.b38.irc.v1.cram	b4cf8998-34a1-4e00-aa23-bcdf8d6b23b5	50cfaf90-0998-5ef5-aa0b-cfaea71d5a7d		[u's3://nih-nhlbi-datacommons/NWD692354.b38.irc.v1.cram', u'gs://topmed-irc-share/genomes/NWD692354.b38.irc.v1.cram']	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/b4cf8998-34a1-4e00-aa23-bcdf8d6b23b5?version=2018-02-28T160408.957538Z\n")
        pd.read_csv(s, sep='\t')

    def test_bagHandler(self):
        s = StringIO("Program	Project	Center Name	Submitter Donor ID	Donor UUID	Submitter Donor Primary Site	Submitter Specimen ID	Specimen UUID	Submitter Specimen Type	Submitter Experimental Design	Submitter Sample ID	Sample UUID	Analysis Type	Workflow Name	Workflow Version	File Type	File Path	Upload File ID	Data Bundle UUID	Metadata.json	File URLs	File DOS URL\n\
        NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study	Framingham	Broad	20428	09df7aef-246a-57eb-9685-e1d4d18b55ab	BLOOD	SRS1353998	2923638f-0784-5704-8d93-5b97b4ca3092	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	NWD692354	dd8337dd-f731-5c3b-9a03-bdae77ca47a9	alignment	topmed-spinnaker	Alpha Build 1	crai	NWD692354.b38.irc.v1.cram.crai	5a00cc38-2f8d-4d34-98e0-0a847579b988	50cfaf90-0998-5ef5-aa0b-cfaea71d5a7d		[u'gs://topmed-irc-share/genomes/NWD692354.b38.irc.v1.cram.crai', u's3://nih-nhlbi-datacommons/NWD692354.b38.irc.v1.cram.crai']	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/5a00cc38-2f8d-4d34-98e0-0a847579b988?version=2018-02-28T160411.061319Z\n\
        NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study	Framingham	Broad	20428	09df7aef-246a-57eb-9685-e1d4d18b55ab	BLOOD	SRS1353998	2923638f-0784-5704-8d93-5b97b4ca3092	Normal - Blood	Seq_DNA_SNP_CNV; Seq_DNA_WholeGenome	NWD692354	dd8337dd-f731-5c3b-9a03-bdae77ca47a9	alignment	topmed-spinnaker	Alpha Build 1	cram	NWD692354.b38.irc.v1.cram	b4cf8998-34a1-4e00-aa23-bcdf8d6b23b5	50cfaf90-0998-5ef5-aa0b-cfaea71d5a7d		[u's3://nih-nhlbi-datacommons/NWD692354.b38.irc.v1.cram', u'gs://topmed-irc-share/genomes/NWD692354.b38.irc.v1.cram']	dos://dos-dss.ucsc-cgp-dev.org/ga4gh/dos/v1/dataobjects/b4cf8998-34a1-4e00-aa23-bcdf8d6b23b5?version=2018-02-28T160408.957538Z\n")
        bag = BagHandler(data=s, bag_info={}, bag_path='manifest')
        bag.create_bdbag()


if __name__ == '__main__':
    unittest.main()
