#!/usr/bin/env python

import unittest
import pandas as pd
# import numpy as np
# from bagitutils import BagHandler
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


if __name__ == '__main__':
    unittest.main()
