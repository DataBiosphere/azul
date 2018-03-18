#!/usr/bin/env python

import unittest
import pandas as pd
# import numpy as np
from ../bagitutils import BagHandler


class TestBagHandlerMethods(unittest.TestCase):

    def test_normalize(self):
        fpath = 'test/test_normalize_df.tsv'
        df = pd.read_csv(fpath)
        args = dict([('data', df),
                     ('bag_name', 'test_df'),
                     ('bag_info', 'test'),
                     ('bag_path', '~/dev/manifest-handover')])
        bag = BagHandler(**args)
        df_test = bag.__normalize(df)

        fpath = 'test/test_normalize_df_mock.tsv'
        df_normalized = pd.read_csv(fpath)

        self.assertEqual(df_test, df_normalized)


if __name__ == '__main__':
    unittest.main()
