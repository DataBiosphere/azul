#!/usr/bin/env python

import shutil
import zipfile
import os
import bagit
import pandas as pd
import numpy as np
import re


class BagHandler:
    """
    Handles data in BagIt data structure.
    """
    def __init__(self, data, bag_name, bag_path, bag_info):
        # Create Pandas dataframe from tab-separated values.
        if isinstance(data, pd.core.frame.DataFrame):
            self.data = data
        else:
            self.data = pd.read_csv(data, sep='\t')
        self.name = bag_name
        self.path = bag_path
        self.info = bag_info

    def create_bdbag(self):
        """Create compressed file that contains a BagIt.
        :return zip_file_name: path to compressed BagIt
        """
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        bag = bagit.make_bag(self.path, self.info)
        bag._reformat_headers()
        participant, sample = self.transform()
        # Add payload in subfolder "data" and write to disk.
        with open(self.path + '/data/participant.tsv', 'w') as fp:
            fp.write(participant)
        with open(self.path + '/data/sample.tsv', 'w') as fp:
            fp.write(sample)
        # Write BagIt to disk and create checksum manifests.
        bag.save(manifests=True)
        # Compress bag.
        zip_file_path = os.path.basename(os.path.normpath(str(bag)))
        zip_file_name = 'manifest.zip'
        zipf = zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED)
        self.__zipdir(zip_file_path, zipf)
        zipf.close()
        shutil.rmtree(self.path, True)
        return zip_file_name

    def __zipdir(self, path, ziph):
        # ziph is zipfile handle
        for root, dirs, files in os.walk(path):
            for file in files:
                ziph.write(os.path.join(root, file))

    def _reformat_headers(self):
        """Removes whitespace and dots in column names, and sets
        all header strings to lower case."""
        df = self.data
        # Remove all spaces from column headers and make lower case.
        df.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
        df.rename(columns=lambda x: x.replace(".", "_"), inplace=True)
        df.rename(columns=lambda x: x.lower(), inplace=True)

    def transform(self):
        """Transforms dataframe df for FireCloud upload and returns
        two dataframes, a tuple of participant and sample, which are then
        uploaded to FireCloud in that order.
        """
        df = self.data
        # Start normalizing the table. First, slice by file type.
        df1 = df[df['file_type'] == 'crai']
        # Extract three columns from df with file type 'cram':
        df2 = df[['file_type',
                  'file_path',
                  'upload_file_id',
                  'file_urls',
                  'file_dos_url']][df['file_type'] == 'cram']
        df2.rename(index=str,
                   columns={'file_type': 'file_type2',
                            'file_path': 'file_path2',
                            'upload_file_id': 'upload_file_id2',
                            'file_urls': 'file_urls2',
                            'file_dos_url': 'file_dos_url2'},
                   inplace=True)
        frames = [df1, df2]  # merge both frames
        for frame in frames:
            frame.reset_index(drop=True, inplace=True)
        # Second, by combining df1 and df2 we obtain a normalized table,
        # using the index from df1.
        df_new = pd.concat(frames, axis=1, join_axes=[df1.index])
        df_new.drop_duplicates(keep='first', inplace=True)
        # Create a table with only one column (donor will be participant
        # in FC).
        participant = df_new['donor_uuid']  # extract one column
        participant.name = 'entity:participant_id'  # rename column header

        # Re-order index of dataframe to be compliant with FireCloud
        # specifications.
        new_index = ([11, 4, 3, 7, 5, 6, 8, 9, 10, 12, 13, 14] +
                     [0, 1, 2, 18, 19, 15, 16, 17, 20, 21, 22] +
                     [23, 24, 25, 26])
        L = df_new.columns.tolist()
        new_col_order = [L[x] for x in new_index]
        sample = df_new.reindex(columns=new_col_order)
        sample = sample.rename(
            index=str,
            columns={'sample_uuid': 'entity:sample_id',
                     'donor_uuid': 'participant_id',
                     'file_type': 'file_type1',
                     'file_path': 'file_path1',
                     'upload_file_id': 'upload_file_id1',
                     'file_urls': 'file_urls1',
                     'file_dos_url': 'file_dos_url1',
                     'metadata.json': 'metadata_json'})
        return participant, sample

    def __normalize(self):
        """
        Normalizes dataframe to First Normal Form (1NF) such that it
        contains only unique entries of donors IDs so it can be used
        as primary key. Part of that is creating new columns with new
        column names of those records that are duplicate.
        :returns df: (Pandas dataframe) normalized
        """
        df = self.data
        # Get list of all column names.
        col_names = [col for col in df]
        # Constrain that list to those column names that hold file info.
        L = [s for s in col_names if bool(re.search('[Ff]ile', s))]
        # file_type = "".join(str(s) for s in L)

        nrecords = len(df['donor_uuid'].unique())  # number of donors
        filetype = df['file_type'].unique()  # create list of filetypes
        for idx, item in enumerate(L):
            print(item,)
            a = np.repeat((filetype[0]), nrecords) 
        return df
