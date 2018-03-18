#!/usr/bin/env python

import shutil
import zipfile
import os
import bagit
import pandas as pd
import numpy as np


class BagHandler:
    """
    
    """
    def __init__(self, data, bag_name, bag_path, bag_info):
        # self.data = response_data
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
        participant, sample = self._transform_df()
        # Add payload in subfolder "data" and write to disk.
        with open(self.path + '/data/participant.tsv', 'w') as fp:
            fp.write(participant)
        with open(self.path + '/data/sample.tsv', 'w') as fp:
            fp.write(sample)
        # Write BagIt to disk and create checksum manifests.
        bag.save(manifests=True)
        # Compress bag.
        zip_file_path = os.path.basename(os.path.normpath(str(bag)))
        zip_file_name = 'manifest_bag.zip'
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

    def _transform_df(self):
        """Transforms dataframe df for FireCloud upload and returns
        two dataframes, a tuple of participant and sample, which are then
        uploaded to FireCloud in that order.
        """
        df = self.data
        # Remove all spaces from column headers and make lower case.
        df.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
        df.rename(columns=lambda x: x.lower(), inplace=True)
        # Start normalizing the table. First, slice by file type.
        df1 = df[df['file_type'] == 'crai']
        df2 = df[['file_type',
                  'file_path',
                  'upload_file_id']][df['file_type'] == 'cram']
        df2.rename(index=str,
                   columns={'file_type': 'file_type2',
                            'file_path': 'file_path2',
                            'upload_file_id': 'upload_file_id2'},
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
                     [0, 1, 2, 18, 19, 15, 16, 17, 20, 21, 22])
        L = df_new.columns.tolist()
        new_col_order = [L[x] for x in new_index]
        df_new = df_new.reindex(columns=new_col_order)
        sample = df_new.rename(
            index=str,
            columns={'sample_uuid': 'entity:sample_id',
                     'donor_uuid': 'participant_id',
                     'file_type': 'file_type1',
                     'file_path': 'file_path1',
                     'upload_file_id': 'upload_file_id1',
                     'metadata.json': 'metadata_json'})
        return participant, sample

    def __normalize(df):
        """
        """
        nrecords = len(df['donor_uuid'].unique()) # number of donors
        filetype = df['file_type'].unique()  # create list of filetypes
        a = np.repeat((filetype[0]), nrecords)  
        for item in np.nditer(filetype):
            print(item,)
        return df

