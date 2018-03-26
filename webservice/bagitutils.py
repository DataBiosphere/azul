#!/usr/bin/env python
import shutil
import zipfile
import os
from StringIO import StringIO

import bagit
import pandas as pd
import numpy as np
import re
import tempfile
import csv

BW_SAMPLE_UUID = "Sample UUID"
BW_DONOR_UUID = "Donor UUID"
BW_FILE_TYPE = "File Type"
BW_FILE_URLS = 'File URLs'
INCOMING_COLUMN_EXCEPTIONS = [BW_SAMPLE_UUID,
                              BW_FILE_TYPE,
                              BW_DONOR_UUID,
                              BW_FILE_URLS,
                              "File Path",
                              "Upload File ID",
                              'File DOS URL'
                              ]

FC_SAMPLE_ID = 'entity:sample_id'
FC_ENTITY_ID = 'entity:participant_id'


class BagHandler:
    """
    Handles data in BagIt data structure.
    """

    def __init__(self, data, bag_name, bag_info):
        # Create Pandas dataframe from tab-separated values.
        if isinstance(data, pd.core.frame.DataFrame):
            self.data = data
        elif type(data) is str:
            self.data = data
        else:
            self.data = pd.read_csv(data, sep='\t')
        self.name = bag_name
        self.info = bag_info

    def create_bag(self):
        """Create compressed file that contains a BagIt.
        :return zip_file_name: path to compressed BagIt
        """
        tempd = tempfile.mkdtemp('bag_tmpd')
        bag_dir = tempd + '/' + self.name
        # Add payload in subfolder "data" and write to disk.
        data_path = bag_dir + '/data'
        os.makedirs(data_path)
        bag = bagit.make_bag(bag_dir, self.info)
        if type(self.data) is str:
            (participants, samples) = self.transform_to_participant_and_sample(self.data)
            with open(data_path + '/participant.tsv', 'w') as tsv:
                fieldnames = [FC_ENTITY_ID]
                writer = csv.DictWriter(tsv, fieldnames=fieldnames, delimiter='\t')
                writer.writeheader()
                for p in participants:
                    writer.writerow({FC_ENTITY_ID: p})
            with open(data_path + '/sample.tsv', 'w') as tsv:
                first = True
                for sample in samples:
                    if first:
                        first = False
                        keys = sample.keys()
                        keys.remove(FC_SAMPLE_ID)
                        fieldnames = [FC_SAMPLE_ID] + sorted(keys)
                        writer = csv.DictWriter(tsv, fieldnames=fieldnames, delimiter='\t')
                        writer.writeheader()
                    writer.writerow(sample)
        else:
            self._reformat_headers()
            participant, sample = self.transform()

            participant.to_csv(path=data_path + '/participant.tsv',
                               sep='\t',
                               index=False,
                               header=True)
            sample.to_csv(path_or_buf=data_path + '/sample.tsv',
                          sep='\t',
                          index=False,
                          header=True)
        # Write BagIt to disk and create checksum manifests.
        bag.save(manifests=True)
        # Compress bag.
        zipfile_tmp = tempfile.NamedTemporaryFile(suffix='.zip', delete=False)
        zipfile_handle = zipfile.ZipFile(zipfile_tmp,
                                         'w', zipfile.ZIP_DEFLATED)
        self.__zipdir(tempd, zipfile_handle)
        zipfile_handle.close()
        shutil.rmtree(tempd, True)
        return zipfile_tmp.name

    def __zipdir(self, path, zip_fh):
        # zip_fh is zipfile handle
        pathLength = len(path)
        for root, dirs, files in os.walk(path):
            for file in files:
                zip_fh.write(os.path.join(root, file), arcname=root[pathLength:] + '/' + file)

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

    def transform_to_participant_and_sample(self, data):
        participants, file_types, native_protocols = self.participants_and_file_types_and_protocols(data)
        return list(participants), self.samples(data, file_types, native_protocols)

    def participants_and_file_types_and_protocols(self, data):
        reader = csv.DictReader(StringIO(data), delimiter='\t')
        participants = set()
        native_protocols = set()
        file_types = {} # Key: file type; value: index
        counter = 1
        for row in reader:
            # Add all participants. It's a set, so no dupes
            participants.add(row[BW_DONOR_UUID])

            # Track all unique file types, and associate a number with each
            file_type = row[BW_FILE_TYPE].strip()
            if file_type not in file_types:
                file_types[file_type] = counter
                counter = counter + 1

            # Track all the different cloud native url protocols
            for file_url in row[BW_FILE_URLS].split(','):
                protocol = self.native_url_protocol(file_url)
                if protocol is not None and protocol not in native_protocols:
                    native_protocols.add(protocol)

        return participants, file_types, native_protocols

    def samples(self, data, file_types, native_protocols):
        reader = csv.DictReader(StringIO(data), delimiter='\t')
        samples = []

        current_specimen_uuid = None
        current_row = None

        for row in reader:
            specimen_uuid = row[BW_SAMPLE_UUID]
            if specimen_uuid != current_specimen_uuid:
                current_specimen_uuid = specimen_uuid
                if current_row is not None:
                    samples.append(current_row)
                current_row = self.init_row(row, file_types, native_protocols)

            self.add_files_to_row(current_row, row, file_types)

        if current_row is not None:
            samples.append(current_row)
        return samples

    def add_files_to_row(self, new_row, existing_row, file_types):
        file_type = existing_row[BW_FILE_TYPE].strip()
        file_type_index = file_types[file_type]
        suffix = str(file_type_index)

        file_urls = existing_row[BW_FILE_URLS].split(',')
        for file_url in file_urls:
            protocol = self.native_url_protocol(file_url)
            if protocol is not None:
                new_row[self.native_column_name(protocol, suffix)] = file_url

        for column in ['File DOS URL', BW_FILE_TYPE, 'File Path', 'Upload File ID']:
            new_row[self.transform_boardwalk_column_to_fc_column(column) + suffix] = existing_row[column]

    def init_row(self, existing_row, file_types, native_protocols):
        # Rename sample column
        row = {FC_SAMPLE_ID: existing_row[BW_SAMPLE_UUID]}

        # Copy rows that don't need transformation, other than FC naming conventions
        for key in existing_row.keys():
            if key not in INCOMING_COLUMN_EXCEPTIONS:
              row[self.transform_boardwalk_column_to_fc_column(key)] = existing_row[key]

        # Rename "DONOR UUID" to 'participant'
        row['participant'] = existing_row[BW_DONOR_UUID]

        # Initialize native, native urls
        for file_type in file_types.values():
            suffix = str(file_type)
            for native_protocol in native_protocols:
                row[self.native_column_name(native_protocol, suffix)] = None
        return row

    @staticmethod
    def native_url_protocol(url):
        index = url.find('://')
        if index > 0:
            return url[:index]

    @staticmethod
    def native_column_name(native_protocol, suffix):
        return native_protocol + '_url' + suffix

    @staticmethod
    def transform_boardwalk_column_to_fc_column(column):
        return column.lower().replace(' ', '_').replace('.', '_')