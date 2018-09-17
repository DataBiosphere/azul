#!/usr/bin/env python
import shutil
import operator
import zipfile
import os
from StringIO import StringIO
import bagit
import tempfile
import csv


# Some column names from Boardwalk manifest. These are the ones that require
# special handling when converting into the FireCloud scheme.
class BoardwalkColumns:
    def __init__(self):
        pass

    # Special columns.
    SAMPLE_UUID = 'Sample UUID'
    DONOR_UUID = 'Donor UUID'
    FILE_TYPE = 'File Type'
    FILE_URLS = 'File URLs'
    FILE_DOS_URI = 'File DOS URI'
    FILE_PATH = 'File Path'
    UPLOAD_FILE_ID = 'Upload File ID'


# Column names in Boardwalk that are file related, except for FILE_URLS,
# which is extra special because its value is a comma-separated list.
FILE_COLUMNS = [
    BoardwalkColumns.FILE_DOS_URI,
    BoardwalkColumns.FILE_TYPE,
    BoardwalkColumns.FILE_PATH,
    BoardwalkColumns.UPLOAD_FILE_ID
]

# Column names in Boardwalk that cannot simply be copied over to FireCloud;
# they require extra logic.
COMPLEX_COLUMNS = FILE_COLUMNS + [
    BoardwalkColumns.SAMPLE_UUID,
    BoardwalkColumns.FILE_URLS
]


class RequiredFirecloudColumns:
    """
    Columns must be present in FireCloud TSVs. The TSVs can contain additional
    columns, but these minimal columns must be present.
    """
    def __init__(self):
        pass

    # The column in the participants.tsv
    PARTICIPANT_ENTITY_ID = 'entity:participant_id'

    # Columns in samples.tsv
    SAMPLE_SAMPLE_ID = 'entity:sample_id'
    SAMPLE_PARTICIPANT = 'participant'


class BagHandler:
    """
    From a Boardwalk manifest, generates a zip file with the contents in a bagit
    format, where the bagit contains two TSVs that can be uploaded to FireCloud.

    The Boardwalk manifest is a single TSV. Each row in the manifest corresponds
    to a file. Several files can be part of the same sample, meaning a sample
    can be spread across multiple rows.

    For FireCloud, the data needs to be broken up into two TSVs, a participant
    and a sample TSV.

    The participants TSV is a one column TSV with the unique participant UUIDs.

    The sample TSV has one row per sample, linked to the participants TSV by
    a participant column. Because a sample may contain multiple files, each
    file for a sample is added as an additional column.

    Simplified example Boardwalk TSV

    DONOR UUID   SAMPLE UUID    FILE
    d1           s1             f1
    d1           s1             f2
    d2           s2             f3

    This gets transformed to a participants TSV, with the two unique donors:

    entity:participant_id
    d1
    d2

    And a sample TSV, with the two samples, linked to participants.tsv by the
    participant column:

    entity:sample_id  participant  file1 file2
    s1                d1           f1    f2
    s2                d2           f3

    In FireCloud, the name of column "entity:participant_id" in
    participants.tsv, and the name of the columns "entity:sample_id" and
    "participant" in samples.tsv must be exactly those. Additional columns in
    sample can have any name, although the convention seems to be lower
    case with underscores, so we convert the Boardwalk column names to follow
    that convention.

    In this example, the file2 column is empty for the second row. That is
    because the different samples can have different numbers of files.
    """

    def __init__(self, data, bag_name, bag_info):
        self.data = data
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

        self.write_csv_files(data_path)

        # Write BagIt to disk and create checksum manifests.
        bag.save(manifests=True)

        # Compress bag.
        zipfile_tmp = tempfile.NamedTemporaryFile(suffix='.zip', delete=False)
        zipfile_handle = zipfile.ZipFile(zipfile_tmp,
                                         'w', zipfile.ZIP_DEFLATED)
        self._zipdir(tempd, zipfile_handle)
        zipfile_handle.close()
        shutil.rmtree(tempd, True)
        return zipfile_tmp.name

    @staticmethod
    def _zipdir(path, zip_fh):
        # zip_fh is zipfile handle
        path_length = len(path)
        for root, dirs, files in os.walk(path):
            for _file in files:
                zip_fh.write(os.path.join(root, _file),
                             arcname=root[path_length:] + '/' + _file)

    def write_csv_files(self, data_path):
        """
        Generates and writes participants.tsv and samples.tsv to data_path
        directory.
        :param data_path: Where to write the files
        :return: None
        """
        participants, samples = self.convert_to_participant_and_sample()

        with open(data_path + '/participants.tsv', 'w') as tsv:
            writer = csv.DictWriter(tsv, fieldnames=[
                RequiredFirecloudColumns.PARTICIPANT_ENTITY_ID], delimiter='\t')
            writer.writeheader()
            for p in participants:
                writer.writerow(
                    {RequiredFirecloudColumns.PARTICIPANT_ENTITY_ID: p})

        with open(data_path + '/samples.tsv', 'w') as tsv:
            first_row = True
            for sample in samples:
                if first_row:
                    first_row = False
                    keys = sample.keys()
                    # entity:sample_id must be first.
                    keys.remove(RequiredFirecloudColumns.SAMPLE_SAMPLE_ID)
                    fieldnames = (
                        [RequiredFirecloudColumns.SAMPLE_SAMPLE_ID] +
                        sorted(keys))
                    writer = csv.DictWriter(tsv, fieldnames=fieldnames,
                                            delimiter='\t')
                    writer.writeheader()
                writer.writerow(sample)

    def convert_to_participant_and_sample(self):
        participants, max_samples, native_protocols, file_types = \
            self.participants_and_max_files_in_sample_and_protocols()
        return list(participants), self.samples(native_protocols, file_types)

    def participants_and_max_files_in_sample_and_protocols(self):
        """
        Does one pass through the CSV, calculating the unique participants,
        the maximum number of files for any one sample, and the total number
        of cloud native protocols being used.
        :return: a tuple with a set of participants, the maximum number of
        files in any one sample, and a set of the unique cloud native protocols.
        """
        reader = csv.DictReader(StringIO(self.data), delimiter='\t')
        participants = set()
        native_protocols = set()
        file_types = set()
        samples = {}  # key: samples UUID, value count
        for row in reader:
            # Add all participants. It's a set, so no dupes
            participants.add(row[BoardwalkColumns.DONOR_UUID])

            sample_uuid = row[BoardwalkColumns.SAMPLE_UUID]
            if sample_uuid in samples:
                samples[sample_uuid] = samples[sample_uuid] + 1
            else:
                samples[sample_uuid] = 1

            # Track all the different cloud native url protocols
            for file_url in row[BoardwalkColumns.FILE_URLS].split(','):
                protocol = self.native_url_protocol(file_url)
                if protocol is not None:
                    native_protocols.add(protocol)

            # Track all file types in manifest.
            file_types.add(row[BoardwalkColumns.FILE_TYPE].lower())

        return participants, max(samples.values()), native_protocols, file_types

    def samples(self, native_protocols, file_types):
        """
        Creates a list of dicts, where one dict is a row in the sample TSV 
        for FireCloud. For all rows of the same sample in the input create 
        one row only, where the file-specific data from each row is 
        appended as additional columns to the one row (reasons
        for having multiple rows of the same sample are for instance 
        different file types or different cloud storage protocols for a given
        sample ID).

        The input is self.data. Requires that data be sorted by
        BoardwalkColumns.SAMPLE_UUID; this routine sorts it. If data could be
        sorted before being passed to this method, then we should remove
        sorting in here.

        :param native_protocols: all the unique native protocols in the data
        :param file_types: all unique file types in the data
        :return: a list of dicts
        """
        reader = csv.DictReader(StringIO(self.data), delimiter='\t')
        samples = []

        current_sample_uuid = None
        current_row = None
        index = None

        for row in sorted(reader, key=operator.itemgetter(
                BoardwalkColumns.SAMPLE_UUID)):  # sort by sample ID
            sample_uuid = row[BoardwalkColumns.SAMPLE_UUID]
            suffix = self.get_suffix(row[BoardwalkColumns.FILE_TYPE])
            if sample_uuid != current_sample_uuid:

                current_sample_uuid = sample_uuid
                index = 1
                if current_row is not None:
                    samples.append(current_row)
                current_row = self.init_sample_row(row, native_protocols,
                                                   file_types)
            else:
                index = index + 1

            self.add_files_to_row(current_row, row, suffix)

        if current_row is not None:
            samples.append(current_row)
        return samples

    def add_files_to_row(self, new_row, existing_row, suffix):
        """
        Takes the file-specific columns of existing_row, and adds them as
        new columns to new_row.
        :param new_row:
        :param existing_row:
        :param suffix:
        :return:
        """
        file_urls = existing_row[BoardwalkColumns.FILE_URLS].split(',')
        for file_url in file_urls:
            protocol = self.native_url_protocol(file_url)
            if protocol is not None:
                new_row[self.native_column_name(protocol, suffix)] = file_url

        for column in FILE_COLUMNS:
            if column in existing_row:
                new_row[self.firecloud_column_name(column) + suffix] = \
                    existing_row[column]

    def init_sample_row(self, existing_row, native_protocols, file_types):
        """
        Create and initialize a sample row
        :param existing_row: the existing row
        :param native_protocols:
        :param file_types: file types in manifest data
        :return: the initialized row
        """
        # Rename sample column and participant.
        row = {RequiredFirecloudColumns.SAMPLE_SAMPLE_ID: existing_row[
            BoardwalkColumns.SAMPLE_UUID],
               RequiredFirecloudColumns.SAMPLE_PARTICIPANT: existing_row[
                   BoardwalkColumns.DONOR_UUID]}

        # Copy rows that don't need transformation, other than FC naming
        # conventions.
        for key, value in existing_row.iteritems():
            if key not in COMPLEX_COLUMNS:
                row[self.firecloud_column_name(key)] = value

        # Initialize columns for files and cloud native urls with appropriate
        # integer denoting the file type (by Team Calcium convention).
        for file_type in file_types:
            suffix = self.get_suffix(file_type)
            for column in FILE_COLUMNS:
                row[self.firecloud_column_name(column) + suffix] = None

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
    def firecloud_column_name(column):
        return column.lower().replace(' ', '_').replace('.', '_')

    @staticmethod
    def get_suffix(s):
        """Lookup-table hack to address the problem of more than one file type 
        occurring in a given file type column (2018-07-16).
        :parameter s: (str) from a row from the manifest TSV file
        :returns suffix: (str) holds integer corresponding to a file type"""

        s = s.lower()

        if s.endswith('fastq.gz') or s.endswith('fastq'): # unaligned read
            return str(0)
        elif s.endswith('crai') or s.endswith('bai'):     # aligned, index
            return str(1)
        elif s.endswith('cram') or s.endswith('bam'):     # aligned read
            return str(2)
        else:
            raise ValueError("{}: unknown file type in manifest".
                             format(repr(s)))
