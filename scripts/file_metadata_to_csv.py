#!/usr/bin/env python
"""
The Flatten class will collapse a set of HCA metadata JSON documents into a single csv file"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "15/02/2019"

import sys
import re
import os
import functools
import csv
from argparse import ArgumentParser
import glob
import json

class Flatten:
    def __init__(self, order = None, ignore = None, format_filter = None):
        self.all_objects = []
        self.all_keys = []
        self.uuid4hex = re.compile('^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.I)

        self.default_order = order if order else [
             "path",
             "^file_name",
             "^file_format",
             "^sequence_file. *",
             "^analysis_file. *",
             "^donor_organism. *",
             "^specimen_from_organism. *",
             "^cell_suspension. *",
             "^. * protocol. *",
             "^project."
        ]

        self.default_ignore = ignore if ignore else [
            "describedBy",
            "schema_type",
            "provenance",
            "biomaterial_id",
            "process_id",
            "contributors",
            "publications",
            "protocol_id",
            "project_description",
            "file_format",
            "file_name"
        ]

        self.default_format_filter = format_filter if format_filter else ["fastq.gz"]


    def _get_file_uuids_from_objects(self, list_of_metadata_objects):
        file_uuids = {}
        for object in list_of_metadata_objects:
            if "schema_type" not in object:
                raise MissingSchemaTypeError("JSON objects must declare a schema type")

            if object["schema_type"] == "file" and self._deep_get(object, ["provenance", "document_id"]):
                file_uuid = self._deep_get(object, ["provenance", "document_id"])
                file_uuids[file_uuid] = object

        if not file_uuids:
            raise MissingFileTypeError("no fileuuids found in any of the metadata objects")

        return file_uuids

    def _deep_get(self, d, keys):
        if not keys or d is None:
            return d
        return self._deep_get(d.get(keys[0]), keys[1:])

    def _set_value(self, master, key, value):

        if key not in master:
            master[key] = str(value)
        else:
            existing_values = master[key].split("||")
            existing_values.append(str(value))
            uniq = sorted(list(set(existing_values)))
            master[key] = "||".join(uniq)

    def _flatten(self, master, obj, parent):
        for key, value in obj.items():
            if key in self.default_ignore :
                continue

            newkey = parent + "." + key
            if isinstance(value, dict):
                self._flatten(master, obj[key], newkey)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._flatten(master, item, newkey)
                    else:
                        self._set_value(master, newkey, item)
            else:
                self._set_value(master, newkey, value)

    def _get_schema_name_from_object(self, object):
        if "describedBy" in object:
            return object["describedBy"].rsplit('/', 1)[-1]
        raise MissingDescribedByError("found a metadata without a describedBy property")

    def _cmp_keys(self, a, b):
        '''
        Simple comparator that uses a set of ordered items with
        regular expression to roughly control the order.
        :param a:
        :param b:
        :return:
        '''
        best_a = sys.maxsize
        best_b = sys.maxsize

        for idx, val in enumerate(self.default_order):
            p = re.compile(val)

            match_a = p.match(a)
            match_b = p.match(b)

            if (match_a):
                if (idx < best_a):
                    best_a = idx

            if (match_b):
                if (idx < best_b):
                    best_b = idx

        if (best_a == best_b):
            if a >= b:
                return 1
            return -1
        return best_a - best_b

    def add_bundle_files_to_row(self, list_of_metadata_objects, dir_name=None):
        '''

        :param list_of_metadata_objects:
        :return:
        '''
        # get all the files
        file_uuids = self._get_file_uuids_from_objects(list_of_metadata_objects)

        for file, content in file_uuids.items():
            obj = {}

            obj["file_name"] = self._deep_get(content, ["file_core", "file_name"])
            obj["file_format"] = self._deep_get(content, ["file_core", "file_format"])

            if  not (obj["file_name"] or obj["file_format"]):
                raise MissingFileNameError("expecting file_core.file_name")

            if dir_name:
                obj["path"] = dir_name + os.sep + obj["file_name"]

            if self.default_format_filter and obj["file_format"] not in self.default_format_filter:
                continue

            schema_name = self._get_schema_name_from_object(content)
            self._flatten(obj, content, schema_name)

            for metadata in list_of_metadata_objects:

                # ignore files
                if metadata["schema_type"] == "file" or metadata["schema_type"] == "link_bundle":
                    continue

                schema_name = self._get_schema_name_from_object(content)
                self._flatten(obj, metadata, schema_name)

            self.all_keys.extend(obj.keys())
            self.all_keys = list(set(self.all_keys))
            self.all_objects.append(obj)


    def dump(self, filename='output.csv', delim=","):

        self.all_keys.sort(key=functools.cmp_to_key(self._cmp_keys))

        delim = delim

        with open(filename, 'w') as csvfile:
            csv_writer = csv.DictWriter(csvfile, self.all_keys, delimiter=delim)
            csv_writer.writeheader()
            for obj in self.all_objects:
                csv_writer.writerow(obj)




def convert_bundle_dirs():
    parser = ArgumentParser()
    parser.add_argument("-d", "--dir", dest="dirname",
                        help="path to bundle directory", metavar="FILE", default=".")
    parser.add_argument("-o", "--output", dest="filename",
                        help="path to output file", default='bundles.csv')
    parser.add_argument("-s", "--seperator", dest="seperator",
                        help="seperator/delimiter for csv", default=',')
    parser.add_argument("-f", "--filter", dest="filter",
                        help="only get metadata for files with this file extension", default='fastq.gz')

    args = parser.parse_args()

    bundle_dir = args.dirname

    flattener = Flatten(format_filter=[ args.filter ])

    uuid4hex = re.compile('^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.I)

    for bundle in os.listdir(bundle_dir):
        # ignore any directory that isn't named with a uuid
        if uuid4hex.match(bundle):
            print ("flattening " + bundle)
            metadata_files = []
            for file in glob.glob(bundle_dir + os.sep + bundle + os.sep + '*.json'):
                with open(file) as f:
                    data = json.load(f)
                    metadata_files.append(data)

            flattener.add_bundle_files_to_row(metadata_files, dir_name=bundle)

    flattener.dump(filename=args.filename, delim=args.seperator )


if __name__ == '__main__':
    convert_bundle_dirs()

class Error(Exception):
   """Base class for other exceptions"""
   pass

class MissingSchemaTypeError(Error):
   pass

class MissingDescribedByError(Error):
   pass

class MissingFileTypeError(Error):
   pass

class MissingFileNameError(Error):
   pass