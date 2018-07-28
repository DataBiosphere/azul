# -*- coding: utf-8 -*-
"""
Utility module to help with getting data from Blue box.

The utils module has a DataExtractor class which aims to get the
metadata and data files from the Blue Box.

This module serves as a layer to interact with the BlueBox,
get the metadata and data files, and present it in a format
that can be used by the Indexer subclasses.
"""

from concurrent.futures import ThreadPoolExecutor
import logging
import os

from hca.dss import SwaggerAPIException

from azul import config

log = logging.getLogger(__name__)


class MetadataDownloader(object):
    """DataExtractor class to help with BlueBox interaction.

    This class works as a helper class for obtaining files from the Blue Box
    via the hca python module.

    """

    def __init__(self, dss_host):
        """
        Create an instance of the DataExtractor.

        It takes the formatted url of the DSS
        (e.g. https://dss.data.humancellatlas.org/v1) to which
        to talk to.

        :param dss_host: The formatted url for the DSS
        """
        self.dss_client = config.dss_client(dss_endpoint=dss_host)

    def __attempt(self, times, func, errors, **kwargs):
        """
        Try a function multiple times.

        Private helper method to try multiple times a function.
        It will try to catch the all of the errors passed to
        the function.

        :param times: The number of times to try the function
        :param func: The function being passed to the method
        :param errors: A tuple of errors to except on
        """
        # The blue box may have trouble returning bundle, so retry 3 times
        retries = 0
        while retries < times:
            try:
                # Execute function with kwargs
                response = func(**kwargs)
                break
            except errors as er:
                # Current retry didn't work. Try again
                log.info("Error on try {}\n:{}".format(retries, er))
                retries += 1
                continue
        else:
            # We ran out of tries
            log.error("Maximum number of retries reached: %s" % retries)
            raise Exception("Unable to access resource")
        return response

    def __get_bundle(self, bundle_uuid, bundle_version, replica):
        """
        Get the metadata and data files.

        Private method for getting the bundle from the bundle_uuid.
        It will attempt to get the bundle contents three times.
        It parses the contents of the bundle into metadata and data
        files.

        :param bundle_uuid: The bundle to pull from the DSS
        :param bundle_version: The bundle version to pull from the DSS
        :param replica: The replica which we should be pulling from
        (e.g aws, gcp, etc)
        :return: Returns a tuple separating (metadata_files, data_files)
        """
        # The blue box may have trouble returning bundle, so __attempt 3 times
        log.info("Getting bundle %s.%s from DSS.", bundle_uuid, bundle_version)
        bundle = self.__attempt(3,
                                self.dss_client.get_bundle,
                                SwaggerAPIException,
                                uuid=bundle_uuid,
                                version=bundle_version,
                                replica=replica)
        # Separate files in bundle by metadata files and data files
        _files = bundle['bundle']['files']
        metadata_files = {f["name"]: f for f in _files if f["indexed"]}
        manifest = _files
        # Return as a tuple
        return metadata_files, manifest

    def __get_file(self, file_uuid, file_version, replica):
        """
        Get a file from the Blue Box.

        This function gets a file from the blue box based on
        the 'file_uuid'. It will attempt to get the file three
        times.

        :param file_uuid: Specifies which file to get
        :param file_version: Specifies which file to get
        :param replica: Specifies the replica to pull from
        :return: Contents of that file
        """
        # The blue box may have trouble returning file, so __attempt 3 times
        log.info("Getting file %s version %s from DSS.", file_uuid, file_version)
        _file = self.__attempt(3,
                               self.dss_client.get_file,
                               SwaggerAPIException,
                               uuid=file_uuid,
                               version=file_version,
                               replica=replica)
        return _file

    def extract_bundle(self, request, replica="aws"):
        """
        Get the files and actual metadata.

        This is the main method that will extract the contents of the bundle
        and separate it into a tuple of (metadata_files, data_files), where
        the metadata_files are actual contents of the metadata files and the
        data_files are the metadata describing the files.

        :param request: The contents of the DSS event notification
        :param replica: The replica to which pull the bundle from
        """
        bundle_uuid = request['match']['bundle_uuid']
        bundle_version = request['match']['bundle_version']
        metadata_files, data_files = self.__get_bundle(bundle_uuid, bundle_version, replica)
        num_workers = int(os.environ['AZUL_DSS_WORKERS'])
        with ThreadPoolExecutor(num_workers) as tpe:
            def get_metadata(item):
                file_name, manifest_entry = item
                return file_name, self.__get_file(manifest_entry['uuid'], manifest_entry['version'], replica)
            metadata_files = dict(tpe.map(get_metadata, metadata_files.items()))
        return metadata_files, data_files
