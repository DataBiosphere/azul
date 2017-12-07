from hca.dss import DSSClient, SwaggerAPIException
import logging

indexer_name = os.getenv('INDEXER_NAME', 'dss-indigo')
module_logger = logging.getLogger(indexer_name + ".indexer")


class DataExtractor(object):
    """
    This class works as a helper class for obtaining files from the Blue Box
    via the hca python module.
    """
    def __init__(self, dss_host, **kwargs):
        """
        Creates an instance of the DataExtractor. It takes the formatted url
        of the DSS (e.g. https://dss.staging.data.humancellatlas.org/v1)
        :param dss_host: The formatted url for the DSS
        """
        self.dss_client = DSSClient()
        self.dss_client.host = dss_host
        self.log = logging.getLogger(indexer_name + ".indexer.DataExtractor")

    def __attempt(self, times, func, errors, **kwargs):
        """
        Private helper method to try multiple times a function
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
                self.log.info("Error on try {}\n:{}".format(retries, er))
                retries += 1
                continue
        else:
            # We ran out of tries
            self.log.error("Maximum number of retries reached: %s" % retries)
            raise Exception("Unable to access resource")
        return response

    def __get_bundle(self, bundle_uuid, replica, **kwargs):
        """
        Private method for getting the bundle from the bundle_uuid
        :param bundle_uuid: The bundle to pull from the DSS
        :param replica: The replica which we should be pulling from
        (e.g aws, gcp, etc)
        :return: Returns a tuple separating (metadata_files, data_files)
        """
        # The blue box may have trouble returning bundle, so __attempt 3 times
        bundle = __attempt(3,
                           self.dss_client.get_bundle,
                           SwaggerAPIException,
                           uuid=bundle_uuid,
                           replica=replica)
        # Separate files in bundle by metadata files and data files
        _files = bundle['bundle']['files']
        metadata_files = [{f["name"]: f} for f in _files if f["indexed"]]
        data_files = [{f["name"]: f} for f in _files if not f["indexed"]]
        # Return as a tuple
        return metadata_files, data_files

    def __get_file(self, file_uuid, replica, **kwargs):
        """
        This function gets a file from the blue box
        :param file_uuid: Specifies which file to get
        :param replica: Specifies the replica to pull from
        :return: Contents of that file
        """
        # The blue box may have trouble returning file, so __attempt 3 times
        _file = __attempt(3,
                          self.dss_client.get_file,
                          SwaggerAPIException,
                          uuid=file_uuid,
                          replica=replica)
        return _file

    def extract_bundle():
        """
        This is the main method that will extract the contents of the bundle
        and separate it into a tuple of (metadata_files, data_files), where
        the metadata_files are actual contents of the metadata files and the
        data_files are the metadata describing the files.
        """
        # Call __get_bundle
        # Then call some other method to thread the calling of metadata_files
        pass

def get_bundles(bundle_uuid, bbhost, replica):
    """
    This function gets a bundle from the blue box
    and sorts returned items by json and data (not json) files
    :param bundle_uuid: tell blue box which bundle to get
    :return: json file with items separated by json files and data files
    """

    # the blue box may have trouble returning bundle, so retry 3 times
    retries = 0
    while retries < 3:
        try:
            # call the blue box
            json_str = urlopen(str(bb_host + (
                'v1/bundles/') + bundle_uuid + "?replica=" + replica)).read()

            # json load string for processing later
            bundle = json.loads(json_str)
            break
        except HTTPError as er:
            app.log.info("Error on try {}\n:{}".format(retries, er))
            retries += 1
            continue
        except URLError as er:
            app.log.info("Error on try {}\n:{}".format(retries, er))
            retries += 1
            continue
    else:
        print("Maximum number of retries reached: {}".format(retries))
        raise Exception("Unable to access bundle '%s'" % bundle_uuid)

    json_files = []
    data_files = []
    # separate files in bundle by json files and data files
    for file in bundle['bundle']['files']:
        if file["name"].endswith(".json"):
            jsonfile = get_file(file["uuid"])
            json_files.append({file["name"]: jsonfile})
        else:
            data_files.append({file["name"]: file})
    return json.dumps({'json_files': json_files, 'data_files': data_files})


# returns the file
def get_file(file_uuid, bbhost, replica):
    """
    This function gets a file from the blue box
    :param file_uuid: tell blue box which file to get
    :return: file
    """
    # '?replica=aws' needed to choose cloud location
    aws_url = bb_host + "v1/files/" + file_uuid + "?replica=" + replica
    # only accept json files
    header = {'accept': 'application/json'}
    try:
        aws_response = requests.get(aws_url, headers=header)
        # not flattened
        file = json.loads(aws_response.content)
    except Exception as e:
        print(e)
        raise NotFoundError("File '%s' does not exist" % file_uuid)
    # queue.put(json.dumps(file))
    return json.dumps(file)

def post_notication(request)
    #request = app.current_request.json_body
    bundle_uuid = request['match']['bundle_uuid']
    bundle = get_bundles(bundle_uuid)
    return bundle
