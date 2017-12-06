# utils.py

# note: Support CORS by adding app.route('/', cors=True)
# returns the name and file uuids sorted by data and json files
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
