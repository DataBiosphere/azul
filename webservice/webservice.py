import ast

import requests

import config
import datetime
from flask import jsonify, request, Blueprint
import logging.config
import os
import zipfile
import bagit
from responseobjects.elastic_request_builder import \
    ElasticTransformDump as EsTd
from responseobjects.utilities import json_pp

# Setting up logging
base_path = os.path.dirname(os.path.abspath(__file__))
logging.config.fileConfig('{}/config/logging.conf'.format(base_path))
bp_logger = logging.getLogger("dashboardService.webservice")
# Setting up the blueprint
webservicebp = Blueprint('webservicebp', 'webservicebp')
# TODO: Write the docstrings so they can support swagger.
# Please see https://github.com/rochacbruno/flasgger
# stackoverflow.com/questions/43911510/ \
# how-to-write-docstring-for-url-parameters


@webservicebp.route('/repository/files', methods=['GET'])
@webservicebp.route('/repository/files/', methods=['GET'])
@webservicebp.route('/repository/files/<file_id>', methods=['GET'])
def get_data(file_id=None):
    """
    Returns a dictionary with entries that can be used by the browser
    to display the data and facets
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when calling ElasticSearch
        - name: from
          in: query
          type: integer
          description: From where should we start returning the results
        - name: size
          in: integer
          type: string
          description: Size of the page being returned
        - name: order
          in: query
          type: string
          description: Whether it should be in ascending or descending order
        - name: sort
          in: query
          type: string
          description: Which field to sort by
    :return: Returns a dictionary with the entries to be used when generating
    the facets and/or table data
    """
    # Setup logging
    logger = logging.getLogger("dashboardService.webservice.get_data")
    # Get all the parameters from the URL
    logger.debug('Parameter file_id: {}'.format(file_id))
    filters = request.args.get('filters', '{"file": {}}')
    logger.debug("Filters string is: {}".format(filters))
    try:
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        logger.error("Malformed filters parameter: {}".format(e.message))
        return "Malformed filters parameter"
    # Make the default pagination
    logger.info("Creating pagination")
    pagination = {
        "from": request.args.get('from', 1, type=int),
        "order": request.args.get('order', 'desc'),
        "size": request.args.get('size', 5, type=int),
        "sort":    request.args.get('sort', 'center_name'),
    }
    logger.debug("Pagination: \n".format(json_pp(pagination)))
    # Handle <file_id> request form
    if file_id is not None:
        logger.info("Handling single file id search")
        filters['file']['fileId'] = {"is": [file_id]}
    # Create and instance of the ElasticTransformDump
    logger.info("Creating ElasticTransformDump object")
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN", "elasticsearch1"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    logger.info("Creating the API response")
    response = es_td.transform_request(filters=filters,
                                       pagination=pagination,
                                       post_filter=True)
    # Returning a single response if <file_id> request form is used
    if file_id is not None:
        response = response['hits'][0]
    return jsonify(response)


@webservicebp.route('/repository/files/piecharts', methods=['GET'])
def get_data_pie():
    """
    Returns a dictionary with entries that can be used by the
    browser to generate piecharts
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when calling ElasticSearch
        - name: from
          in: query
          type: integer
          description: From where should we start returning the results
        - name: size
          in: integer
          type: string
          description: Size of the page being returned
        - name: order
          in: query
          type: string
          description: Whether it should be in ascending or descending order
        - name: sort
          in: integer
          type: string
          description: Which field to sort by
    :return: Returns a dictionary with the entries to be used when generating
    a pie chart
    """
    # Setup logging
    logger = logging.getLogger("dashboardService.webservice.get_data_pie")
    # Get all the parameters from the URL
    filters = request.args.get('filters', '{"file": {}}')
    logger.debug("Filters string is: {}".format(filters))
    try:
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        logger.error("Malformed filters parameter: {}".format(e.message))
        return "Malformed filters parameter"
    # Make the default pagination
    logger.info("Creating pagination")
    pagination = {
        "from": request.args.get('from', 1, type=int),
        "order": request.args.get('order', 'desc'),
        "size": request.args.get('size', 5, type=int),
        "sort":    request.args.get('sort', 'center_name'),
    }
    logger.debug("Pagination: \n".format(json_pp(pagination)))
    # Create and instance of the ElasticTransformDump
    logger.info("Creating ElasticTransformDump object")
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN", "elasticsearch1"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    logger.info("Creating the API response")
    response = es_td.transform_request(filters=filters,
                                       pagination=pagination,
                                       post_filter=False)
    # Returning a single response if <file_id> request form is used
    return jsonify(response)


@webservicebp.route('/repository/files/summary', methods=['GET'])
def get_summary():
    """
    Returns a summary based on the filters passed on to the call. Based on the
    ICGC endpoint.
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when calling ElasticSearch
    :return: Returns a jsonified Summary API response
    """
    # Setup logging
    logger = logging.getLogger("dashboardService.webservice.get_summary")
    # Get the filters from the URL
    filters = request.args.get('filters', '{"file": {}}')
    logger.debug("Filters string is: {}".format(filters))
    try:
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        logger.error("Malformed filters parameter: {}".format(e.message))
        return "Malformed filters parameter"
    # Create and instance of the ElasticTransformDump
    logger.info("Creating ElasticTransformDump object")
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN", "elasticsearch1"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    logger.info("Creating the API response")
    response = es_td.transform_summary(filters=filters)
    # Returning a single response if <file_id> request form is used
    return jsonify(response)


@webservicebp.route('/keywords', methods=['GET'])
def get_search():
    """
    Creates and returns a dictionary with entries that best match the query
    passed in to the endpoint
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when calling ElasticSearch
        - name: q
          in: query
          type: string
          description: String query to use when calling ElasticSearch
        - name: type
          in: query
          type: string
          description: Which type of response format should be returned
        - name: field
          in: query
          type: string
          description: Which field to search on. Defaults to file id
        - name: from
          in: query
          type: integer
          description: From where should we start returning the results
        - name: size
          in: integer
          type: string
          description: Size of the page being returned
    :return: A dictionary with entries that best match the query passed in
    to the endpoint
    """
    # Setup logging
    logger = logging.getLogger("dashboardService.webservice.get_search")
    # Get all the parameters from the URL
    # Get the query to use for searching. Forcing it to be str for now
    _query = request.args.get('q', '', type=str)
    logger.debug("String query is: {}".format(_query))
    # Get the filters
    filters = request.args.get('filters', '{"file": {}}')
    try:
        # Set up the default filter if it is returned as an empty dictionary
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        logger.error("Malformed filters parameter: {}".format(e.message))
        return "Malformed filters parameter"
    # Generate the pagination dictionary out of the endpoint parameters
    logger.info("Creating pagination")
    pagination = {
        "from": request.args.get('from', 1, type=int),
        "size": request.args.get('size', 5, type=int)
    }
    logger.debug("Pagination: \n".format(json_pp(pagination)))
    # Get the entry format and search field
    _type = request.args.get('type', 'file')
    # Get the field to search
    field = request.args.get('field', 'fileId')
    # HACK: Adding this small check to make sure the search bar works with
    if _type in {'donor', 'file-donor'}:
        field = 'donor'
    # Create and instance of the ElasticTransformDump
    logger.info("Creating ElasticTransformDump object")
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN", "elasticsearch1"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    logger.info("Creating the API response")
    response = es_td.transform_autocomplete_request(pagination,
                                                    filters=filters,
                                                    _query=_query,
                                                    search_field=field,
                                                    entry_format=_type)
    return jsonify(response)


@webservicebp.route('/repository/files/order', methods=['GET'])
def get_order():
    """
    Get the order of the facets from the order_config file
    :return: A dictionary with a list containing the order of the facets
    """
    # Setup logging
    logger = logging.getLogger("dashboardService.webservice.get_order")
    # Open the order_config file and get the order list
    logger.info("Getting t")
    with open('{}/order_config'.format(
            os.path.dirname(config.__file__))) as order:
        order_list = [line.rstrip('\n') for line in order]
    return jsonify({'order': order_list})


@webservicebp.route('/repository/files/export', methods=['GET'])
def get_manifest():
    """
    Creates and returns a manifest based on the filters pased on
    to this endpoint
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when generating the manifest
    :return: A manifest that the user can use to download the files in there
    """
    # Setup logging
    logger = logging.getLogger("dashboardService.webservice.get_manifest")
    filters = request.args.get('filters', '{"file": {}}')
    logger.debug("Filters string is: {}".format(filters))
    try:
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        logger.error("Malformed filters parameter: {}".format(e.message))
        return "Malformed filters parameter"
    # Create and instance of the ElasticTransformDump
    logger.info("Creating ElasticTransformDump object")
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN", "elasticsearch1"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    logger.info("Creating the API response")
    response = es_td.transform_manifest(filters=filters)
    # Return the excel file
    return response

@webservicebp.route('/repository/files/export/firecloud', methods=['GET'])
def export_to_firecloud():
    """
    Creates a FireCloud workspace based on the filters, workspace, and namespace passed to
    to this endpoint. The authorization header should contain a bearer token that will be used against the
    FireCloud API.
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when generating the manifest
        - name: workspace
          in: query
          type: string
          description: The name of the FireCloud workspace to create
        - name: namespace
          in: query
          type: string
          description: The namespace of the FireCloud workspace to create
    :return: TBD, probably a JSON object with the url of the FireCloud workspace
    """
    # Setup logging
    logger = logging.getLogger("dashboardService.webservice.export_to_firecloud")
    filters = request.args.get('filters', '{"file": {}}')
    logger.debug("Filters string is: {}".format(filters))
    try:
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        logger.error("Malformed filters parameter: {}".format(e.message))
        return "Malformed filters parameter"
    workspace = request.args.get('workspace')
    if workspace is None:
        logger.error("Missing workspace parameter")
        return "Missing workspace parameter", 400
    namespace = request.args.get('namespace')
    if namespace is None:
        return "Missing namespace parameter", 400
    auth = request.headers.get('authorization')
    if auth is None:
        return "Unauthorized", 401
    response = {
        'auth': auth,
        'workspace': workspace,
        'namespace': namespace
    }
    # Create and instance of the ElasticTransformDump
    logger.info("Creating ElasticTransformDump object")
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN", "elasticsearch1"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    logger.info("Creating the API response")
    response = es_td.transform_manifest(filters=filters)
    # Create and return the BDbag folder.
    bag_name = 'manifest_bag'
    bag_path = os.getcwd() + '/' + bag_name
    bag_info = {'organization': 'UCSC Genomics Institute',
                'data_type': 'TOPMed',
                'date_created': datetime.datetime.now().isoformat()}
    args = dict(
            bag_path=bag_path,
            bag_info=bag_info,
            payload=response.get_data())
    logger.info("Creating a compressed BDbag containing manifest.")
    bag = create_bdbag(**args)  # bag is a compressed file

    fileobj = open(bag, 'rb')
    domain = "egyjdjlme2.execute-api.us-west-2.amazonaws.com/api/exportBag"
    fc_lambda_protocol = os.getenv("FC_LAMBDA_PROTOCOL", "https")
    fc_lambda_domain = os.getenv("FC_LAMBDA_DOMAIN", domain)
    fc_lambda_port = os.getenv("FC_LAMBDA_PORT", '443')
    url = (fc_lambda_protocol +
           '://' + fc_lambda_port +
           '/' + fc_lambda_domain +
           '?workspace=' + workspace +
           '&namespace=' + namespace)
    headers = {'Content-Type': 'application/octet-stream',
               'Accept': 'application/json',
               'Authorization': auth}
    return requests.post(url=url,
                         data=fileobj,
                         headers=headers)

def create_bdbag(bag_path, bag_info, payload):
    """Create compressed BDbag file."""
    if not os.path.exists(bag_path):
        os.makedirs(bag_path)
    bag = bagit.make_bag(bag_path, bag_info)
    # Add payload in subfolder "data".
    with open(bag_path + '/data/manifest.tsv', 'w') as fp:
        fp.write(payload)
    bag.save(manifests=True)  # creates checksum manifests
    # Compress bag.
    zip_file_path = os.path.basename(os.path.normpath(str(bag)))
    zip_file_name = 'manifest_bag.zip'
    zipf = zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED)
    zipdir(zip_file_path, zipf)
    zipf.close()
    return zip_file_name


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))
