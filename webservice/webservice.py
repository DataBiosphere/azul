import ast
import requests
import config
import datetime
from flask import jsonify, request, Blueprint
import logging.config
import os
import uuid
from responseobjects.elastic_request_builder import \
    ElasticTransformDump as EsTd
from responseobjects.utilities import json_pp
from bagitutils import BagHandler
from s3_file_handler import S3FileHandler

import functools

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


def authorize_with_dashboard(f):
    @functools.wraps(f)
    def check_auth(*args, **kwargs):
        headers = {}
        # look at header for what we need
        if 'Authorization' in request.headers:
            headers['Authorization'] = request.headers['Authorization']
        # add cookie to request for auth
        if 'Cookie' in request.headers:
            headers['Cookie'] = request.headers['Cookie']
        # set the X-Forwarded-For and User-agent to prevent session protection on dashboard from causing problems
        headers['X-Forwarded-For'] = request.headers.get('X-Forwarded-For', request.remote_addr)
        if 'User-Agent' in request.headers:
            headers['User-Agent'] = request.headers['User-Agent']
        # make call to dashboard
        res = requests.get(request.url_root + 'authorization', headers=headers)
        if res.status_code == 204:
            return f(*args, **kwargs)
        else:
            return '', res.status_code
    return check_auth


@webservicebp.route('/repository/files', methods=['GET'])
@webservicebp.route('/repository/files/', methods=['GET'])
@webservicebp.route('/repository/files/<file_id>', methods=['GET'])
@authorize_with_dashboard
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
@authorize_with_dashboard
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
@authorize_with_dashboard
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
@authorize_with_dashboard
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
@authorize_with_dashboard
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


@webservicebp.route('/repository/files/export',
                    methods=['GET'])
@authorize_with_dashboard
def get_manifest():
    """
    Creates and returns a manifest based on the filters and format passed on
    to this endpoint.

    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when generating the manifest
        - name: format
          in: query
          type: string
          values: 'tsv' (default), 'bdbag'
          description: Output format. If format is 'bdbag' the output is a
            presigned URL to the S3 temporary storage. Any other format string
            or none will output a TSV file.
    :return: A manifest that the user can use to download the files in there
    """
    # Setup logging
    logger = logging.getLogger("dashboardService.webservice.get_manifest")
    filters = request.args.get('filters', '{"file": {}}')
    logger.debug("Filters string is: {}".format(filters))

    # NOTE: "format" here is a HTTP query parameter, not a Python command.
    format = request.args.get('format', default='tsv')
    logger.info("Returning with format={}".format(format))

    logger.debug("Output format is: {}".format(format))
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
    if format == 'bdbag':  # returns URL to BDBag
        response_bag = bdbag_response(response)
        if 'status' in response_bag:
            http_status = response_bag['status']
        else:
            logger.debug(
                "Response object {} has no status".format(response_bag))
            http_status = 200
        return jsonify(response_bag), http_status
    else:
        return response

def bdbag_response(response_obj):
    """
    Create a response for BDBag upload to S3. Returns just the presigned URL of
    the S3 location if all goes well. Otherwise the HTTP status code, error
    code, and a message are returned in JSON format.

    :param response_obj: Contains the selected metadata as a TSV.
    :type response_obj: A Flask response object.
    :return response: Information depends on status of actions.
    :rtype response: JSON
    """
    logger = logging.getLogger('dashboardService.webservice.bdbag_response')
    # Create and return the BDbag folder.
    bag_name = 'manifest'
    bag_info = {'organization': '',
                'data_type': '',
                'date_created': datetime.datetime.now().isoformat()}

    # Instantiate bag object.
    bag = BagHandler(data=response_obj.get_data(),
                     bag_info=bag_info,
                     bag_name=bag_name)
    zipped_bag = bag.create_bag()  # path to compressed bag
    logger.info('Creating a compressed BDbag containing manifest.')

    # Import bucket environment variable, launch instance of S3-file handler,
    # and upload the BDBag file to S3.

    # Transfer parameters.
    azul_s3_aws_region = os.getenv('AZUL_S3_AWS_REGION', 'us-west-2')
    azul_presigned_url_expiration = \
        os.getenv('AZUL_PRESIGNED_URL_EXPIRATION', 3600)  # in seconds
    bucket_key = str(uuid.uuid4())
    azul_s3_bucket = os.getenv('AZUL_S3_BUCKET', 'azul-s3-bucket')
    access_key_id = os.getenv('AWS_ACCESS_KEY_ID', None)
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', None)

    s3 = S3FileHandler(azul_s3_aws_region, access_key_id, secret_key)
    r = s3.upload_object_to_bucket(azul_s3_bucket,
                                   zipped_bag,
                                   bucket_key)
    os.remove(zipped_bag)

    if r['status_code'] == 200:
        logger.info("Uploaded BDbag {} to S3 bucket {}.".format(bucket_key,
                                                                azul_s3_bucket))
        # Generate presigned URL to access that s3 location.
        result = s3.create_presigned_url(azul_s3_bucket, bucket_key,
                                         azul_presigned_url_expiration)
        if result['status_code'] == 200:
            # Happy path: bag is uploaded, and presigned URL generated.
            logger.info("Successfully created presigned URL for bucket {}.".
                        format(''.join([azul_s3_bucket, '/', bucket_key])))
            response = {'url': result['presigned_url']}
        else:
            logger.error("Failed to create presigned URL for bucket {}.".
                        format(''.join([azul_s3_bucket, '/', bucket_key])))
            response = {'status': result['status_code'],
                        'msg': 'BDBag uploaded to S3, '
                               'but could not generate presigned URL.'}
    else:
        logger.error("Upload to S3 bucket {} failed: {}".format(
            azul_s3_bucket, r['status_code']))
        response = {
            'msg': 'BDBag upload to S3 bucket {} failed.'
                .format(azul_s3_bucket),
            'status': r['status_code']
        }
    return response

