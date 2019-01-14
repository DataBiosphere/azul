import ast
import base64
import binascii
import json
from concurrent.futures import ThreadPoolExecutor
import hashlib
import logging.config
import os
import re
import time
import urllib.parse
import uuid

# noinspection PyPackageRequirements
import boto3
from botocore.exceptions import ClientError
# noinspection PyPackageRequirements
from chalice import BadRequestError, Chalice, ChaliceViewError, NotFoundError, Response, UnauthorizedError
from more_itertools import one
import requests

from azul import config
from azul.health import Health
from azul.service import service_config
from azul.service.responseobjects.cart_item_manager import CartItemManager, DuplicateItemError, ResourceAccessError
from azul.service.responseobjects.elastic_request_builder import (BadArgumentException,
                                                                  ElasticTransformDump as EsTd,
                                                                  IndexNotFoundError)
from azul.service.responseobjects.manifest_service import ManifestService
from azul.service.responseobjects.step_function_helper import StateMachineError
from azul.service.responseobjects.storage_service import StorageService
from azul.service.responseobjects.utilities import json_pp

ENTRIES_PER_PAGE = 10

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)
for top_level_pkg in (__name__, 'azul'):
    logging.getLogger(top_level_pkg).setLevel(logging.INFO)

app = Chalice(app_name=config.service_name, configure_logs=False)
# FIXME: this should be configurable via environment variable (https://github.com/DataBiosphere/azul/issues/419)
app.debug = True
# FIXME: please use module logger instead (https://github.com/DataBiosphere/azul/issues/419)
app.log.setLevel(logging.DEBUG)


# TODO: Write the docstrings so they can support swagger.
# Please see https://github.com/rochacbruno/flasgger
# stackoverflow.com/questions/43911510/ \
# how-to-write-docstring-for-url-parameters


def _get_pagination(current_request):
    pagination = {
        "order": current_request.query_params.get('order', 'desc'),
        "size": int(current_request.query_params.get('size', ENTRIES_PER_PAGE)),
        "sort": current_request.query_params.get('sort', 'specimenId'),
    }

    sa = current_request.query_params.get('search_after')
    sb = current_request.query_params.get('search_before')
    sa_uid = current_request.query_params.get('search_after_uid')
    sb_uid = current_request.query_params.get('search_before_uid')

    if not sb and sa:
        pagination['search_after'] = [sa, sa_uid]
    elif not sa and sb:
        pagination['search_before'] = [sb, sb_uid]
    elif sa and sb:
        raise BadArgumentException("Bad arguments, only one of search_after or search_before can be set")

    return pagination


@app.route('/', cors=True)
def hello():
    return {'Hello': 'World!'}


@app.route('/health/basic', methods=['GET'], cors=True)
def basic_health():
    return {
        'up': True,
    }


@app.route('/health', methods=['GET'], cors=True)
def health():
    health = Health('service')
    return Response(
        body=json.dumps(health.as_json),
        status_code=200 if health.up else 503
    )


@app.route('/version', methods=['GET'], cors=True)
def version():
    from azul.changelog import compact_changes
    return {
        'git': config.git_status,
        'changes': compact_changes(limit=10)
    }


@app.route('/repository/files', methods=['GET'], cors=True)
@app.route('/repository/files/{file_id}', methods=['GET'], cors=True)
def get_data(file_id=None):
    """
    Returns a dictionary with entries that can be used by the browser
    to display the data and facets
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when calling ElasticSearch
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
        - name: search_after
          in: query
          type: string
          description: The value of the 'sort' field for the hit after which all results should be returned.  Not valid
          to set both this and search_before.
        - name: search_after_uid
          in: query
          type: string
          description: The value of the elasticsearch UID corresponding to the hit above, if search_after is set.
        - name: search_before
          in: query
          type: string
          description: The value of the 'sort' field for the hit before which all results should be returned.  Not valid
          to set both this and search_after.
        - name: search_before_uid
          in: query
          type: string
          description: The value of the elasticsearch UID corresponding to the hit above, if search_before is set.
    :return: Returns a dictionary with the entries to be used when generating
    the facets and/or table data
    """
    # Setup logging
    logger = app.log
    # Get all the parameters from the URL
    logger.debug('Parameter file_id: {}'.format(file_id))
    if app.current_request.query_params is None:
        app.current_request.query_params = {}
    filters = app.current_request.query_params.get('filters', '{"file": {}}')
    logger.debug("Filters string is: {}".format(filters))
    try:
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        # Make the default pagination
        logger.info("Creating pagination")
        pagination = _get_pagination(app.current_request)
        logger.debug("Pagination: \n".format(json_pp(pagination)))
        # Handle <file_id> request form
        if file_id is not None:
            logger.info("Handling single file id search")
            filters['file']['fileId'] = {"is": [file_id]}
        # Create and instance of the ElasticTransformDump
        logger.info("Creating ElasticTransformDump object")
        es_td = EsTd()
        # Get the response back
        logger.info("Creating the API response")
        response = es_td.transform_request(filters=filters,
                                           pagination=pagination,
                                           post_filter=True,
                                           entity_type='files')

        for hit in response['hits']:
            for file in hit['files']:
                file['url'] = file_url(file['uuid'], version=file['version'], replica='aws')

    except BadArgumentException as bae:
        raise BadRequestError(msg=bae.message)
    except IndexNotFoundError as infe:
        raise NotFoundError(msg=infe.message)
    else:
        if file_id is not None:
            response = response['hits'][0]
        return response


@app.route('/repository/specimens', methods=['GET'], cors=True)
@app.route('/repository/specimens/{specimen_id}', methods=['GET'], cors=True)
def get_specimen_data(specimen_id=None):
    """
    Returns a dictionary with entries that can be used by the browser
    to display the data and facets
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when calling ElasticSearch
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
        - name: search_after
          in: query
          type: string
          description: The value of the 'sort' field for the hit after which all results should be returned.  Not valid
          to set both this and search_before.
        - name: search_after_uid
          in: query
          type: string
          description: The value of the elasticsearch UID corresponding to the hit above, if search_after is set.
        - name: search_before
          in: query
          type: string
          description: The value of the 'sort' field for the hit before which all results should be returned.  Not valid
          to set both this and search_after.
        - name: search_before_uid
          in: query
          type: string
          description: The value of the elasticsearch UID corresponding to the hit above, if search_before is set.
    :return: Returns a dictionary with the entries to be used when generating
    the facets and/or table data
    """
    # Setup logging
    logger = app.log
    # Get all the parameters from the URL
    logger.debug('Parameter specimen_id: {}'.format(specimen_id))
    if app.current_request.query_params is None:
        app.current_request.query_params = {}
    filters = app.current_request.query_params.get('filters', '{"file": {}}')
    logger.debug("Filters string is: {}".format(filters))
    logger.info("Extracting the filter parameter from the request")
    filters = ast.literal_eval(filters)
    # Make the default pagination
    logger.info("Creating pagination")
    pagination = _get_pagination(app.current_request)
    logger.debug("Pagination: \n".format(json_pp(pagination)))
    # Handle <file_id> request form
    if specimen_id is not None:
        logger.info("Handling single file id search")
        filters['file']['fileId'] = {"is": [specimen_id]}
    # Create and instance of the ElasticTransformDump
    logger.info("Creating ElasticTransformDump object")
    es_td = EsTd()
    # Get the response back
    logger.info("Creating the API response")

    try:
        response = es_td.transform_request(filters=filters,
                                           pagination=pagination,
                                           post_filter=True,
                                           entity_type='specimens')
    except BadArgumentException as bae:
        raise BadRequestError(msg=bae.message)
    except IndexNotFoundError as infe:
        raise NotFoundError(msg=infe.message)
    else:
        # Returning a single response if <specimen_id> request form is used
        if specimen_id is not None:
            response = response['hits'][0]
        return response


@app.route('/repository/projects', methods=['GET'], cors=True)
@app.route('/repository/projects/{project_id}', methods=['GET'], cors=True)
def get_project_data(project_id=None):
    """
    Returns a dictionary with entries that can be used by the browser
    to display the data and facets
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when calling ElasticSearch
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
        - name: search_after
          in: query
          type: string
          description: The value of the 'sort' field for the hit after which all results should be returned.  Not valid
          to set both this and search_before.
        - name: search_after_uid
          in: query
          type: string
          description: The value of the elasticsearch UID corresponding to the hit above, if search_after is set.
        - name: search_before
          in: query
          type: string
          description: The value of the 'sort' field for the hit before which all results should be returned.  Not valid
          to set both this and search_after.
        - name: search_before_uid
          in: query
          type: string
          description: The value of the elasticsearch UID corresponding to the hit above, if search_before is set.
    :return: Returns a dictionary with the entries to be used when generating
    the facets and/or table data
    """
    # Setup logging
    logger = app.log
    # Get all the parameters from the URL
    logger.debug('Parameter specimen_id: {}'.format(project_id))
    if app.current_request.query_params is None:
        app.current_request.query_params = {}
    filters = app.current_request.query_params.get('filters', '{"file": {}}')
    logger.debug("Filters string is: {}".format(filters))
    try:
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        # Make the default pagination
        logger.info("Creating pagination")
        pagination = _get_pagination(app.current_request)
        logger.debug("Pagination: \n".format(json_pp(pagination)))
        # Handle <file_id> request form
        if project_id is not None:
            logger.info("Handling single file id search")
            filters['file']['projectId'] = {"is": [project_id]}
        # Create and instance of the ElasticTransformDump
        logger.info("Creating ElasticTransformDump object")
        es_td = EsTd()
        # Get the response back
        logger.info("Creating the API response")
        response = es_td.transform_request(filters=filters,
                                           pagination=pagination,
                                           post_filter=True,
                                           entity_type='projects')
    except BadArgumentException as bae:
        raise BadRequestError(msg=bae.message)
    else:
        # Return a single response if <project_id> request form is used
        if project_id is not None:
            return response['hits'][0]

        # Filter out certain fields if getting list of projects
        for hit in response['hits']:
            for project in hit['projects']:
                project.pop('contributors')
                project.pop('projectDescription')
                project.pop('publications')
        return response


@app.route('/repository/summary', methods=['GET'], cors=True)
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
    logger = logging.getLogger("dashboardService.webservice.get_summary")
    if app.current_request.query_params is None:
        app.current_request.query_params = {}
    # Get the filters from the URL
    filters = app.current_request.query_params.get('filters', '{"file": {}}')
    logger.debug("Filters string is: {}".format(filters))
    try:
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception as e:
        logger.error("Malformed filters parameter: {}".format(e))
        return "Malformed filters parameter"
    # Create and instance of the ElasticTransformDump
    logger.info("Creating ElasticTransformDump object")
    es_td = EsTd()
    # Get the response back
    logger.info("Creating the API response")

    # Request a summary for each entity type and cherry-pick summary fields from the summaries for the entity
    # that is authoritative for those fields.
    #
    summary_fields_by_authority = {
        'files': ['totalFileSize', 'fileTypeSummaries', 'fileCount'],
        'specimens': ['organCount', 'donorCount', 'labCount', 'totalCellCount', 'organSummaries', 'specimenCount'],
        'projects': ['projectCount']
    }
    with ThreadPoolExecutor(max_workers=len(summary_fields_by_authority)) as executor:
        summaries = dict(executor.map(lambda entity_type:
                                      (entity_type, es_td.transform_summary(filters=filters, entity_type=entity_type)),
                                      summary_fields_by_authority))
    unified_summary = {field: summaries[entity_type][field]
                       for entity_type, summary_fields in summary_fields_by_authority.items()
                       for field in summary_fields}
    assert all(len(unified_summary) == len(summary) for summary in summaries.values())

    # Returning a single response if <file_id> request form is used
    return unified_summary


@app.route('/keywords', methods=['GET'], cors=True)
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
        - name: size
          in: integer
          type: string
          description: Size of the page being returned
        - name: search_after
          in: query
          type: string
          description: The value of the 'sort' field for the hit after which all results should be returned.  Not valid
          to set both this and search_before.
        - name: search_after_uid
          in: query
          type: string
          description: The value of the elasticsearch UID corresponding to the hit above, if search_after is set.
        - name: search_before
          in: query
          type: string
          description: The value of the 'sort' field for the hit before which all results should be returned.  Not valid
          to set both this and search_after.
        - name: search_before_uid
          in: query
          type: string
          description: The value of the elasticsearch UID corresponding to the hit above, if search_before is set.
    :return: A dictionary with entries that best match the query passed in
    to the endpoint
    """
    # Setup logging
    logger = logging.getLogger("dashboardService.webservice.get_search")
    if app.current_request.query_params is None:
        app.current_request.query_params = {}
    # Get all the parameters from the URL
    # Get the query to use for searching. Forcing it to be str for now
    _query = app.current_request.query_params.get('q', '')
    logger.debug("String query is: {}".format(_query))
    # Get the filters
    filters = app.current_request.query_params.get('filters', '{"file": {}}')
    try:
        # Set up the default filter if it is returned as an empty dictionary
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception as e:
        logger.error("Malformed filters parameter: {}".format(e))
        return "Malformed filters parameter"
    # Generate the pagination dictionary out of the endpoint parameters
    logger.info("Creating pagination")
    pagination = _get_pagination(app.current_request)
    logger.debug("Pagination: \n".format(json_pp(pagination)))
    # Get the entry format and search field
    _type = app.current_request.query_params.get('type', 'file')
    # Get the field to search
    field = app.current_request.query_params.get('field', 'fileId')
    # HACK: Adding this small check to make sure the search bar works with
    if _type in {'donor', 'file-donor'}:
        field = 'donor'
    # Create and instance of the ElasticTransformDump
    logger.info("Creating ElasticTransformDump object")
    es_td = EsTd()
    # Get the response back
    logger.info("Creating the API response")
    response = es_td.transform_autocomplete_request(pagination,
                                                    filters=filters,
                                                    _query=_query,
                                                    search_field=field,
                                                    entry_format=_type)
    return response


@app.route('/repository/files/order', methods=['GET'], cors=True)
def get_order():
    """
    Get the order of the facets from the order_config file
    :return: A dictionary with a list containing the order of the facets
    """
    # Setup logging
    logger = logging.getLogger("dashboardService.webservice.get_order")
    # Open the order_config file and get the order list
    logger.info("Getting t")
    with open('{}/order_config'.format(os.path.dirname(service_config.__file__))) as order:
        order_list = [line.rstrip('\n') for line in order]
    return {'order': order_list}


@app.route('/repository/files/export', methods=['GET'], cors=True)
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
    if app.current_request.query_params is None:
        app.current_request.query_params = {}
    filters = app.current_request.query_params.get('filters', '{"file": {}}')
    format = app.current_request.query_params.get('format', 'tsv')
    logger.debug("Filters string is: {}".format(filters))
    try:
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception as e:
        logger.error("Malformed filters parameter: {}".format(e))
        return "Malformed filters parameter"
    # Create and instance of the ElasticTransformDump
    logger.info("Creating ElasticTransformDump object")
    es_td = EsTd()
    # Get the response back
    logger.info("Creating the API response")
    try:
        response = es_td.transform_manifest(filters=filters, format=format)
    except ValueError as e:
            return 400, str(e)

    # Return the excel file
    return response


@app.route('/manifest/files', methods=['GET'], cors=True)
def start_manifest_generation():
    """
    Initiate and check status of a manifest generation job, returning a either a 301 or 302 response
    redirecting to either the location of the manifest or a URL to re-check the status of the manifest job.

    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when generating the manifest
        - name: token
          in: query
          type: string
          description: An opaque string describing the manifest generation job

    :return: If the manifest generation has been started or is still ongoing, the response will have a
    301 status and will redirect to a URL that will get a recheck the status of the manifest.

    If the manifest generation is done and the manifest is ready to be downloaded, the response will
    have a 302 status and will redirect to the URL of the manifest.
    """
    status_code, retry_after, location = handle_manifest_generation_request()
    return Response(body='',
                    headers={
                        'Retry-After': str(retry_after),
                        'Location': location
                    },
                    status_code=status_code)


@app.route('/fetch/manifest/files', methods=['GET'], cors=True)
def start_manifest_generation_fetch():
    """
    Initiate and check status of a manifest generation job, returning a 200 response with
    simulated headers in the body.

    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when generating the manifest
        - name: token
          in: query
          type: string
          description: An opaque string describing the manifest generation job

    :return:  A 200 response with a JSON body describing the status of the manifest.

    If the manifest generation has been started or is still ongoing, the response will look like:

    ```
    {
        "Status": 301,
        "Retry-After": 2,
        "Location": "https://…"
    }
    ```

    The `Status` field emulates HTTP status code 301 Moved Permanently.

    `Retry-After` is the recommended number of seconds to wait before requesting the URL the `Location` field.

    `Location` is the URL to make a GET request to in order to recheck the status.

    If the client receives a response body with the `Status` field set to 301, the client should wait the number of
    seconds specified in `Retry-After` and then request the URL given in the `Location` field. The URL will point
    back at this endpoint so the client should expect a response of the same shape. Note that the actual HTTP
    response is of status 200, only the `Status` field of the body will be 301. The intent is to emulate HTTP while
    bypassing the default client behavior which, in most web browsers, is to ignore `Retry-After`. The response
    described here is intended to be processed by client-side Javascript such that the recommended delay in
    `Retry-After` can be handled in Javascript rather that relying on the native implementation by the web browser.

    If the manifest generation is done and the manifest is ready to be downloaded, the response will be:

    ```
    {
        "Status": 302,
        "Location": "https://manifest.url"
    }
    ```

    The client should request the URL given in the `Location` field. The URL will point to a different service and
    the client should expect a response containing the actual manifest. Currently the `Location` field of the final
    response is a signed URL to an object in S3 but clients should not depend on that.
    """
    status_code, retry_after, location = handle_manifest_generation_request()
    response = {
        'Status': status_code,
        'Location': location
    }
    if status_code == 301:  # Only return Retry-After if manifest is not ready
        response['Retry-After'] = retry_after
    return response


def handle_manifest_generation_request():
    """
    Start a manifest generation job and return a status code, Retry-After, and a retry URL for
    the view function to handle
    """
    logger = logging.getLogger("dashboardService.webservice.get_manifest")

    query_params = app.current_request.query_params or {}

    filters = query_params.get('filters', '{"file": {}}')
    logger.debug('Filters string is: {}'.format(filters))
    try:
        logger.info('Extracting the filter parameter from the request')
        filters = ast.literal_eval(filters)
        filters = {'file': {}} if filters == {} else filters
    except Exception as e:
        logger.error('Malformed filters parameter: {}'.format(e))
        raise BadRequestError('Malformed filters parameter')

    token = query_params.get('token')

    manifest_service = ManifestService()
    if token is None:
        execution_id = str(uuid.uuid4())
        manifest_service.start_manifest_generation(filters, execution_id)
        token = manifest_service.encode_params({'execution_id': execution_id})

    retry_url = self_url()

    try:
        return manifest_service.get_manifest_status(token, retry_url)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ExecutionDoesNotExist':
            raise BadRequestError('Invalid token given')
        raise
    except StateMachineError as e:
        raise ChaliceViewError(e.msg)
    except ValueError as e:
        raise BadRequestError(e.args)


@app.lambda_function(name=config.manifest_lambda_basename)
def generate_manifest(event, context):
    """
    Create a manifest based on the given filters and store it in S3

    :param: event: dict containing function input
        Valid params:
            - filters: dict containing filters to use in ES request
            - format: str to specify manifest output format, values are
                      'tsv' (default) or 'bdbag'
    :return: The URL to the generated manifest
    """
    filters = event.get('filters', {'file': {}})
    format = event.get('format')
    response = EsTd().transform_manifest(filters=filters, format=format)
    return {'Location': response.headers['Location']}


proxy_endpoint_path = "/fetch/dss/files"


@app.route(proxy_endpoint_path + '/{uuid}', methods=['GET'], cors=True)
def files_proxy(uuid):
    """
    Initiate checking out a file for download from the data store

    parameters:
        - name: uuid
          in: path
          type: string
          description: UUID of the file to be checked out
        - name: fileName
          in: query
          type: string
          description: The desired name of the file. If absent, the UUID of the file will be used.

    :return: A 200 response with a JSON body describing the status of the checkout performed by DSS.

    All other query parameters are forwarded to DSS in order to initiate checkout process for the correct file. For
    more information refer to https://dss.data.humancellatlas.org under GET `/files/{uuid}`.

    If the file checkout has been started or is still ongoing, the response will look like:

    ```
    {
        "Status": 301,
        "Retry-After": 2,
        "Location": "https://…"
    }
    ```

    The `Status` field emulates HTTP status code 301 Moved Permanently.

    `Retry-After` is the recommended number of seconds to wait before requesting the URL specified in
     the `Location` field.

    `Location` is the URL to make a GET request to in order to recheck the status of the checkout process.

    If the client receives a response body with the `Status` field set to 301, the client should wait the number of
    seconds specified in `Retry-After` and then request the URL given in the `Location` field. The URL will point
    back at this endpoint so the client should expect a response of the same shape. Note that the actual HTTP
    response is of status 200, only the `Status` field of the body will be 301. The intent is to emulate HTTP while
    bypassing the default client behavior which, in most web browsers, is to ignore `Retry-After`. The response
    described here is intended to be processed by client-side Javascript such that the recommended delay in
    `Retry-After` can be handled in Javascript rather that relying on the native implementation by the web browser.

    If the file checkout is done and the file is ready to be downloaded, the response will be:

    ```
    {
        "Status": 302,
        "Location": "https://org-humancellatlas-dss-checkout.s3.amazonaws.com/blobs/…"
    }
    ```

    The client should request the URL given in the `Location` field. The URL will point to an entirely different
    service and when requesting the URL, the client should expect a response containing the actual manifest.
    Currently the `Location` field of the final response is a signed URL to an object in S3 but clients should not
    depend on that. The response will also include a `Content-Disposition` header set to `attachment; filename=`
    followed by the value of the fileName parameter specified in the initial request or the UUID of the file if that
    parameter was omitted.
    """
    params = app.current_request.query_params
    url = config.dss_endpoint + '/files/' + urllib.parse.quote(uuid, safe='')
    file_name = params.pop('fileName', None)
    dss_response = requests.get(url, params=params, allow_redirects=False)
    if dss_response.status_code == 301:
        retry_after = int(dss_response.headers.get('Retry-After'))
        location = dss_response.headers['Location']
        location = urllib.parse.urlparse(location)
        query = urllib.parse.parse_qs(location.query, strict_parsing=True)
        params = {k: one(v) for k, v in query.items()}
        if file_name is not None:
            params['fileName'] = file_name
        body = {"Status": 301, "Retry-After": retry_after, "Location": file_url(uuid, **params)}
        return Response(body=json.dumps(body), status_code=200)
    elif dss_response.status_code == 302:
        location = dss_response.headers['Location']
        # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved
        if True:
            location = urllib.parse.urlparse(location)
            query = urllib.parse.parse_qs(location.query, strict_parsing=True)
            expires = int(one(query['Expires']))
            s3 = boto3.client('s3')
            if file_name is None:
                file_name = uuid
            bucket = location.netloc.partition('.')[0]
            assert bucket == config.dss_checkout_bucket
            location = s3.generate_presigned_url(ClientMethod=s3.get_object.__name__,
                                                 ExpiresIn=round(expires - time.time()),
                                                 Params={
                                                     'Bucket': bucket,
                                                     'Key': location.path[1:],
                                                     'ResponseContentDisposition': 'attachment;filename=' + file_name,
                                                 })
        body = {"Status": 302, "Location": location}
        return Response(body=json.dumps(body), status_code=200)
    else:
        dss_response.raise_for_status()


def file_url(uuid, **params):
    uuid = urllib.parse.quote(uuid, safe="")
    url = self_url(endpoint_path=f'{proxy_endpoint_path}/{uuid}')
    params = urllib.parse.urlencode(params)
    return f'{url}?{params}'


def self_url(endpoint_path=None):
    protocol = app.current_request.headers.get('x-forwarded-proto', 'http')
    base_url = app.current_request.headers['host']
    if endpoint_path is None:
        endpoint_path = app.current_request.context['path']
    retry_url = f'{protocol}://{base_url}{endpoint_path}'
    return retry_url


@app.route('/url', methods=['POST'], cors=True)
def shorten_query_url():
    """
    Take a URL as input and return a (potentially) shortened URL that will redirect to the given URL

    parameters:
        - name: url
          in: body
          type: string
          description: URL to shorten

    :return: A 200 response with JSON body containing the shortened URL:

    ```
    {
        "url": "http://url.data.humancellatlas.org/b3N"
    }
    ```

    A 400 error is returned if an invalid URL is given.  This could be a URL that is not whitelisted
    or a string that is not a valid web URL.
    """
    try:
        url = app.current_request.json_body['url']
    except KeyError:
        raise BadRequestError('`url` must be given in the request body')

    url_hostname = urllib.parse.urlparse(url).netloc
    if len(list(filter(lambda whitelisted_url: re.fullmatch(whitelisted_url, url_hostname),
                       config.url_shortener_whitelist))) == 0:
        raise BadRequestError('Invalid URL given')

    url_hash = hash_url(url)
    storage_service = StorageService(config.url_redirect_full_domain_name)

    def get_url_response(path):
        return {'url': f'http://{config.url_redirect_full_domain_name}/{path}'}

    key_length = 3
    while key_length <= len(url_hash):
        key = url_hash[:key_length]
        try:
            existing_url = storage_service.get(key).decode(encoding='utf-8')
        except storage_service.client.exceptions.NoSuchKey:
            try:
                storage_service.put(key,
                                    data=bytes(url, encoding='utf-8'),
                                    ACL='public-read',
                                    WebsiteRedirectLocation=url)
            except ClientError as e:
                if e.response['Error']['Code'] == 'InvalidRedirectLocation':
                    raise BadRequestError('Invalid URL given')
                else:
                    raise
            return get_url_response(key)
        if existing_url == url:
            return get_url_response(key)
        key_length += 1
    raise ChaliceViewError('Could not create shortened URL')


def hash_url(url):
    url_hash = hashlib.sha1(bytes(url, encoding='utf-8')).digest()
    return base64.urlsafe_b64encode(url_hash).decode()


# TODO: Authentication for carts
def get_user_id():
    user_id = app.current_request.headers.get('Fake-Authorization', '')
    if user_id == '' or app.current_request.context['identity']['sourceIp'] not in config.cart_api_ip_whitelist:
        raise UnauthorizedError('Missing access key')
    return user_id


def transform_cart_to_response(cart):
    """
    Remove fields from response to return only user-relevant attributes
    """
    return {
        'CartId': cart['CartId'],
        'CartName': cart['CartName']
    }


@app.route('/resources/carts', methods=['POST'], cors=True)
def create_cart():
    """
    Create a cart with the given name for the authenticated user

    Returns a 400 error if a cart with the given name already exists

    parameters:
        - name: CartName
          in: body
          type: string
          description: Name to give the cart (must be unique to the user)

    :return: Name and ID of the created cart
        {
            "CartName": str,
            "CartId": str
        }
    """
    user_id = get_user_id()
    try:
        cart_name = app.current_request.json_body['CartName']
    except KeyError:
        raise BadRequestError('CartName parameter must be given')
    try:
        cart_id = CartItemManager().create_cart(user_id, cart_name, False)
    except DuplicateItemError as e:
        raise BadRequestError(e.msg)
    return {
        'CartId': cart_id,
        'CartName': cart_name
    }


# TODO: implement default cart (may need to change get_all_carts() endpoint)
@app.route('/resources/carts/{cart_id}', methods=['GET'], cors=True)
def get_cart(cart_id):
    """
    Get the cart of the given ID belonging to the user

    Returns a 404 error if the cart does not exist or does not belong to the user

    :return: {
        "CartName": str,
        "CartId": str
    }
    """
    user_id = get_user_id()
    cart = CartItemManager().get_cart(user_id, cart_id)
    if cart is None:
        raise NotFoundError('Cart does not exist')
    return transform_cart_to_response(cart)


@app.route('/resources/carts', methods=['GET'], cors=True)
def get_all_carts():
    """
    Get a list of all carts belonging the user

    :return: {
        "carts": [
            {
                "CartName": str,
                "CartId": str
            },
            ...
        ]
    }
    """
    user_id = get_user_id()
    carts = CartItemManager().get_user_carts(user_id)
    return [transform_cart_to_response(cart) for cart in carts]


@app.route('/resources/carts/{cart_id}', methods=['DELETE'], cors=True)
def delete_cart(cart_id):
    """
    Delete the given cart if it exists and return the deleted cart

    Returns a 404 error if the cart does not exist or does not belong to the user

    :return: The deleted cart
        {
            "CartName": str,
            "CartId": str
        }
    """
    user_id = get_user_id()
    deleted_cart = CartItemManager().delete_cart(user_id, cart_id)
    if deleted_cart is None:
        raise NotFoundError('Cart does not exist')
    return transform_cart_to_response(deleted_cart)


@app.route('/resources/carts/{cart_id}', methods=['PUT'], cors=True)
def update_cart(cart_id):
    """
    Update a cart's attributes.  Only the listed parameters can be updated

    Returns a 404 error if the cart does not exist or does not belong to the user

    parameters:
        - name: CartName
          in: body
          type: string
          description: Name to update the cart with (must be unique to the user)

    :return: The updated cart
        {
            "CartName": str,
            "CartId": str
        }
    """
    user_id = get_user_id()
    request_body = app.current_request.json_body
    update_params = dict(request_body)
    try:
        updated_cart = CartItemManager().update_cart(user_id, cart_id, update_params)
    except ResourceAccessError as e:
        raise NotFoundError(e.msg)
    except DuplicateItemError as e:
        raise BadRequestError(e.msg)
    return transform_cart_to_response(updated_cart)


@app.route('/resources/carts/{cart_id}/items', methods=['GET'], cors=True)
def get_items_in_cart(cart_id):
    """
    Get a list of items in a cart

    Returns a 404 error if the cart does not exist or does not belong to the user

    :return: {
        "CartId": str,
        "items": [
            {
                "CartItemId": str,
                "CartId": str,
                "EntityId": str,
                "BundleUuid": str,
                "BundleVersion": str,
                "EntityType": str
            },
            ...
        ]
    }
    """
    user_id = get_user_id()
    try:
        return {
            'CartId': cart_id,
            'items': CartItemManager().get_cart_items(user_id, cart_id)
        }
    except ResourceAccessError as e:
        raise NotFoundError(e.msg)


@app.route('/resources/carts/{cart_id}/items', methods=['POST'], cors=True)
def add_item_to_cart(cart_id):
    """
    Add cart item to a cart and return the ID of the created item

    Returns a 404 error if the cart does not exist or does not belong to the user
    Returns a 400 error if an invalid item was given

    parameters:
        - name: EntityId
          in: body
          type: string
        - name: BundleUuid
          in: body
          type: string
        - name: BundleVersion
          in: body
          type: string
        - name: EntityType
          in: body
          type: string

    :return: {
        "CartItemId": str
    }
    """
    user_id = get_user_id()
    try:
        request_body = app.current_request.json_body
        entity_id = request_body['EntityId']
        bundle_id = request_body['BundleUuid']
        bundle_version = request_body['BundleVersion']
        entity_type = request_body['EntityType']
    except KeyError:
        raise BadRequestError('EntityId, BundleUuid, BundleVersion, and EntityType must be given')
    try:
        item_id = CartItemManager().add_cart_item(user_id, cart_id, entity_id, bundle_id, bundle_version, entity_type)
    except ResourceAccessError as e:
        raise NotFoundError(e.msg)
    return {
        'CartItemId': item_id
    }


@app.route('/resources/carts/{cart_id}/items/{item_id}', methods=['DELETE'], cors=True)
def delete_cart_item(cart_id, item_id):
    """
    Delete an item from the cart

    Returns a 404 error if the cart does not exist or does not belong to the user, or if the item does not exist

    :return: If an item was deleted, return:
        ```
        {
            "deleted": true
        }
        ```

    """
    user_id = get_user_id()
    try:
        deleted_item = CartItemManager().delete_cart_item(user_id, cart_id, item_id)
    except ResourceAccessError as e:
        raise NotFoundError(e.msg)
    if deleted_item is None:
        raise NotFoundError('Item does not exist')
    return {'deleted': True}


@app.route('/resources/carts/{cart_id}/items/batch', methods=['POST'], cors=True)
def add_all_results_to_cart(cart_id):
    """
    Add all entities matching the given filters to a cart

    parameters:
        - name: filters
          in: body
          type: string
          description: Filter for the entities to add to the cart
        - name: entityType
          in: body
          type: string
          description: Entity type to apply the filters on

    :return: number of items that will be written and a URL to check the status of the write
        e.g.: {
            "count": 1000,
            "statusUrl": "https://status.url/resources/carts/status/{token}"
        }
    """
    user_id = get_user_id()
    request_body = app.current_request.json_body
    try:
        entity_type = request_body['entityType']
        filters = request_body['filters']
    except KeyError:
        raise BadRequestError('entityType and filters must be given')

    if entity_type not in {'files', 'specimens', 'projects'}:
        raise BadRequestError('entityType must be one of files, specimens, or projects')

    try:
        filters = json.loads(filters)
    except json.JSONDecodeError:
        raise BadRequestError('Invalid filters given')
    hits, search_after = EsTd().transform_cart_item_request(entity_type, filters=filters, size=1)
    item_count = hits.total

    write_params = {
        'filters': filters,
        'entity_type': entity_type,
        'cart_id': cart_id,
        'item_count': item_count,
        'batch_size': 10000
    }
    token = CartItemManager().start_batch_cart_item_write(user_id, cart_id, write_params)
    status_url = self_url(f'/resources/carts/status/{token}')

    return {'count': item_count, 'statusUrl': status_url}


@app.lambda_function(name=config.cart_item_write_lambda_basename)
def cart_item_write_batch(event, context):
    """Write a single batch to Dynamo and return pagination information for next batch to write"""
    entity_type = event['entity_type']
    filters = event['filters']
    cart_id = event['cart_id']
    batch_size = event['batch_size']

    if 'write_result' in event:
        search_after = event['write_result']['search_after']
    else:
        search_after = None

    num_written, next_search_after = CartItemManager().write_cart_item_batch(entity_type, filters, cart_id,
                                                                             batch_size, search_after)
    return {
        'search_after': next_search_after,
        'count': num_written
    }


@app.route('/resources/carts/status/{token}', methods=['GET'], cors=True)
def get_cart_item_write_progress(token):
    """
    Get the status of a batch cart item write job

    Returns a 400 error if the token cannot be decoded or the token points to a non-existent execution

    parameters:
        - name: token
          in: path
          type: string
          description: An opaque string generated by the server containing information about the write job to check

    :return: The status of the job

        If the job is still running a URL to recheck the status is given:
            e.g.:
            ```
            {
                "done": false,
                "statusUrl": "https://status.url/resources/carts/status/{token}"
            }
            ```

        If the job is finished, a boolean indicating if the write was successful is returned:
            e.g.:
            ```
            {
                "done": true,
                "success": true
            }
            ```
    """
    try:
        status = CartItemManager().get_batch_cart_item_write_status(token)
    except (KeyError, UnicodeDecodeError, binascii.Error, json.decoder.JSONDecodeError):
        raise BadRequestError('Invalid token given')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ExecutionDoesNotExist':
            raise BadRequestError('Invalid token given')
        else:
            raise
    response = {
        'done': status != 'RUNNING',
    }
    if not response['done']:
        response['statusUrl'] = self_url()
    else:
        response['success'] = status == 'SUCCEEDED'
    return response
