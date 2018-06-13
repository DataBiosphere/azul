import ast
from chalicelib import config
import logging.config
import os
from chalicelib.responseobjects.elastic_request_builder import BadArgumentException, ElasticTransformDump as EsTd
from chalicelib.responseobjects.utilities import json_pp
from chalice import Chalice

ENTRIES_PER_PAGE = 10

# Setting up logging
base_path = os.path.dirname(os.path.abspath(__file__))
app = Chalice(app_name=os.getenv('INDEXER_NAME', 'cgp-dashboard-service'))
app.debug = True
app.log.setLevel(logging.DEBUG)


# TODO: Write the docstrings so they can support swagger.
# Please see https://github.com/rochacbruno/flasgger
# stackoverflow.com/questions/43911510/ \
# how-to-write-docstring-for-url-parameters


@app.route('/')
def hello():
    return 'Hello World!'


def _get_pagination(current_request):
    pagination = {
        "order": current_request.query_params.get('order', 'desc'),
        "size": int(current_request.query_params.get('size', ENTRIES_PER_PAGE)),
        "sort": current_request.query_params.get('sort', 'entity_id'),
    }

    sa = current_request.query_params.get('search_after')
    sb = current_request.query_params.get('search_before')
    sa_uid = current_request.query_params.get('search_after_uid')
    sb_uid = current_request.query_params.get('search_before_uid')
    if not sa and not sb:
        pagination['from'] = int(current_request.query_params.get('from', 1))
    elif not sb:
        pagination['search_after'] = [sa, sa_uid]
    elif not sa:
        pagination['search_before'] = [sb, sb_uid]
    else:
        raise BadArgumentException("Bad arguments, only one of search_after or search_before can be set")

    return pagination


@app.route('/repository/files', methods=['GET'])
@app.route('/repository/files/{file_id}', methods=['GET'])
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
        es_td = EsTd(es_domain=os.getenv("ES_DOMAIN", "localhost"),
                     es_port=os.getenv("ES_PORT", 9200),
                     es_protocol=os.getenv("ES_PROTOCOL", "http"))
        # Get the response back
        logger.info("Creating the API response")
        response = es_td.transform_request(filters=filters,
                                           pagination=pagination,
                                           post_filter=True,
                                           index="ES_FILE_INDEX")
    except BadArgumentException as bae:
        response = dict(error=bae.message)
        response.status_code = 400
        return response
    except Exception as e:
        logger.error("Malformed filters parameter: {}".format(e))
        return "Malformed filters parameter"
    # Returning a single response if <file_id> request form is used
    if file_id is not None:
        response = response['hits'][0]
    return response


@app.route('/repository/files/piecharts', methods=['GET'])
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
    if app.current_request.query_params is None:
        app.current_request.query_params = {}
    # Get all the parameters from the URL
    filters = app.current_request.query_params.get('filters', '{"file": {}}')
    logger.debug("Filters string is: {}".format(filters))
    try:
        logger.info("Extracting the filter parameter from the request")
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters

        # Make the default pagination
        logger.info("Creating pagination")
        pagination = _get_pagination(app.current_request)
        # Create and instance of the ElasticTransformDump
        logger.info("Creating ElasticTransformDump object")
        es_td = EsTd(es_domain=os.getenv("ES_DOMAIN", "localhost"),
                     es_port=os.getenv("ES_PORT", 9200),
                     es_protocol=os.getenv("ES_PROTOCOL", "http"))
        # Get the response back
        logger.info("Creating the API response")
        response = es_td.transform_request(filters=filters,
                                           pagination=pagination,
                                           post_filter=False)
        # Returning a single response if <file_id> request form is used
        return response
    except BadArgumentException as bae:
        response = dict(error=bae.message)
        response.status_code = 400
        return response
    except Exception as e:
        logger.error("Malformed filters parameter: {}".format(e))
        return "Malformed filters parameter"


@app.route('/repository/files/summary', methods=['GET'])
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
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN", "localhost"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    logger.info("Creating the API response")
    response = es_td.transform_summary(filters=filters)
    # Returning a single response if <file_id> request form is used
    return response


@app.route('/keywords', methods=['GET'])
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
    pagination = {
        "from": int(app.current_request.query_params.get('from', 1)),
        "size": int(app.current_request.query_params.get('size', 5))
    }
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
    return response


@app.route('/repository/files/order', methods=['GET'])
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
    return {'order': order_list}


@app.route('/repository/files/export', methods=['GET'])
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
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN", "elasticsearch1"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    logger.info("Creating the API response")
    response = es_td.transform_manifest(filters=filters)
    # Return the excel file
    return response
