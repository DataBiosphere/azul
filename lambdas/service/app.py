import ast
import logging.config
import os

from chalice import Chalice, BadRequestError, NotFoundError

from azul import config
from azul.service import service_config
from azul.service.responseobjects.elastic_request_builder import (BadArgumentException,
                                                                  IndexNotFoundError,
                                                                  ElasticTransformDump as EsTd)
from azul.service.responseobjects.utilities import json_pp

ENTRIES_PER_PAGE = 10

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)
for top_level_pkg in (__name__, 'azul'):
    logging.getLogger(top_level_pkg).setLevel(logging.INFO)

app = Chalice(app_name=config.service_name, configure_logs=False)
app.debug = True  # FIXME: this should be configurable via environment variable (https://github.com/DataBiosphere/azul/issues/419)
app.log.setLevel(logging.DEBUG)  # FIXME: please use module logger instead (https://github.com/DataBiosphere/azul/issues/419)


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


@app.route('/health', methods=['GET'], cors=True)
def health():
    from azul.health import get_elasticsearch_health

    return {
        'status': 'UP',
        'elasticsearch': get_elasticsearch_health()
    }


@app.route('/version', methods=['GET'], cors=True)
def version():
    return {
        'git': config.git_status
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


@app.route('/repository/summary/{entity_type}', methods=['GET'], cors=True)
def get_summary(entity_type=None):
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
    if entity_type not in ('specimens', 'files'):
        raise BadRequestError("Bad arguments, entity_type must be 'files' or 'specimens'")
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
    response = es_td.transform_summary(filters=filters, entity_type=entity_type)
    # Returning a single response if <file_id> request form is used
    return response


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
    response = es_td.transform_manifest(filters=filters)
    # Return the excel file
    return response
