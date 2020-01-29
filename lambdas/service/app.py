import base64
import binascii
import hashlib
import json
import logging.config
import math
import os
import re
import time
from typing import (
    Any,
    Callable,
    Mapping,
    Optional,
)
import urllib.parse

from botocore.exceptions import ClientError
import chalice
# noinspection PyPackageRequirements
from chalice import (
    AuthResponse,
    BadRequestError,
    ChaliceViewError,
    NotFoundError,
    Response,
)
from more_itertools import one
import requests

from azul import (
    config,
    drs,
)
from azul.chalice import AzulChaliceApp
from azul.deployment import aws
from azul.health import HealthController
from azul.logging import configure_app_logging
from azul.openapi import annotated_specs
from azul.plugin import Plugin
from azul.portal_service import PortalService
from azul.security.authenticator import (
    AuthenticationError,
    Authenticator,
)
from azul.service import BadArgumentException
from azul.service.async_manifest_service import AsyncManifestService
from azul.service.cart_export_job_manager import (
    CartExportJobManager,
    InvalidExecutionTokenError,
)
from azul.service.cart_export_service import CartExportService
from azul.service.cart_item_manager import (
    CartItemManager,
    DuplicateItemError,
    ResourceAccessError,
)
from azul.service.collection_data_access import CollectionDataAccess
from azul.service.elasticsearch_service import (
    ElasticsearchService,
    IndexNotFoundError,
)
from azul.service.manifest_service import ManifestService
from azul.service.repository_service import (
    EntityNotFoundError,
    InvalidUUIDError,
    RepositoryService,
)
from azul.service.storage_service import StorageService
from azul.strings import pluralize

log = logging.getLogger(__name__)

app = AzulChaliceApp(app_name=config.service_name)
configure_app_logging(app, log)

openapi_spec = {
    'info': {
        'description': 'This should probably be really long',
        # Version should be updated in any PR tagged API with a major version
        # update for breaking changes, and a minor version otherwise
        'version': '1.0'
    },
    'tags': [
        {
            'name': 'Health',
            'description': 'For checking on service health'
        }
    ],
    'servers': [
        {'url': config.service_endpoint()}
    ]
}

sort_defaults = {
    'files': ('fileName', 'asc'),
    'samples': ('sampleId', 'asc'),
    'projects': ('projectTitle', 'asc'),
    'bundles': ('bundleVersion', 'desc')
}


def _get_pagination(current_request, entity_type):
    query_params = current_request.query_params or {}
    default_sort, default_order = sort_defaults[entity_type]
    pagination = {
        "order": query_params.get('order', default_order),
        "size": int(query_params.get('size', '10')),
        "sort": query_params.get('sort', default_sort),
    }
    sa = query_params.get('search_after')
    sb = query_params.get('search_before')
    sa_uid = query_params.get('search_after_uid')
    sb_uid = query_params.get('search_before_uid')

    if not sb and sa:
        pagination['search_after'] = [sa, sa_uid]
    elif not sa and sb:
        pagination['search_before'] = [sb, sb_uid]
    elif sa and sb:
        raise BadArgumentException("Bad arguments, only one of search_after or search_before can be set")
    pagination['_self_url'] = self_url()  # For `_generate_paging_dict()`
    return pagination


@app.authorizer()
def jwt_auth(auth_request) -> AuthResponse:
    given_token = auth_request.token
    authenticator = Authenticator()

    try:
        claims = authenticator.authenticate_bearer_token(given_token)
    except AuthenticationError:
        # By specifying no routes, the access is denied.
        log.warning('Auth: ERROR', stack_info=True)
        return AuthResponse(routes=[], principal_id='anonymous')
    except Exception as e:
        # Unknown error, block all access temporarily.
        # Note: When the code is running on AWS, this function will become a
        #       separate lambda function, unlike how to work on a local machine.
        #       Due to that, this method cannot fail unexpectedly. Otherwise,
        #       The endpoint will respond with HTTP 500 with no log messages.
        log.error(f'Auth: AUTHENTICATE: Critical ERROR {e} {given_token}', stack_info=True, exc_info=e)
        return AuthResponse(routes=[], principal_id='no_access')

    # When this code is running on AWS, the context of the authorizer has to be
    # a string-to-primitive-type dictionary. When the context is retrieved in
    # the caller function, the value somehow got casted into string.
    context = {
        prop: value
        for prop, value in claims.items()
        if not isinstance(value, (list, tuple, dict))
    }
    context['token'] = given_token
    return AuthResponse(routes=['*'], principal_id='user', context=context)


pkg_root = os.path.dirname(os.path.abspath(__file__))


@app.route('/', cors=True)
def swagger_ui():
    local_path = os.path.join(pkg_root, 'vendor')
    dir_name = local_path if os.path.exists(local_path) else pkg_root
    with open(os.path.join(dir_name, 'static', 'swagger-ui.html')) as f:
        openapi_ui_html = f.read()
    return Response(status_code=200,
                    headers={"Content-Type": "text/html"},
                    body=openapi_ui_html)


@app.route('/openapi', methods=['GET'], cors=True)
def openapi():
    gateway_id = os.environ['api_gateway_id']
    spec = annotated_specs(gateway_id, app, openapi_spec)
    return Response(status_code=200,
                    headers={"content-type": "application/json"},
                    body=spec)


health_spec = {
    'responses': {
        '200': {
            'description': '200 response',
            'content': {
                'application/json': {
                    'schema': {'$ref': '#/components/schemas/Empty'}
                }
            }
        }
    },
    'tags': ['Health']
}


@app.route('/health', methods=['GET'], cors=True, method_spec={
    **health_spec,
    'summary': 'List health information for webservice'
})
@app.route('/health/{keys}', methods=['GET'], cors=True, path_spec={
    'parameters': [
        {
            'name': 'keys',
            'in': 'path',
            'required': True,
            'schema': {'type': 'string'}
        }
    ],
}, method_spec={
    **health_spec,
    'summary': 'List health information for a specific key',
    'description': 'List health information for a specific key, or all keys if `/health/` is used.'
})
def health(keys: Optional[str] = None):
    controller = HealthController(lambda_name='service',
                                  keys=keys,
                                  request_path=app.current_request.context['path'])
    return controller.response()


@app.schedule('rate(1 minute)', name=config.service_cache_health_lambda_basename)
def generate_health_object(_event: chalice.app.CloudWatchEvent):
    controller = HealthController(lambda_name='service')
    controller.generate_cache()


@app.route('/version', methods=['GET'], cors=True)
def version():
    from azul.changelog import compact_changes
    return {
        'git': config.lambda_git_status,
        'changes': compact_changes(limit=10)
    }


def validate_repository_search(params, **validators):
    validate_params(params, **{
        'filters': validate_filters,
        'order': str,
        'search_after': str,
        'search_after_uid': str,
        'search_before': str,
        'search_before_uid': str,
        'size': validate_size,
        'sort': validate_facet,
        **validators
    })


def validate_size(size):
    """
    >>> validate_size('1000')

    >>> validate_size('1001')
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: Invalid value for parameter `size`, must not be greater than 1000
    >>> validate_size('0')
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: Invalid value for parameter `size`, must be greater than 0
    >>> validate_size('foo')
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: Invalid value for parameter `size`
    """
    try:
        size = int(size)
    except BaseException:
        raise BadRequestError(f'Invalid value for parameter `size`')
    else:
        max_size = 1000
        if size > max_size:
            raise BadRequestError(f'Invalid value for parameter `size`, must not be greater than {max_size}')
        elif size < 1:
            raise BadRequestError(f'Invalid value for parameter `size`, must be greater than 0')


def validate_filters(filters):
    """
    >>> validate_filters('{"fileName": {"is": ["foo.txt"]}}')

    >>> validate_filters('"')
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: The `filters` parameter is not valid JSON

    >>> validate_filters('""')
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: The `filters` parameter must be a dictionary.

    >>> validate_filters('{"disease": ["H syndrome"]}') # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: \
    The `filters` parameter entry for `disease` must be a single-item dictionary.

    >>> validate_filters('{"disease": {"is": "H syndrome"}}') # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: The value of the `is` relation in the `filters` parameter entry for \
    `disease` is not a list.

    >>> validate_filters('{"disease": {"was": "H syndrome"}}') # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: The relation in the `filters` parameter entry for `disease` must be \
    one of ('is', 'contains', 'within', 'intersects')
    """
    try:
        filters = json.loads(filters)
    except Exception:
        raise BadRequestError(f'The `filters` parameter is not valid JSON')
    if type(filters) is not dict:
        raise BadRequestError(f'The `filters` parameter must be a dictionary.')
    for facet, filter_ in filters.items():
        validate_facet(facet)
        try:
            relation, value = one(filter_.items())
        except Exception:
            raise BadRequestError(f'The `filters` parameter entry for `{facet}` must be a single-item dictionary.')
        else:
            valid_relations = ('is', 'contains', 'within', 'intersects')
            if relation in valid_relations:
                if not isinstance(value, list):
                    raise BadRequestError(
                        msg=f'The value of the `{relation}` relation in the `filters` parameter '
                            f'entry for `{facet}` is not a list.')
            else:
                raise BadRequestError(f'The relation in the `filters` parameter entry for `{facet}`'
                                      f' must be one of {valid_relations}')


def validate_facet(value):
    """
    >>> validate_facet('fileName')

    >>> validate_facet('fooBar')
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: Invalid parameter `fooBar`
    """
    translation = Plugin.load().service_config().translation
    if value not in translation:
        raise BadRequestError(msg=f'Invalid parameter `{value}`')


class Mandatory:
    """
    Validation wrapper signifying that a parameter is mandatory.
    """

    def __init__(self, validator: Callable) -> None:
        super().__init__()
        self._validator = validator

    def __call__(self, param):
        return self._validator(param)


def validate_params(query_params: Mapping[str, str],
                    allow_extra_params: bool = False,
                    **validators: Callable[[Any], Any]) -> None:
    """
    Validates request query parameters for web-service API.

    :param query_params: Parameters to be validated.

    :param allow_extra_params: When False, only parameters specified via '**validators' are accepted, and validation
                               fails if additional parameters are present. When True, additional parameters are allowed
                               but their value is not validated.

    :param validators: A dictionary mapping the name of a parameter to a function that will be used to validate the
                       parameter if it is provided. The callable will be called with a single argument, the parameter
                       value to be validated, and is expected to raise ValueError or TypeError if the value is invalid.
                       Only these exceptions will yield a 4xx status response, all other exceptions will yield
                       a 500 status response. If the validator is an instance of `Mandatory`, then validation will fail
                       if its corresponding parameter is not provided.

    >>> validate_params({'order': 'asc'}, order=str)

    >>> validate_params({'size': 'foo'}, size=int)
    Traceback (most recent call last):
        ...
    chalice.app.BadRequestError: BadRequestError: Invalid input type for `size`

    >>> validate_params({'order': 'asc', 'foo': 'bar'}, order=str)
    Traceback (most recent call last):
        ...
    chalice.app.BadRequestError: BadRequestError: Invalid query parameter `foo`

    >>> validate_params({'order': 'asc', 'foo': 'bar'}, order=str, allow_extra_params=True)

    >>> validate_params({}, foo=str)

    >>> validate_params({}, foo=Mandatory(str))
    Traceback (most recent call last):
        ...
    chalice.app.BadRequestError: BadRequestError: Missing required query parameter `foo`

    """

    def fmt_error(err_description, params):
        joined = ', '.join(f'`{p}`' for p in params)
        return f'{err_description} {pluralize("query parameter", len(params))} {joined}'

    provided_params = set(query_params.keys())
    validation_params = set(validators.keys())
    mandatory_params = {p for p, v in validators.items() if isinstance(v, Mandatory)}

    if not allow_extra_params:
        extra_params = provided_params - validation_params
        if extra_params:
            raise BadRequestError(msg=fmt_error('Invalid', extra_params))

    if mandatory_params:
        missing_params = mandatory_params - provided_params
        if missing_params:
            # Sorting is to produce a deterministic error message
            raise BadRequestError(msg=fmt_error('Missing required', sorted(missing_params)))

    provided_params &= validation_params

    for param_name in provided_params:
        param_value = query_params[param_name]
        validator = validators[param_name]
        try:
            validator(param_value)
        except (TypeError, ValueError):
            raise BadRequestError(msg=f'Invalid input type for `{param_name}`')


@app.route('/integrations', methods=['GET'], cors=True)
def get_integrations():
    query_params = app.current_request.query_params or {}
    validate_params(query_params,
                    entity_type=Mandatory(str),
                    integration_type=Mandatory(str),
                    entity_ids=str)
    try:
        entity_ids = query_params['entity_ids']
    except KeyError:
        # Case where parameter is absent (do not filter using entity_id field)
        entity_ids = None
    else:
        if entity_ids:
            # Case where parameter is present and non-empty (filter for matching id value)
            entity_ids = {entity_id.strip() for entity_id in entity_ids.split(',')}
        else:
            # Case where parameter is present but empty (filter for missing entity_id field,
            # i.e., there are no acceptable id values)
            entity_ids = set()

    entity_type = query_params['entity_type']
    integration_type = query_params['integration_type']

    portal_service = PortalService()
    body = portal_service.list_integrations(entity_type, integration_type, entity_ids)
    return Response(status_code=200,
                    headers={"content-type": "application/json"},
                    body=json.dumps(body))


def repository_search(entity_type: str, item_id: str):
    query_params = app.current_request.query_params or {}
    validate_repository_search(query_params)
    filters = query_params.get('filters')
    try:
        pagination = _get_pagination(app.current_request, entity_type)
        service = RepositoryService()
        return service.get_data(entity_type, pagination, filters, item_id, file_url)
    except (BadArgumentException, InvalidUUIDError) as e:
        raise BadRequestError(msg=e)
    except (EntityNotFoundError, IndexNotFoundError) as e:
        raise NotFoundError(msg=e)


@app.route('/repository/files', methods=['HEAD', 'GET'], cors=True)
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
    return repository_search('files', file_id)


@app.route('/repository/samples', methods=['HEAD', 'GET'], cors=True)
@app.route('/repository/samples/{sample_id}', methods=['GET'], cors=True)
def get_sample_data(sample_id=None):
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
    return repository_search('samples', sample_id)


@app.route('/repository/bundles', methods=['HEAD', 'GET'], cors=True)
@app.route('/repository/bundles/{bundle_uuid}', methods=['GET'], cors=True)
def get_bundle_data(bundle_uuid=None):
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
    return repository_search('bundles', bundle_uuid)


@app.route('/repository/projects', methods=['HEAD', 'GET'], cors=True)
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
    return repository_search('projects', project_id)


@app.route('/repository/summary', methods=['HEAD', 'GET'], cors=True)
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
    query_params = app.current_request.query_params or {}
    validate_params(query_params, filters=str)
    filters = query_params.get('filters')
    service = RepositoryService()
    try:
        return service.get_summary(filters)
    except BadArgumentException as e:
        raise BadRequestError(msg=e)


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
    :return: A dictionary with entries that best match the query passed in
    to the endpoint
    """
    query_params = app.current_request.query_params or {}
    validate_repository_search(query_params, q=str, type=str, field=str)
    filters = query_params.get('filters')
    _query = query_params.get('q', '')
    entity_type = query_params.get('type', 'files')
    field = query_params.get('field', 'fileId')
    service = RepositoryService()
    try:
        pagination = _get_pagination(app.current_request, entity_type)
    except BadArgumentException as e:
        raise BadRequestError(msg=e)
    return service.get_search(entity_type, pagination, filters, _query, field)


@app.route('/repository/files/order', methods=['GET'], cors=True)
def get_order():
    """
    Return the ordering on facets
    """
    return {'order': Plugin.load().service_config().order_config}


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
        - name: format
          in: query
          type: string
          description: The desired format of the output. Possible values are `compact` (the default) for a tab-separated
          manifest and `terra.bdbag` for a manifest in the format documented `http://bd2k.ini.usc.edu/tools/bdbag/. The
          latter is essentially a ZIP file containing two manifests: one for participants (aka Donors) and one for
          samples (aka specimens). The format of the manifests inside the BDBag is documented here:
          https://software.broadinstitute.org/firecloud/documentation/article?id=10954
        - name: token
          in: query
          type: string
          description: Reserved. Do not pass explicitly.

    :return: If the manifest generation has been started or is still ongoing, the response will have a
    301 status and will redirect to a URL that will get a recheck the status of the manifest.

    If the manifest generation is done and the manifest is ready to be downloaded, the response will
    have a 302 status and will redirect to the URL of the manifest.
    """
    wait_time, location = handle_manifest_generation_request()
    return Response(body='',
                    headers={
                        'Retry-After': str(wait_time),
                        'Location': location
                    },
                    status_code=301 if wait_time else 302)


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
        - name: format
          in: query
          type: string
          description: The desired format of the output. Possible values are `compact` (the default) for a tab-separated
          manifest and `terra.bdbag` for a manifest in the format documented `http://bd2k.ini.usc.edu/tools/bdbag/. The
          latter is essentially a ZIP file containing two manifests: one for participants (aka Donors) and one for
          samples (aka specimens). The format of the manifests inside the BDBag is documented here:
          https://software.broadinstitute.org/firecloud/documentation/article?id=10954
        - name: token
          in: query
          type: string
          description: Reserved. Do not pass explicitly.

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
        "Location": "https://…"
    }
    ```

    The client should request the URL given in the `Location` field. The URL will point to a different service and
    the client should expect a response containing the actual manifest. Currently the `Location` field of the final
    response is a signed URL to an object in S3 but clients should not depend on that.
    """
    wait_time, location = handle_manifest_generation_request()
    response = {
        'Status': 301 if wait_time else 302,
        'Location': location
    }
    if wait_time:  # Only return Retry-After if manifest is not ready
        response['Retry-After'] = wait_time
    return response


def handle_manifest_generation_request():
    """
    Start a manifest generation job and return a status code, Retry-After, and
    a retry URL for the view function to handle.
    """
    query_params = app.current_request.query_params or {}
    validate_params(query_params, filters=str, format=str, token=str)
    format_ = query_params.get('format', 'compact')
    if format_ not in ('compact', 'terra.bdbag', 'full'):
        raise BadRequestError(f'{format_} is not a valid manifest format.')
    service = ManifestService(StorageService())
    filters = service.parse_filters(query_params.get('filters'))
    presigned_url = service.get_cached_manifest(format_, filters)
    if presigned_url is None:
        token = query_params.get('token')
        retry_url = self_url()
        async_service = AsyncManifestService()
        try:
            return async_service.start_or_inspect_manifest_generation(retry_url,
                                                                      token=token,
                                                                      format_=format_,
                                                                      filters=filters)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ExecutionDoesNotExist':
                raise BadRequestError('Invalid token given')
            raise
        except ValueError as e:
            raise BadRequestError(e.args)
    else:
        return 0, presigned_url


# noinspection PyUnusedLocal
@app.lambda_function(name=config.manifest_lambda_basename)
def generate_manifest(event, context):
    """
    Create a manifest based on the given filters and store it in S3

    :param: event: dict containing function input
        Valid params:
            - filters: dict containing filters to use in ES request
            - format: str to specify manifest output format, values are
                      'compact' (default) or 'terra.bdbag'
    :return: The URL to the generated manifest
    """
    service = ManifestService(StorageService())
    presigned_url = service.get_manifest(event['format'], event['filters'])
    return {'Location': presigned_url}


@app.route('/dss/files/{uuid}', methods=['GET'], cors=True)
def dss_files(uuid):
    """
    Initiate checking out a file for download from the HCA data store (DSS)

    parameters:
        - name: uuid
          in: path
          type: string
          description: UUID of the file to be checked out
        - name: fileName
          in: query
          type: string
          description: The desired name of the file. If absent, the UUID of the file will be used.
        - name: requestIndex
          in: query
          type: int
          description: Number of attempts made through the endpoint to fetch  the desired file.
        - name: wait
          in: query
          type: int
          description: If this parameter is 1 and the checkout is still in process, the server will wait before
          returning a response. This parameter should only be set to 1 by clients who don't honor the Retry-After
          header, preventing them from quickly exhausting the maximum number of redirects. If the server cannot wait
          the full amount, any amount of wait time left will still be returned in the Retry-After header of the
          response.

    :return: A 301 or 302 response describing the status of the checkout performed by DSS.

    All query parameters not mentioned above are forwarded to DSS in order to initiate checkout process for the
    correct file. For more information refer to https://dss.data.humancellatlas.org under GET `/files/{uuid}`.

    If the file checkout has been started or is still ongoing, the response will be a 301 redirect to this very
    endpoint. The response MAY carry a Retry-After header, even if server-side waiting was requested via `wait=1`.

    `Retry-After` is the recommended number of seconds to wait before requesting the URL specified in the `Location`
    header.

    If the client receives a 301 response, the client should wait the number of seconds specified in `Retry-After`
    and then request the URL given in the `Location` header. The URL in the `Location` header will point back at this
    endpoint so the client should expect a response of the same kind.

    If the file checkout is done and the file is ready to be downloaded, the response will be a 302 redirect with the
    `Location` header set to a signed URL. Requesting that URL will yield the actual content of the file.

    The client should request the URL given in the `Location` field. The URL will point to an entirely different
    service and when requesting the URL, the client should expect a response containing the actual file. Currently
    the `Location` header of the 302 response is a signed URL to an object in S3 but clients should not depend on
    that. The response will also include a `Content-Disposition` header set to `attachment; filename=` followed by
    the value of the `fileName` parameter specified in the initial request or the UUID of the file if that parameter
    was omitted.
    """
    body = _dss_files(uuid, fetch=False)
    status_code = body.pop('Status')
    return Response(body='',
                    headers={k: str(v) for k, v in body.items()},
                    status_code=status_code)


@app.route('/fetch/dss/files/{uuid}', methods=['GET'], cors=True)
def fetch_dss_files(uuid):
    """
    Initiate checking out a file for download from the HCA data store (DSS)

    parameters:
        - name: uuid
          in: path
          type: string
          description: UUID of the file to be checked out
        - name: fileName
          in: query
          type: string
          description: The desired name of the file. If absent, the UUID of the file will be used.
        - name: requestIndex
          in: query
          type: int
          description: Number of attempts made through the endpoint to fetch  the desired file.
        - name: wait
          in: query
          type: int
          description: If this parameter is 1 and the checkout is still in process, the server will wait before
          returning a response. This parameter should only be set to 1 by clients who don't honor the Retry-After
          header, preventing them from quickly exhausting the maximum number of redirects. If the server cannot wait
          the full amount, any amount of wait time left will still be returned in the Retry-After header of the
          response.

    :return: A 200 response with a JSON body describing the status of the checkout performed by DSS.

    All query parameters not mentioned above are forwarded to DSS in order to initiate checkout process for the
    correct file. For more information refer to https://dss.data.humancellatlas.org under GET `/files/{uuid}`.

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

    If the client receives a response body with the `Status` field set to 301, the client must wait the number of
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
    service and when requesting the URL, the client should expect a response containing the actual file. Currently
    the `Location` field of the final response is a signed URL to an object in S3 but clients should not depend on
    that. The response will also include a `Content-Disposition` header set to `attachment; filename=` followed by
    the value of the fileName parameter specified in the initial request or the UUID of the file if that parameter
    was omitted.
    """
    body = _dss_files(uuid, fetch=True)
    return Response(body=json.dumps(body), status_code=200)


def _dss_files(uuid, fetch=True):
    query_params = app.current_request.query_params
    validate_params(query_params, True, fileName=str, wait=int, requestIndex=int)
    dss_endpoint = config.dss_endpoint
    url = dss_endpoint + '/files/' + urllib.parse.quote(uuid, safe='')
    file_name = query_params.pop('fileName', None)
    wait = query_params.pop('wait', None)
    request_index = int(query_params.pop('requestIndex', '0'))
    dss_response = requests.get(url, params=query_params, allow_redirects=False)
    if dss_response.status_code == 301:
        retry_after = min(int(dss_response.headers.get('Retry-After')),
                          int(1.3 ** request_index))
        location = dss_response.headers['Location']
        location = urllib.parse.urlparse(location)
        query = urllib.parse.parse_qs(location.query, strict_parsing=True)
        query_params = {k: one(v) for k, v in query.items()}
        query_params['requestIndex'] = request_index + 1
        if file_name is not None:
            query_params['fileName'] = file_name
        if wait is not None:
            if wait == '0':
                pass
            elif wait == '1':
                # Sleep in the lambda but ensure that we wake up before it runs out of execution time (and before API
                # Gateway times out) so we get a chance to return a response to the client.
                remaining_lambda_seconds = app.lambda_context.get_remaining_time_in_millis() / 1000
                server_side_sleep = min(float(retry_after),
                                        remaining_lambda_seconds - config.api_gateway_timeout_padding - 3)
                time.sleep(server_side_sleep)
                retry_after = round(retry_after - server_side_sleep)
            else:
                raise BadRequestError(f"Invalid value '{wait}' for 'wait' parameter")
            query_params['wait'] = wait
        response = {
            "Status": 301,
            **({"Retry-After": retry_after} if retry_after else {}),
            "Location": file_url(uuid, fetch=fetch, **query_params)
        }
    elif dss_response.status_code == 302:
        location = dss_response.headers['Location']
        # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved
        if True:
            location = urllib.parse.urlparse(location)
            query = urllib.parse.parse_qs(location.query, strict_parsing=True)
            expires = int(one(query['Expires']))
            if file_name is None:
                file_name = uuid
            bucket = location.netloc.partition('.')[0]
            assert bucket == aws.dss_checkout_bucket(dss_endpoint), bucket
            with aws.direct_access_credentials(dss_endpoint, lambda_name='service'):
                # FIXME: make region configurable (https://github.com/DataBiosphere/azul/issues/1560)
                s3 = aws.client('s3', region_name='us-east-1')
                params = {
                    'Bucket': bucket,
                    'Key': location.path[1:],
                    'ResponseContentDisposition': 'attachment;filename=' + file_name,
                }
                location = s3.generate_presigned_url(ClientMethod=s3.get_object.__name__,
                                                     ExpiresIn=round(expires - time.time()),
                                                     Params=params)
        response = {"Status": 302, "Location": location}
    else:
        dss_response.raise_for_status()
        assert False
    return response


def file_url(uuid: str, fetch: bool = True, **params: str):
    uuid = urllib.parse.quote(uuid, safe="")
    view_function = fetch_dss_files if fetch else dss_files
    url = self_url(endpoint_path=view_function.path.format(uuid=uuid))
    params = urllib.parse.urlencode(params)
    return f'{url}?{params}'


def self_url(endpoint_path=None):
    protocol = app.current_request.headers.get('x-forwarded-proto', 'http')
    base_url = app.current_request.headers['host']
    if endpoint_path is None:
        endpoint_path = app.current_request.context['path']
    return f'{protocol}://{base_url}{endpoint_path}'


@app.route('/auth', methods=['GET'], cors=True)
def authenticate_via_fusillade():
    request = app.current_request
    authenticator = Authenticator()
    query_params = request.query_params or {}
    validate_params(query_params, redirect_uri=str)
    if authenticator.is_client_authenticated(request.headers):
        return Response(body='', status_code=200)
    else:
        return Response(body='',
                        status_code=302,
                        headers=dict(Location=Authenticator.get_fusillade_login_url(query_params.get('redirect_uri'))))


@app.route('/auth/callback', methods=['GET'], cors=True)
def handle_callback_from_fusillade():
    # For prototyping only
    try:
        request = app.current_request
        query = request.query_params or {}
        expected_params = ('access_token', 'id_token', 'expires_in', 'decoded_token', 'state')
        response_body = {k: json.loads(query[k]) if k == 'decoded_token' else query[k] for k in expected_params}
        response_body.update(dict(login_url=Authenticator.get_fusillade_login_url()))

        return Response(body=json.dumps(response_body), status_code=200)
    except KeyError:
        return Response(body='', status_code=400)


@app.route('/me', methods=['GET'], cors=True, authorizer=jwt_auth)
def access_info():
    request = app.current_request
    last_updated_timestamp = time.time()
    claims = {
        k: int(v) if k in ('exp', 'iat') else v
        for k, v in request.context['authorizer'].items()
    }
    ttl_in_seconds = math.floor(claims['exp'] - last_updated_timestamp)
    response_body = dict(
        claims=claims,
        ttl=ttl_in_seconds,
        last_updated=last_updated_timestamp
    )
    return Response(body=json.dumps(response_body, sort_keys=True),
                    status_code=200)


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
        "url": "http://url.singlecell.gi.ucsc.edu/b3N"
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


def get_user_id():
    return app.current_request.context['authorizer']['sub']


def transform_cart_to_response(cart):
    """
    Remove fields from response to return only user-relevant attributes
    """
    return {
        'CartId': cart['CartId'],
        'CartName': cart['CartName']
    }


@app.route('/resources/carts', methods=['POST'], cors=True, authorizer=jwt_auth)
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


@app.route('/resources/carts/{cart_id}', methods=['GET'], cors=True, authorizer=jwt_auth)
def get_cart(cart_id):
    """
    Get the cart of the given ID belonging to the user

    The default cart is accessible under its the actual cart UUID or by passing
    "default" as the cart ID. If the default cart does not exist, the endpoint
    WILL NOT create the default one automatically.

    This endpoint returns a 404 error if the cart does not exist or does not belong to the user.

    :return: {
        "CartName": str,
        "CartId": str
    }
    """
    user_id = get_user_id()
    cart_item_manager = CartItemManager()
    try:
        if cart_id == 'default':
            cart = cart_item_manager.get_default_cart(user_id)
        else:
            cart = cart_item_manager.get_cart(user_id, cart_id)
    except ResourceAccessError:
        raise NotFoundError('Cart does not exist')
    else:
        return transform_cart_to_response(cart)


@app.route('/resources/carts', methods=['GET'], cors=True, authorizer=jwt_auth)
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


@app.route('/resources/carts/{cart_id}', methods=['DELETE'], cors=True, authorizer=jwt_auth)
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


@app.route('/resources/carts/{cart_id}', methods=['PUT'], cors=True, authorizer=jwt_auth)
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


@app.route('/resources/carts/{cart_id}/items', methods=['GET'], cors=True, authorizer=jwt_auth)
def get_items_in_cart(cart_id):
    """
    Get a list of items in a cart
     parameters:
        - name: resume_token
          in: query
          type: string
          description: Reserved. Do not pass explicitly.

    The default cart is accessible under its the actual cart UUID or by passing
    "default" as the cart ID. If the default cart does not exist, the endpoint
    will create one automatically.

    Returns a 404 error if the cart does not exist or does not belong to the user

    :return: {
        "CartId": str,
        "Items": [
            {
                "CartItemId": str,
                "CartId": str,
                "EntityId": str,
                "EntityVersion": str,
                "EntityType": str
            },
            ...
        ]
    }
    """
    cart_id = None if cart_id == 'default' else cart_id
    user_id = get_user_id()
    query_params = app.current_request.query_params or {}
    validate_params(query_params, resume_token=str)
    resume_token = query_params.get('resume_token')
    try:
        page = CartItemManager().get_paginable_cart_items(user_id, cart_id, resume_token=resume_token)
        return {
            'CartId': cart_id,
            'Items': page['items'],
            'ResumeToken': page['resume_token'],
            'PageLength': page['page_length']
        }
    except ResourceAccessError as e:
        raise NotFoundError(e.msg)


@app.route('/resources/carts/{cart_id}/items', methods=['POST'], cors=True, authorizer=jwt_auth)
def add_item_to_cart(cart_id):
    """
    Add cart item to a cart and return the ID of the created item

    The default cart is accessible under its the actual cart UUID or by passing
    "default" as the cart ID. If the default cart does not exist, the endpoint
    will create one automatically.

    Returns a 404 error if the cart does not exist or does not belong to the user
    Returns a 400 error if an invalid item was given

    parameters:
        - name: EntityId
          in: body
          type: string
        - name: EntityType
          in: body
          type: string
        - name: EntityVersion
          in: body
          type: string
          required: false

    :return: {
        "CartItemId": str
    }
    """
    cart_id = None if cart_id == 'default' else cart_id
    user_id = get_user_id()
    try:
        request_body = app.current_request.json_body
        entity_id = request_body['EntityId']
        entity_type = request_body['EntityType']
        entity_version = request_body.get('EntityVersion') or None
    except KeyError:
        raise BadRequestError('EntityId and EntityType must be given')
    try:
        item_id = CartItemManager().add_cart_item(user_id=user_id,
                                                  cart_id=cart_id,
                                                  entity_id=entity_id,
                                                  entity_type=entity_type,
                                                  entity_version=entity_version)
    except ResourceAccessError as e:
        raise NotFoundError(e.msg)
    return {
        'CartItemId': item_id
    }


@app.route('/resources/carts/{cart_id}/items/{item_id}', methods=['DELETE'], cors=True, authorizer=jwt_auth)
def delete_cart_item(cart_id, item_id):
    """
    Delete an item from the cart

    The default cart is accessible under its the actual cart UUID or by passing
    "default" as the cart ID. If the default cart does not exist, the endpoint
    will create one automatically.

    Returns a 404 error if the cart does not exist or does not belong to the user, or if the item does not exist

    :return: If an item was deleted, return:
        ```
        {
            "deleted": true
        }
        ```

    """
    cart_id = None if cart_id == 'default' else cart_id
    user_id = get_user_id()
    try:
        deleted_item = CartItemManager().delete_cart_item(user_id, cart_id, item_id)
    except ResourceAccessError as e:
        raise NotFoundError(e.msg)
    if deleted_item is None:
        raise NotFoundError('Item does not exist')
    return {'deleted': True}


@app.route('/resources/carts/{cart_id}/items/batch', methods=['POST'], cors=True, authorizer=jwt_auth)
def add_all_results_to_cart(cart_id):
    """
    Add all entities matching the given filters to a cart

    The default cart is accessible under its the actual cart UUID or by passing
    "default" as the cart ID. If the default cart does not exist, the endpoint
    will create one automatically.

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
    cart_id = None if cart_id == 'default' else cart_id
    user_id = get_user_id()
    request_body = app.current_request.json_body
    try:
        entity_type = request_body['entityType']
        filters = request_body['filters']
    except KeyError:
        raise BadRequestError('entityType and filters must be given')

    if entity_type not in {'files', 'samples', 'projects'}:
        raise BadRequestError('entityType must be one of files, samples, or projects')

    try:
        filters = json.loads(filters or '{}')
    except json.JSONDecodeError:
        raise BadRequestError('Invalid filters given')
    service = ElasticsearchService()
    hits, search_after = service.transform_cart_item_request(entity_type, filters=filters, size=1)
    item_count = hits.total

    token = CartItemManager().start_batch_cart_item_write(user_id, cart_id, entity_type, filters, item_count, 10000)
    status_url = self_url(f'/resources/carts/status/{token}')

    return {'count': item_count, 'statusUrl': status_url}


@app.lambda_function(name=config.cart_item_write_lambda_basename)
def cart_item_write_batch(event, _context):
    """
    Write a single batch to Dynamo and return pagination information for next
    batch to write.
    """
    entity_type = event['entity_type']
    filters = event['filters']
    cart_id = event['cart_id']
    batch_size = event['batch_size']
    if 'write_result' in event:
        search_after = event['write_result']['search_after']
    else:
        search_after = None
    num_written, next_search_after = CartItemManager().write_cart_item_batch(entity_type,
                                                                             filters,
                                                                             cart_id,
                                                                             batch_size,
                                                                             search_after)
    return {
        'search_after': next_search_after,
        'count': num_written
    }


@app.route('/resources/carts/status/{token}', methods=['GET'], cors=True, authorizer=jwt_auth)
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


def assert_jwt_ttl(expected_ttl):
    remaining_ttl = math.floor(int(app.current_request.context['authorizer']['exp']) - time.time())
    if remaining_ttl < expected_ttl:
        raise BadRequestError('The TTL of the access token is too short.')


@app.route('/resources/carts/{cart_id}/export', methods=['GET', 'POST'], cors=True, authorizer=jwt_auth)
def export_cart_as_collection(cart_id: str):
    """
    Initiate and check status of a cart export job, returning a either a 301 or 302 response
    redirecting to either the location of the manifest or a URL to re-check the status of
    the export job.

    parameters:
        - name: cart_id
          in: path
          type: string
          description: The cart ID to export
        - name: token
          in: query
          type: string
          description: An opaque string describing the cart export job job

    :return: If the cart export has been started or is still ongoing, the response will have a
    301 status and will redirect to a URL that will get a recheck the status of the export.

    If the export is done, the response will have a 200 status and will be:

    ```
    {
        "CollectionUrl": str
    }
    ```

    The `CollectionUrl` is the URL to the collection on the DSS.

    If the export is timed out or aborted, the endpoint will respond with an empty HTTP 410 response.
    """
    result = handle_cart_export_request(cart_id)
    if result['status_code'] == 200:
        return {'CollectionUrl': result['headers']['Location']}
    else:
        return Response(body='',
                        status_code=result['status_code'],
                        headers=result['headers'])


@app.route('/fetch/resources/carts/{cart_id}/export', methods=['GET', 'POST'], cors=True, authorizer=jwt_auth)
def export_cart_as_collection_fetch(cart_id: str):
    """
    Initiate and check status of a cart export job, returning a either a 301 or 302 response
    redirecting to either the location of the manifest or a URL to re-check the status of
    the export job.

    parameters:
        - name: cart_id
          in: path
          type: string
          description: The cart ID to export
        - name: token
          in: query
          type: string
          description: An opaque string describing the cart export job job

    :return: A 200 response with a JSON body describing the status of the export.

    If the export generation has been started or is still ongoing, the response will look like:

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

    If the export is done, the response will be:

    ```
    {
        "Status": 200,
        "Location": "https://dss.<DEPLOYMENT>.singlecell.gi.ucsc.edu/v1/collections/<CART_ID>?..."
    }
    ```

    The `Location` is the URL to the collection on the DSS.

    If the export is timed out or aborted, the response will be:

    ```
    {
        "Status": 410
    }
    ```
    """
    result = handle_cart_export_request(cart_id)
    return {
        'Status': result['status_code'],
        **result['headers']
    }


def handle_cart_export_request(cart_id: str = None):
    assert_jwt_ttl(config.cart_export_min_access_token_ttl)
    user_id = get_user_id()
    query_params = app.current_request.query_params or {}
    validate_params(query_params, token=str)
    token = query_params.get('token')
    bearer_token = app.current_request.context['authorizer']['token']
    job_manager = CartExportJobManager()
    headers = {}
    if app.current_request.method == 'POST':
        token = job_manager.initiate(user_id, cart_id, bearer_token)
    try:
        job = job_manager.get(token)
    except InvalidExecutionTokenError:
        raise BadRequestError('Invalid token given')
    if job['user_id'] != user_id:
        raise NotFoundError  # This implies that this execution does not exist for this user.
    if job['status'] == 'SUCCEEDED':
        status_code = 200
        last_state = job['last_update']['state']
        collection_version = last_state['collection_version']
        collection_url = CollectionDataAccess.endpoint_url('collections', last_state['collection_uuid'])
        headers['Location'] = f"{collection_url}?version={collection_version}&replica=aws"
    elif job['status'] == 'FAILED':
        raise ChaliceViewError('Export failed')
    elif job['final']:
        status_code = 410  # job aborted
    else:
        status_code = 301
        headers['Location'] = f'{self_url()}?token={token}'
        headers['Retry-After'] = '10'
    return {
        'status_code': status_code,
        'headers': headers
    }


# noinspection PyUnusedLocal
@app.lambda_function(name=config.cart_export_dss_push_lambda_basename)
def cart_export_send_to_collection_api(event, context):
    """
    Export the data to DSS Collection API
    """
    execution_id = event['execution_id']
    user_id = event['user_id']
    cart_id = event['cart_id']
    access_token = event['access_token']
    collection_uuid = event['collection_uuid']
    collection_version = event['collection_version']
    job_starting_timestamp = event.get('started_at') or time.time()
    resume_token = event.get('resume_token')
    expected_exported_item_count = event.get('expected_exported_item_count')

    if resume_token is None:
        log.info('Export %s: Creating a new collection', execution_id)
        expected_exported_item_count = CartItemManager().get_cart_item_count(user_id, cart_id)
        log.info('Export %s: There are %d items to export.', execution_id, expected_exported_item_count)
    else:
        log.info('Export %s: Resuming', execution_id)

    batch = CartExportService().export(export_id=execution_id,
                                       user_id=user_id,
                                       cart_id=cart_id,
                                       access_token=access_token,
                                       resume_token=resume_token,
                                       collection_uuid=collection_uuid,
                                       collection_version=collection_version)
    last_updated_timestamp = time.time()
    updated_collection = batch['collection']
    next_resume_token = batch['resume_token']
    exported_item_count = batch['exported_item_count']

    return {
        'execution_id': execution_id,
        'access_token': access_token,
        'user_id': user_id,
        'cart_id': cart_id,
        'collection_uuid': updated_collection['uuid'],
        'collection_version': updated_collection['version'],
        'resumable': next_resume_token is not None,
        'resume_token': next_resume_token,
        'started_at': job_starting_timestamp,
        'last_updated_at': last_updated_timestamp,
        'exported_item_count': (event.get('exported_item_count') or 0) + exported_item_count,
        'expected_exported_item_count': expected_exported_item_count
    }


def file_to_drs(doc):
    """
    Converts an aggregate file document to a DRS data object response.
    """
    urls = [
        file_url(uuid=doc['uuid'],
                 version=doc['version'],
                 replica='aws',
                 fetch=False,
                 wait='1',
                 fileName=doc['name']),
        gs_url(doc['uuid'], doc['version'])
    ]

    return {
        'id': doc['uuid'],
        'urls': [
            {
                'url': url
            }
            for url in urls
        ],
        'size': str(doc['size']),
        'checksums': [
            {
                'checksum': doc['sha256'],
                'type': 'sha256'
            }
        ],
        'aliases': [doc['name']],
        'version': doc['version'],
        'name': doc['name']
    }


@app.route(drs.drs_http_object_path('{file_uuid}'), methods=['GET'], cors=True)
def get_data_object(file_uuid):
    """
    Return a DRS data object dictionary for a given DSS file UUID and version.
    """
    query_params = app.current_request.query_params or {}
    validate_params(query_params, version=str)
    file_version = query_params.get('version')
    filters = {
        "fileId": {"is": [file_uuid]},
        **({"fileVersion": {"is": [file_version]}} if file_version else {})
    }
    service = ElasticsearchService()
    pagination = _get_pagination(app.current_request, entity_type='files')
    response = service.transform_request(filters=filters,
                                         pagination=pagination,
                                         post_filter=True,
                                         entity_type='files')
    if response['hits']:
        doc = one(one(response['hits'])['files'])
        data_obj = file_to_drs(doc)
        assert data_obj['id'] == file_uuid
        assert file_version is None or data_obj['version'] == file_version
        return Response({'data_object': data_obj}, status_code=200)
    else:
        return Response({'msg': "Data object not found."}, status_code=404)


def gs_url(file_uuid, version):
    url = config.dss_endpoint + '/files/' + urllib.parse.quote(file_uuid, safe='')
    params = dict({'file_version': version} if version else {},
                  directurl=True,
                  replica='gcp')
    while True:
        if app.lambda_context.get_remaining_time_in_millis() / 1000 > 3:
            dss_response = requests.get(url, params=params, allow_redirects=False)
            if dss_response.status_code == 302:
                url = dss_response.next.url
                assert url.startswith('gs')
                return url
            elif dss_response.status_code == 301:
                url = dss_response.next.url
                remaining_lambda_seconds = app.lambda_context.get_remaining_time_in_millis() / 1000
                server_side_sleep = min(1, max(remaining_lambda_seconds - config.api_gateway_timeout_padding - 3, 0))
                time.sleep(server_side_sleep)
            else:
                raise ChaliceViewError({
                    'msg': f'Received {dss_response.status_code} from DSS. Could not get file'
                })
        else:
            raise GatewayTimeoutError({
                'msg': f"DSS timed out getting file: '{file_uuid}', version: '{version}'."
            })


class GatewayTimeoutError(ChaliceViewError):
    STATUS_CODE = 504
