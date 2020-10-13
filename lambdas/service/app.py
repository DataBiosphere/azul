import base64
import binascii
import copy
from functools import (
    lru_cache,
)
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
    Sequence,
)
import urllib.parse

from botocore.exceptions import (
    ClientError,
)
import chalice
# noinspection PyPackageRequirements
from chalice import (
    AuthResponse,
    BadRequestError,
    ChaliceViewError,
    NotFoundError,
    Response,
)
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    IndexName,
    RequirementError,
    config,
    drs,
)
from azul.chalice import (
    AzulChaliceApp,
)
from azul.drs import (
    AccessMethod,
)
from azul.health import (
    HealthController,
)
from azul.logging import (
    configure_app_logging,
)
from azul.openapi import (
    application_json,
    format_description,
    params,
    responses,
    schema,
)
from azul.plugins import (
    MetadataPlugin,
    ServiceConfig,
)
from azul.portal_service import (
    PortalService,
)
from azul.security.authenticator import (
    AuthenticationError,
    Authenticator,
)
from azul.service import (
    BadArgumentException,
)
from azul.service.async_manifest_service import (
    AsyncManifestService,
)
from azul.service.cart_export_job_manager import (
    CartExportJobManager,
    InvalidExecutionTokenError,
)
from azul.service.cart_export_service import (
    CartExportService,
)
from azul.service.cart_item_manager import (
    CartItemManager,
    DuplicateItemError,
    ResourceAccessError,
)
from azul.service.collection_data_access import (
    CollectionDataAccess,
)
from azul.service.drs_controller import (
    DRSController,
)
from azul.service.elasticsearch_service import (
    ElasticsearchService,
    IndexNotFoundError,
    Pagination,
)
from azul.service.index_query_service import (
    EntityNotFoundError,
    IndexQueryService,
)
from azul.service.manifest_service import (
    ManifestFormat,
    ManifestService,
)
from azul.service.repository_controller import (
    RepositoryController,
)
from azul.service.storage_service import (
    StorageService,
)
from azul.strings import (
    pluralize,
)
from azul.types import (
    JSON,
    MutableJSON,
)
from azul.uuids import (
    InvalidUUIDError,
)

log = logging.getLogger(__name__)

spec = {
    'openapi': '3.0.1',
    'info': {
        'title': config.service_name,
        'description': format_description(f'''

            # Overview

            Azul is a REST web service for querying metadata associated with
            both experimental and analysis data stored in the [HCA Data Store
            (DSS)](https://github.com/HumanCellAtlas/data-store). In order to
            deliver response times that make it suitable for interactive use
            cases, the set of metadata properties that it exposes for sorting,
            filtering, and aggregation is limited. Azul provides a uniform view
            of the metadata over a range of diverse schemas, effectively
            shielding clients from changes in the schema as they occur over
            time. It does so, however, at the expense of detail in the set of
            metadata properties it exposes and in the accuracy with which it
            aggregates them.

            Azul denormalizes and aggregates metadata into several different
            indices for selected entity types:

             - [projects](#operations-Index-get_index_projects)

             - [samples](#operations-Index-get_index_samples)

             - [files](#operations-Index-get_index_files)

             - [bundles](#operations-Index-get_index_bundles)

            This set of indexes forms a catalog. There is a default catalog
            called `{config.default_catalog}` which will be used unless a
            different catalog name is specified using the `catalog` query
            parameter. Metadata from different catalogs is completely
            independent: a response obtained by querying one catalog does not
            necessarily correlate to a response obtained by querying another
            one. Two catalogs can  contain metadata from the same source or
            different sources. It is only guranteed that the body of a
            response by any given endpoint adheres to one schema,
            independently of what catalog was specified in the request.

            Azul provides the ability to download metadata in tabular form via
            the [Manifests](#operations-tag-Manifests) endpoints. The resulting
            manifests include links to associated data files and can be used by
            the [DCP CLI](https://github.com/HumanCellAtlas/dcp-cli) to download
            the listed files. Manifests can be generated for a selection of
            files using filters. These filters are interchangeable with the
            filters used by the [Index](#operations-tag-Index) endpoints.

            Azul also provides a
            [summary](#operations-Index-get_index_summary) view of
            indexed data.

            ## Data model

            Any index, when queried, returns a JSON array of hits. Each hit
            represents a metadata entity. Nested in each hit is a summary of the
            properties of entities associated with the hit. An entity is
            associated either by a direct edge in the original metadata graph,
            or indirectly as a series of edges. The nested properties are
            grouped by the type of the associated entity. The properties of all
            data files associated with a particular sample, for example, are
            listed under `hits[*].files` in a `/index/samples` response. It
            is important to note that while each _hit_ represents a discrete
            entity, the properties nested within that hit are the result of an
            aggregation over potentially many associated entities.

            To illustrate this, consider a data file that is part of two
            projects (a project is a group of related experiments, typically by
            one laboratory, institution or consortium). Querying the `files`
            index for this file yields a hit looking something like:

            ```
            {{
                "projects": [
                    {{
                        "projectTitle": "Project One"
                        "laboratory": ...,
                        ...
                    }},
                    {{
                        "projectTitle": "Project Two"
                        "laboratory": ...,
                        ...
                    }}
                ],
                "files": [
                    {{
                        "format": "pdf",
                        "name": "Team description.pdf",
                        ...
                    }}
                ]
            }}
            ```

            This example hit contains two kinds of nested entities (a hit in
            an actual Azul response will contain more): There are the two
            projects entities, and the file itself. These nested entities
            contain selected metadata properties extracted in a consistent way.
            This makes filtering and sorting simple.

            Also notice that there is only one file. When querying a particular
            index, the corresponding entity will always be a singleton like
            this.
        '''),
        # Version should be updated in any PR tagged API with a major version
        # update for breaking changes, and a minor version otherwise
        'version': '1.0'
    },
    'tags': [
        {
            'name': 'Index',
            'description': 'Query the indices for entities of interest'
        },
        {
            'name': 'Manifests',
            'description': 'Complete listing of files matching a given filter in TSV and other formats'
        },
        {
            'name': 'Repository',
            'description': 'Access to data files in the underlying repository'
        },
        {
            'name': 'DSS',
            'description': 'Access to files maintained in the Data Store'
        },
        {
            'name': 'DRS',
            'description': 'DRS-compliant proxy of the underlying repository'
        },
        {
            'name': 'Auxiliary',
            'description': 'Describes various aspects of the Azul service'
        }
    ],
    'servers': [
        {'url': config.service_endpoint()}
    ],
}


class ServiceApp(AzulChaliceApp):

    @property
    def drs_controller(self):
        return self._create_controller(DRSController)

    @property
    def health_controller(self):
        # Don't cache. Health controller is meant to be short-lived since it
        # applies it's own caching. If we cached the controller, we'd never
        # observe any changes in health.
        return HealthController(lambda_name='service')

    @property
    def repository_controller(self):
        return self._create_controller(RepositoryController)

    def _create_controller(self, controller_cls):
        return controller_cls(lambda_context=self.lambda_context,
                              file_url_func=self.file_url)

    @property
    def catalog(self) -> str:
        request = self.current_request
        # request is none during `chalice package`
        if request is not None:
            # params is None whenever no params are passed
            params = request.query_params
            if params is not None:
                try:
                    return params['catalog']
                except KeyError:
                    pass
        return config.default_catalog

    @property
    def metadata_plugin(self) -> MetadataPlugin:
        return self._metadata_plugin(self.catalog)

    @lru_cache(maxsize=None)
    def _metadata_plugin(self, catalog: CatalogName):
        return MetadataPlugin.load(catalog).create()

    @property
    def service_config(self) -> ServiceConfig:
        return self.metadata_plugin.service_config()

    @property
    def facets(self) -> Sequence[str]:
        return sorted(self.service_config.translation.keys())

    def __init__(self):
        super().__init__(app_name=config.service_name,
                         # see LocalAppTestCase.setUpClass()
                         unit_test=globals().get('unit_test', False),
                         spec=spec)

    def get_pagination(self, entity_type: str) -> Pagination:
        query_params = self.current_request.query_params or {}
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
            pagination['search_after'] = [json.loads(sa), sa_uid]
        elif not sa and sb:
            pagination['search_before'] = [json.loads(sb), sb_uid]
        elif sa and sb:
            raise BadArgumentException("Bad arguments, only one of search_after or search_before can be set")
        pagination['_self_url'] = app.self_url()  # For `_generate_paging_dict()`
        return pagination

    def file_url(self,
                 catalog: CatalogName,
                 file_uuid: str,
                 fetch: bool = True,
                 **params: str) -> str:
        file_uuid = urllib.parse.quote(file_uuid, safe='')
        view_function = fetch_dss_files if fetch else dss_files
        url = self.self_url(endpoint_path=view_function.path.format(file_uuid=file_uuid))
        params = urllib.parse.urlencode(dict(params, catalog=catalog))
        return f'{url}?{params}'

    def self_url(self, endpoint_path=None) -> str:
        protocol = app.current_request.headers.get('x-forwarded-proto', 'http')
        base_url = app.current_request.headers['host']
        if endpoint_path is None:
            endpoint_path = app.current_request.context['path']
        return f'{protocol}://{base_url}{endpoint_path}'


app = ServiceApp()
configure_app_logging(app, log)

sort_defaults = {
    'files': ('fileName', 'asc'),
    'samples': ('sampleId', 'asc'),
    'projects': ('projectTitle', 'asc'),
    'bundles': ('bundleVersion', 'desc')
}


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


@app.route('/openapi', methods=['GET'], cors=True, method_spec={
    'summary': 'Return OpenAPI specifications for this service',
    'description': 'This endpoint returns the [OpenAPI specifications]'
                   '(https://github.com/OAI/OpenAPI-Specification) for this '
                   'service. These are the specifications used to generate the '
                   'page you are visiting now.',
    'responses': {
        '200': {
            'description': '200 response',
            **responses.json_content(
                schema.object(
                    openapi=str,
                    **{k: schema.object() for k in ['info', 'tags', 'servers', 'paths', 'components']}
                )
            )
        }
    },
    'tags': ['Auxiliary']
})
def openapi():
    return Response(status_code=200,
                    headers={"content-type": "application/json"},
                    body=app.specs)


health_up_key = {
    'up': format_description('''
        indicates the overall result of the health check
    '''),
}

fast_health_keys = {
    **{
        prop.key: format_description(prop.description)
        for prop in HealthController.fast_properties['service']
    },
    **health_up_key
}

health_all_keys = {
    **{
        prop.key: format_description(prop.description)
        for prop in HealthController.all_properties
    },
    **health_up_key
}


def health_spec(health_keys: dict):
    return {
        'responses': {
            f'{200 if up else 503}': {
                'description': format_description(f'''
                    {'The' if up else 'At least one of the'} checked resources
                    {'are' if up else 'is not'} healthy.

                    The response consists of the following keys:

                ''') + ''.join(f'* `{k}` {v}' for k, v in health_keys.items()) + format_description(f'''

                    The top-level `up` key of the response is
                    `{'true' if up else 'false'}`.

                ''') + (format_description(f'''
                    {'All' if up else 'At least one'} of the nested `up` keys
                    {'are `true`' if up else 'is `false`'}.
                ''') if len(health_keys) > 1 else ''),
                **responses.json_content(
                    schema.object(
                        additional_properties=schema.object(
                            additional_properties=True,
                            up=schema.enum(up)
                        ),
                        up=schema.enum(up)
                    ),
                    example={
                        k: up if k == 'up' else {} for k in health_keys
                    }
                )
            } for up in [True, False]
        },
        'tags': ['Auxiliary']
    }


@app.route('/health', methods=['GET'], cors=True, method_spec={
    'summary': 'Complete health check',
    'description': format_description('''
        Health check of the service and all resources it depends on. This may
        take long time to complete and exerts considerable load on the service.
        For that reason it should not be requested frequently or by automated
        monitoring facilities that would be better served by the
        [`/health/fast`](#operations-Auxiliary-get_health_fast) or
        [`/health/cached`](#operations-Auxiliary-get_health_cached) endpoints.
    '''),
    **health_spec(health_all_keys)
})
def health():
    return app.health_controller.health()


@app.route('/health/basic', methods=['GET'], cors=True, method_spec={
    'summary': 'Basic health check',
    'description': format_description('''
        Health check of only the REST API itself, excluding other resources
        the service depends on. A 200 response indicates that the service is
        reachable via HTTP(S) but nothing more.
    '''),
    **health_spec(health_up_key)
})
def basic_health():
    return app.health_controller.basic_health()


@app.route('/health/cached', methods=['GET'], cors=True, method_spec={
    'summary': 'Cached health check for continuous monitoring',
    'description': format_description('''
        Return a cached copy of the
        [`/health/fast`](#operations-Auxiliary-get_health_fast) response.
        This endpoint is optimized for continuously running, distributed health
        monitors such as Route 53 health checks. The cache ensures that the
        service is not overloaded by these types of health monitors. The cache
        is updated every minute.
    '''),
    **health_spec(fast_health_keys)
})
def cached_health():
    return app.health_controller.cached_health()


@app.route('/health/fast', methods=['GET'], cors=True, method_spec={
    'summary': 'Fast health check',
    'description': format_description('''
        Performance-optimized health check of the REST API and other critical
        resources the service depends on. This endpoint can be requested more
        frequently than [`/health`](#operations-Auxiliary-get_health) but
        periodically scheduled, automated requests should be made to
        [`/health/cached`](#operations-Auxiliary-get_health_cached).
    '''),
    **health_spec(fast_health_keys)
})
def fast_health():
    return app.health_controller.fast_health()


@app.route('/health/{keys}', methods=['GET'], cors=True, method_spec={
    'summary': 'Selective health check',
    'description': format_description('''
        This endpoint allows clients to request a health check on a specific set
        of resources. Each resource is identified by a *key*, the same key
        under which the resource appears in a
        [`/health`](#operations-Auxiliary-get_health) response.
    '''),
    **health_spec(health_all_keys)
}, path_spec={
    'parameters': [
        params.path(
            'keys',
            type_=schema.array(schema.enum(*sorted(HealthController.all_keys()))),
            description='''
                A comma-separated list of keys selecting the health checks to be
                performed. Each key corresponds to an entry in the response.
        ''')
    ],
})
def custom_health(keys: Optional[str] = None):
    return app.health_controller.custom_health(keys)


@app.schedule('rate(1 minute)', name=config.service_cache_health_lambda_basename)
def update_health_cache(_event: chalice.app.CloudWatchEvent):
    app.health_controller.update_cache()


@app.route('/version', methods=['GET'], cors=True, method_spec={
    'summary': 'Describe current version of the Azul service',
    'tags': ['Auxiliary'],
    'responses': {
        '200': {
            'description': 'Version endpoint is reachable.',
            **responses.json_content(
                schema.object(
                    git=schema.object(
                        commit=str,
                        dirty=bool
                    ),
                    changes=schema.array(
                        schema.object(
                            title=str,
                            issues=schema.array(str),
                            upgrade=schema.array(str),
                            notes=schema.optional(str)
                        )
                    )
                )
            )
        }
    }
})
def version():
    from azul.changelog import (
        compact_changes,
    )
    return {
        'git': config.lambda_git_status,
        'changes': compact_changes(limit=10)
    }


def validate_repository_search(params, **validators):
    validate_params(params, **{
        'catalog': validate_catalog,
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


min_page_size = 1
max_page_size = 1000


def validate_catalog(catalog):
    try:
        IndexName.validate_catalog_name(catalog)
    except RequirementError as e:
        raise BadRequestError(e)
    else:
        if catalog not in config.catalogs:
            raise BadRequestError(f'Catalog name {catalog!r} is invalid. '
                                  f'Must be one of {set(config.catalogs)}.')


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
        raise BadRequestError('Invalid value for parameter `size`')
    else:
        if size > max_page_size:
            raise BadRequestError(f'Invalid value for parameter `size`, must not be greater than {max_page_size}')
        elif size < min_page_size:
            raise BadRequestError('Invalid value for parameter `size`, must be greater than 0')


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

    >>> validate_filters('{"sampleDisease": ["H syndrome"]}') # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: \
    The `filters` parameter entry for `sampleDisease` must be a single-item dictionary.

    >>> validate_filters('{"sampleDisease": {"is": "H syndrome"}}') # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: The value of the `is` relation in the `filters` parameter entry for \
    `sampleDisease` is not a list.

    >>> validate_filters('{"sampleDisease": {"was": "H syndrome"}}') # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: The relation in the `filters` parameter entry for `sampleDisease` \
    must be one of ('is', 'contains', 'within', 'intersects')
    """
    try:
        filters = json.loads(filters)
    except Exception:
        raise BadRequestError('The `filters` parameter is not valid JSON')
    if type(filters) is not dict:
        raise BadRequestError('The `filters` parameter must be a dictionary.')
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


def validate_facet(facet_name: str):
    """
    >>> validate_facet('fileName')

    >>> validate_facet('fooBar')
    Traceback (most recent call last):
    ...
    chalice.app.BadRequestError: BadRequestError: Unknown facet `fooBar`
    """
    if facet_name not in app.service_config.translation:
        raise BadRequestError(msg=f'Unknown facet `{facet_name}`')


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

    :param query_params: the parameters to be validated

    :param allow_extra_params:

        When False, only parameters specified via '**validators' are
        accepted, and validation fails if additional parameters are present.
        When True, additional parameters are allowed but their value is not
        validated.

    :param validators:

        A dictionary mapping the name of a parameter to a function that will
        be used to validate the parameter if it is provided. The callable
        will be called with a single argument, the parameter value to be
        validated, and is expected to raise ValueError, TypeError or
        azul.RequirementError if the value is invalid. Only these exceptions
        will yield a 4xx status response, all other exceptions will yield a
        500 status response. If the validator is an instance of `Mandatory`,
        then validation will fail if its corresponding parameter is not
        provided.

    >>> validate_params({'order': 'asc'}, order=str)

    >>> validate_params({'size': 'foo'}, size=int)
    Traceback (most recent call last):
        ...
    chalice.app.BadRequestError: BadRequestError: Invalid input value for `size`

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
        except (TypeError, ValueError, RequirementError):
            raise BadRequestError(msg=f'Invalid input value for `{param_name}`')


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


def repository_search(entity_type: str, item_id: Optional[str]) -> JSON:
    query_params = app.current_request.query_params or {}
    validate_repository_search(query_params)
    catalog = app.catalog
    filters = query_params.get('filters')
    try:
        service = IndexQueryService()
        return service.get_data(catalog=catalog,
                                entity_type=entity_type,
                                file_url_func=app.file_url,
                                item_id=item_id,
                                filters=filters,
                                pagination=app.get_pagination(entity_type))
    except (BadArgumentException, InvalidUUIDError) as e:
        raise BadRequestError(msg=e)
    except (EntityNotFoundError, IndexNotFoundError) as e:
        raise NotFoundError(msg=e)


generic_object_spec = schema.object(additional_properties=True)
array_of_object_spec = schema.array(generic_object_spec)
hit_spec = schema.object(
    additional_properties=True,
    protocols=array_of_object_spec,
    entryId=str,
    samples=array_of_object_spec,
    specimens=array_of_object_spec,
    cellLines=array_of_object_spec,
    donorOrganisms=array_of_object_spec,
    organoids=schema.array(str),
    cellSuspensions=array_of_object_spec,
)

page_spec = schema.object(
    hits=schema.array(hit_spec),
    pagination=generic_object_spec,
    termFacets=generic_object_spec
)

filters_param_spec = params.query(
    'filters',
    schema.optional(application_json(schema.object_type(
        default='{}',
        example={'cellCount': {'within': [[10000, 1000000000]]}},
        properties={
            facet: {
                'oneOf': [
                    schema.object(is_=schema.array({})),
                    *(
                        schema.object_type({
                            op: schema.array({}, minItems=2, maxItems=2)
                        })
                        for op in ['contains', 'within', 'intersects']
                    )
                ]
            }
            for facet in app.facets
        }
    ))),
    description=format_description('''
        Criteria to filter entities from the search results.

        Each filter consists of a facet name, a relational operator, and an
        array of facet values. The available operators are "is", "within",
        "contains", and "intersects". Multiple filters are combined using "and"
        logic. An entity must match all filters to be included in the response.
        How multiple facet values within a single filter are combined depends
        on the operator.

        For the "is" operator, multiple values are combined using "or"
        logic. For example, `{"fileFormat": {"is": ["fastq", "fastq.gz"]}}`
        selects entities where the file format is either "fastq" or
        "fastq.gz". For the "within", "intersects", and "contains"
        operators, the facet values must come in nested pairs specifying
        upper and lower bounds, and multiple pairs are combined using "and"
        logic. For example, `{"donorCount": {"within": [[1,5], [5,10]]}}`
        selects entities whose donor organism count falls within both
        ranges, i.e., is exactly 5.''' + f'''

        Supported facet names are: {', '.join(app.facets)}
    ''')
)

catalog_param_spec = params.query(
    'catalog',
    schema.optional(schema.with_default(app.catalog,
                                        type_=schema.enum(*config.catalogs))),
    description='The name of the catalog to query.')


def repository_search_params_spec(index_name):
    sort_default, order_default = sort_defaults[index_name]
    return [
        catalog_param_spec,
        filters_param_spec,
        params.query(
            'size',
            schema.optional(schema.with_default(10, type_=schema.in_range(min_page_size, max_page_size))),
            description='The number of hits included per page.'),
        params.query(
            'sort',
            schema.optional(schema.with_default(sort_default, type_=schema.enum(*app.facets))),
            description='The facet to sort the hits by.'),
        params.query(
            'order',
            schema.optional(schema.with_default(order_default, type_=schema.enum('asc', 'desc'))),
            description=format_description('''
                The ordering of the sorted hits, either ascending
                or descending.
            ''')
        ),
        *[
            params.query(
                param,
                schema.optional(str),
                description=format_description('''
                    Use the `next` and `previous` properties of the
                    `pagination` response element to navigate between pages.
                '''),
                deprecated=True)
            for param in [
                'search_before',
                'search_before_uid',
                'search_after',
                'search_after_uid'
            ]
        ]
    ]


def repository_search_spec(index_name):
    id_spec_link = f'#operations-Index-get_index_{index_name}__{index_name.rstrip("s")}_id_'
    return {
        'summary': f'Search the {index_name} index for entities of interest.',
        'tags': ['Index'],
        'parameters': repository_search_params_spec(index_name),
        'responses': {
            '200': {
                'description': format_description(f'''
                    Paginated list of {index_name} that meet the search
                    criteria ("hits"). The structure of these hits is documented
                    under the [corresponding endpoint for a specific entity]({id_spec_link}).

                    The `pagination` section describes the total number of hits
                    and total number of pages, as well as user-supplied search
                    parameters for page size and sorting behavior. It also
                    provides links for navigating forwards and backwards between
                    pages of results.

                    The `termFacets` section tabulates the occurrence of unique
                    values within nested fields of the `hits` section across all
                    entities meeting the filter criteria (this includes entities
                    not listed on the current page, meaning that this section
                    will be invariable across all pages from the same search).
                    Not every nested field is tabulated, but the set of
                    tabulated fields is consistent between entity types.
                '''),
                **responses.json_content(page_spec)
            }
        }
    }


def repository_id_spec(index_name_singular: str):
    search_spec_link = f'#operations-Index-get_index_{index_name_singular}s'
    return {
        'summary': f'Detailed information on a particular {index_name_singular} entity.',
        'tags': ['Index'],
        'parameters': [
            catalog_param_spec,
            params.path(f'{index_name_singular}_id', str, description=f'The UUID of the desired {index_name_singular}')
        ],
        'responses': {
            '200': {
                'description': format_description(f'''
                    This response describes a single {index_name_singular} entity. To
                    search the index for multiple entities, see the
                    [corresponding search endpoint]({search_spec_link}).

                    The properties that are common to all entity types are
                    listed in the schema below; however, additional properties
                    may be present for certain entity types. With the exception
                    of the {index_name_singular}'s unique identifier, all
                    properties are arrays, even in cases where only one value is
                    present.

                    The structures of the objects within these arrays are not
                    perfectly consistent, since they may represent either
                    singleton entities or aggregations depending on context.

                    For example, any biomaterial that yields a cell suspension
                    which yields a sequence file will be considered a "sample".
                    Therefore, the `samples` field is polymorphic, and each
                    sample may be either a specimen, an organoid, or a cell
                    line (the field `sampleEntityType` can be used to
                    discriminate between these cases).
                '''),
                **responses.json_content(hit_spec)
            }
        }
    }


def repository_head_spec(index_name):
    search_spec_link = f'#operations-Index-get_index_{index_name}'
    return {
        'summary': 'Perform a query without returning its result.',
        'tags': ['Index'],
        'responses': {
            '200': {
                'description': format_description(f'''
                    The HEAD method can be used to test whether the
                    {index_name} index is operational, or to check the validity
                    of query parameters for the
                    [GET method]({search_spec_link}).
                ''')
            }
        }
    }


def repository_head_search_spec(index_name):
    return {
        **repository_head_spec(index_name),
        'parameters': repository_search_params_spec(index_name)
    }


repository_summary_spec = {
    'tags': ['Index'],
    'parameters': [catalog_param_spec, filters_param_spec]
}


@app.route('/index/files', methods=['GET'], method_spec=repository_search_spec('files'), cors=True)
@app.route('/index/files', methods=['HEAD'], method_spec=repository_head_search_spec('files'), cors=True)
@app.route('/index/files/{file_id}', methods=['GET'], method_spec=repository_id_spec('file'), cors=True)
def get_data(file_id: Optional[str] = None) -> JSON:
    return repository_search('files', file_id)


@app.route('/index/samples', methods=['GET'], method_spec=repository_search_spec('samples'), cors=True)
@app.route('/index/samples', methods=['HEAD'], method_spec=repository_head_search_spec('samples'), cors=True)
@app.route('/index/samples/{sample_id}', methods=['GET'], method_spec=repository_id_spec('sample'), cors=True)
def get_sample_data(sample_id: Optional[str] = None) -> JSON:
    return repository_search('samples', sample_id)


@app.route('/index/bundles', methods=['GET'], method_spec=repository_search_spec('bundles'), cors=True)
@app.route('/index/bundles', methods=['HEAD'], method_spec=repository_head_search_spec('bundles'), cors=True)
@app.route('/index/bundles/{bundle_id}', methods=['GET'], method_spec=repository_id_spec('bundle'), cors=True)
def get_bundle_data(bundle_id: Optional[str] = None) -> JSON:
    return repository_search('bundles', bundle_id)


@app.route('/index/projects', methods=['GET'], method_spec=repository_search_spec('projects'), cors=True)
@app.route('/index/projects', methods=['HEAD'], method_spec=repository_head_search_spec('projects'), cors=True)
@app.route('/index/projects/{project_id}', methods=['GET'], method_spec=repository_id_spec('project'), cors=True)
def get_project_data(project_id: Optional[str] = None) -> JSON:
    return repository_search('projects', project_id)


@app.route('/index/summary', methods=['GET'], method_spec={
    'summary': 'Statistics on the data present across all entities.',
    'responses': {
        '200': {
            'description': format_description('''
                Counts the total number and total size in bytes of assorted
                entities, subject to the provided filters.

                `fileTypeSummaries` provides the count and total size in bytes
                of files grouped by their format, e.g. "fastq" or "matrix."
                `fileCount` and `totalFileSize` compile these figures across all
                file formats. Likewise, `cellCountSummaries` counts cells and
                their associated documents grouped by organ type, with
                `totalCellCount` compiling cell counts across organ types and
                `organTypes` listing all referenced organs.

                Total counts of unique entities are also provided for other
                entity types such as projects and tissue donors. These values
                are not grouped/aggregated.
            '''),
            **responses.json_content(
                schema.object(
                    additional_properties=True,
                    organTypes=schema.array(str),
                    totalFileSize=int,
                    fileTypeSummaries=array_of_object_spec,
                    totalCellCount=int,
                    cellCountSummaries=array_of_object_spec
                )
            )
        }
    },
    **repository_summary_spec
}, cors=True)
@app.route('/index/summary', methods=['HEAD'], method_spec={
    **repository_head_spec('summary'),
    **repository_summary_spec
})
def get_summary():
    """
    Returns a summary based on the filters passed on to the call. Based on the
    ICGC endpoint.
    :return: Returns a jsonified Summary API response
    """
    query_params = app.current_request.query_params or {}
    validate_params(query_params,
                    filters=str,
                    catalog=IndexName.validate_catalog_name)
    filters = query_params.get('filters')
    catalog = app.catalog
    service = IndexQueryService()
    try:
        return service.get_summary(catalog, filters)
    except BadArgumentException as e:
        raise BadRequestError(msg=e)


@app.route('/keywords', methods=['GET'], cors=True, method_spec={
    'deprecated': True,
    'responses': {'200': {'description': 'OK'}},
    'tags': ['Index']
})
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
    catalog = app.catalog
    filters = query_params.get('filters')
    _query = query_params.get('q', '')
    entity_type = query_params.get('type', 'files')
    field = query_params.get('field', 'fileId')
    service = IndexQueryService()
    try:
        pagination = app.get_pagination(entity_type)
    except BadArgumentException as e:
        raise BadRequestError(msg=e)
    return service.get_search(catalog, entity_type, pagination, filters, _query, field)


@app.route('/index/files/order', methods=['GET'], cors=True, method_spec={
    'parameters': [
        catalog_param_spec
    ],
    'deprecated': True,
    'responses': {'200': {'description': 'OK'}},
    'tags': ['Index']
})
def get_order():
    """
    Return the ordering on facets
    """
    return {'order': app.service_config.order_config}


token_param_spec = params.query('token',
                                schema.optional(str),
                                description='Reserved. Do not pass explicitly.')

manifest_path_spec = {
    'parameters': [
        catalog_param_spec,
        filters_param_spec,
        params.query(
            'format',
            schema.optional(schema.enum(*[format_.value for format_ in ManifestFormat], type_=str)),
            description=f'''
                The desired format of the output.

                - `{ManifestFormat.compact.value}` (the default) for a compact, tab-separated
                  manifest

                - `{ManifestFormat.full.value}` for a full tab-separated manifest

                - `{ManifestFormat.terra_bdbag.value}` for a manifest in the
                  [BDBag format][1]. This provides a ZIP file containing two manifests: one for
                  Participants (aka Donors) and one for Samples (aka Specimens). For more on the
                  format of the manifests see [documentation here][2]

                - `{ManifestFormat.curl.value}` for a [curl configuration file][3] manifest.
                This manifest can be used with the curl program to download all the files listed
                in the manifest

                [1]: http://bd2k.ini.usc.edu/tools/bdbag/

                [2]: https://software.broadinstitute.org/firecloud/documentation/article?id=10954

                [3]: https://curl.haxx.se/docs/manpage.html#-K
            ''',
        ),
        token_param_spec
    ],
}


# Copy of path_spec required due to FIXME: https://github.com/DataBiosphere/azul/issues/1646
@app.route('/manifest/files', methods=['GET'], cors=True, path_spec=copy.copy(manifest_path_spec), method_spec={
    'tags': ['Manifests'],
    'summary': 'Request a download link to a manifest file and redirect',
    'description': format_description('''
        Initiate and check status of a manifest generation job, returning
        either a 301 response redirecting to a URL to re-check the status of
        the manifest generation or a 302 response redirecting to the location
        of the completed manifest.
    '''),
    'responses': {
        '301': {
            'description': format_description('''
                The manifest generation has been started or is ongoing.
                The response is a redirect back to this endpoint, so the client
                should expect a subsequent response of the same kind.
            '''),
            'headers': {
                'Location': {
                    'description': 'URL to recheck the status of the '
                                   'manifest generation.',
                    'schema': {'type': 'string', 'format': 'url'},
                },
                'Retry-After': {
                    'description': 'Recommended number of seconds to wait '
                                   'before requesting the URL specified in '
                                   'the Location header.',
                    'schema': {'type': 'string'},
                },
            },
        },
        '302': {
            'description': format_description('''
                The manifest generation is complete and ready for download.
            '''),
            'headers': {
                'Location': {
                    'description': 'URL that will yield the actual '
                                   'manifest file.',
                    'schema': {'type': 'string', 'format': 'url'},
                },
                'Retry-After': {
                    'description': 'Recommended number of seconds to wait '
                                   'before requesting the URL specified in '
                                   'the Location header.',
                    'schema': {'type': 'string'},
                },
            },
        }
    },
})
def start_manifest_generation():
    wait_time, location = handle_manifest_generation_request()
    return Response(body='',
                    headers={
                        'Retry-After': str(wait_time),
                        'Location': location
                    },
                    status_code=301 if wait_time else 302)


# Copy of path_spec required due to FIXME: https://github.com/DataBiosphere/azul/issues/1646
@app.route('/fetch/manifest/files', methods=['GET'], cors=True, path_spec=copy.copy(manifest_path_spec), method_spec={
    'tags': ['Manifests'],
    'summary': 'Request a download link to a manifest file and check status',
    'description': format_description('''
        Initiate a manifest generation or check the status of an already ongoing
        generation, returning a 200 response with simulated HTTP headers in the
        body.
    '''),
    'responses': {
        '200': {
            **responses.json_content(
                schema.object(
                    Status=int,
                    Location={'type': 'string', 'format': 'url'},
                    **{'Retry-After': schema.optional(int)}
                )
            ),
            'description': format_description('''
                Manifest generation with status report, emulating the response
                code and headers of the `/manifest/files` endpoint. Note that
                the actual HTTP response will have status 200 while the `Status`
                field of the body will be 301 or 302. The intent is to emulate
                HTTP while bypassing the default client behavior, which (in most
                web browsers) is to ignore `Retry-After`. The response described
                here is intended to be processed by client-side Javascript such
                that the recommended delay in `Retry-After` can be handled in
                Javascript rather than relying on the native implementation by
                the web browser.

                For a detailed description of the fields in the response see
                the documentation for the headers they emulate in the
                [`/manifest/files`](#operations-Manifests-get_manifest_files)
                endpoint response.
            '''),
        },
    },
})
def start_manifest_generation_fetch():
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
    validate_params(query_params,
                    format=ManifestFormat,
                    catalog=IndexName.validate_catalog_name,
                    filters=str,
                    token=str)
    catalog = app.catalog
    filters = query_params.get('filters', '{}')
    validate_filters(filters)
    format_ = ManifestFormat(query_params.get('format', ManifestFormat.compact.value))
    service = ManifestService(StorageService())
    filters = service.parse_filters(filters)

    object_key = None
    token = query_params.get('token')
    if token is None:
        object_key, presigned_url = service.get_cached_manifest(format_=format_,
                                                                catalog=catalog,
                                                                filters=filters)
        if presigned_url is not None:
            return 0, presigned_url
    retry_url = app.self_url()
    async_service = AsyncManifestService()
    try:
        return async_service.start_or_inspect_manifest_generation(retry_url,
                                                                  format_=format_,
                                                                  catalog=catalog,
                                                                  filters=filters,
                                                                  token=token,
                                                                  object_key=object_key)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ExecutionDoesNotExist':
            raise BadRequestError('Invalid token given')
        raise
    except ValueError as e:
        raise BadRequestError(e.args)


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
    presigned_url, was_cached = service.get_manifest(format_=ManifestFormat(event['format']),
                                                     catalog=event['catalog'],
                                                     filters=event['filters'],
                                                     object_key=event['object_key'])
    return {'Location': presigned_url}


file_fqid_parameters_spec = [
    params.path(
        'file_uuid',
        str,
        description='The UUID of the file to be returned.'),
    params.query(
        'version',
        schema.optional(str),
        description=format_description('''
            The version of the file to be returned. File versions are opaque
            strings with only one documented property: they can be
            lexicographically compared with each other in order to determine
            which version is more recent. If this parameter is omitted then the
            most recent version of the file is returned.
        ''')
    )
]


# FIXME: remove /dss/files endpoint
#        https://github.com/databiosphere/azul/issues/2311

@app.route('/dss/files/{file_uuid}', methods=['GET'], cors=True)
def dss_files(file_uuid: str) -> Response:
    return repository_files(file_uuid)


# FIXME: remove /fetch/dss/files endpoint
#        https://github.com/databiosphere/azul/issues/2311

@app.route('/fetch/dss/files/{file_uuid}', methods=['GET'], cors=True)
def fetch_dss_files(file_uuid: str) -> Response:
    return fetch_repository_files(file_uuid)


repository_files_spec = {
    'tags': ['Repository'],
    'parameters': [
        catalog_param_spec,
        *file_fqid_parameters_spec,
        params.query(
            'fileName',
            schema.optional(str),
            description=format_description('''
                The desired name of the file. The given value will be included
                in the Content-Disposition header of the response. If absent, a
                best effort to determine the file name from metadata will be
                made. If that fails, the UUID of the file will be used instead.
            ''')
        ),
        params.query(
            'wait',
            schema.optional(int),
            description=format_description('''
                If 0, the client is responsible for honoring the waiting
                period specified in the Retry-After response header. If 1, the
                server will delay the response in order to consume as much of
                that waiting period as possible. This parameter should only be
                set to 1 by clients who can't honor the `Retry-After` header,
                preventing them from quickly exhausting the maximum number of
                redirects. If the server cannot wait the full amount, any
                amount of wait time left will still be returned in the
                Retry-After header of the response.
            ''')
        ),
        params.query(
            'replica',
            schema.optional(str),
            description=format_description('''
                If the underlying repository offers multiple replicas of the
                requested file, use the specified replica. Otherwise, this
                parameter is ignored. If absent, the only replica — for
                repositories that don't support replication — or the default
                replica — for those that do — will be used.

                All query parameters not mentioned above are forwarded to the
                underlying respository. For more information on the DSS as a
                repository refer to https://dss.data.humancellatlas.org under
                `GET /files/{uuid}`.
            '''),
        ),
        params.query(
            'requestIndex',
            schema.optional(int),
            description='Do not use. Reserved for internal purposes.'
        ),
        params.query(
            'drsPath',
            schema.optional(str),
            description='Do not use. Reserved for internal purposes.'
        ),
        token_param_spec
    ]
}


@app.route('/repository/files/{file_uuid}', methods=['GET'], cors=True, method_spec={
    **repository_files_spec,
    'summary': 'Redirect to a URL for downloading a given data file from the '
               'underlying repository',
    'responses': {
        '301': {
            'description': format_description('''
                A URL to the given file is still being prepared. Retry by
                waiting the number of seconds specified in the `Retry-After`
                header of the response and the requesting the URL specified in
                the `Location` header.
            '''),
            'headers': {
                'Location': responses.header(str, description=format_description('''
                    A URL pointing back at this endpoint, potentially with
                    different or additional request parameters.
                ''')),
                'Retry-After': responses.header(int, description=format_description('''
                    Recommended number of seconds to wait before requesting the
                    URL specified in the `Location` header. The response may
                    carry this header even if server-side waiting was requested
                    via `wait=1`.
                '''))
            }
        },
        '302': {
            'description': format_description('''
                The file can be downloaded from the URL returned in the
                `Location` header.
            '''),
            'headers': {
                'Location': responses.header(str, description=format_description('''
                        A URL that will yield the actual content of the file.
                ''')),
                'Content-Disposition': responses.header(str, description=format_description('''
                        Set to a value that makes user agents download the file
                        instead of rendering it, suggesting a meaningful name
                        for the downloaded file stored on the user's file
                        system. The suggested file name is taken  from the
                        `fileName` request parameter or, if absent, from
                        metadata describing the file. It generally does not
                        correlate with the path component of the URL returned
                        in the `Location` header.
                '''))
            }
        },
    }
})
def repository_files(file_uuid: str) -> Response:
    result = _repository_files(file_uuid, fetch=False)
    status_code = result.pop('Status')
    return Response(body='',
                    headers={k: str(v) for k, v in result.items()},
                    status_code=status_code)


@app.route('/fetch/repository/files/{file_uuid}', methods=['GET'], cors=True, method_spec={
    **repository_files_spec,
    'summary': 'Request a URL for downloading a given data file',
    'responses': {
        '200': {
            'description': format_description(f'''
                Emulates the response code and headers of {repository_files.path}
                while bypassing the default user agent behavior. Note that the
                status code of a successful response will be 200 while the
                `Status` field of its body will be 302.

                The response described here is intended to be processed by
                client-side Javascript such that the emulated headers can
                be handled in Javascript rather than relying on the native
                implementation by the web browser.
            '''),
            **responses.json_content(
                schema.object(
                    Status=int,
                    Location=str,
                )
            )
        }
    }
})
def fetch_repository_files(file_uuid: str) -> Response:
    body = _repository_files(file_uuid, fetch=True)
    return Response(body=json.dumps(body), status_code=200)


def _repository_files(file_uuid: str, fetch: bool) -> MutableJSON:
    query_params = app.current_request.query_params or {}

    def validate_replica(replica: str) -> None:
        if replica not in ('aws', 'gcp'):
            raise ValueError

    def validate_wait(wait: Optional[str]) -> Optional[int]:
        if wait is None:
            return None
        elif wait == '0':
            return False
        elif wait == '1':
            return True
        else:
            raise ValueError

    validate_params(query_params,
                    catalog=str,
                    version=str,
                    fileName=str,
                    wait=validate_wait,
                    requestIndex=int,
                    replica=validate_replica,
                    drsPath=str,
                    token=str)

    return app.repository_controller.download_file(catalog=app.catalog,
                                                   fetch=fetch,
                                                   file_uuid=file_uuid,
                                                   query_params=query_params)


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
    if 0 == len(list(filter(
        lambda whitelisted_url: re.fullmatch(whitelisted_url, url_hostname),
        config.url_shortener_whitelist
    ))):
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


@app.test_route('/resources/carts', methods=['POST'], cors=True, authorizer=jwt_auth)
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


@app.test_route('/resources/carts/{cart_id}', methods=['GET'], cors=True, authorizer=jwt_auth)
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


@app.test_route('/resources/carts', methods=['GET'], cors=True, authorizer=jwt_auth)
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


@app.test_route('/resources/carts/{cart_id}', methods=['DELETE'], cors=True, authorizer=jwt_auth)
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


@app.test_route('/resources/carts/{cart_id}', methods=['PUT'], cors=True, authorizer=jwt_auth)
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


@app.test_route('/resources/carts/{cart_id}/items', methods=['GET'], cors=True, authorizer=jwt_auth)
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


@app.test_route('/resources/carts/{cart_id}/items', methods=['POST'], cors=True, authorizer=jwt_auth)
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
        catalog = request_body['Catalog']
        entity_id = request_body['EntityId']
        entity_type = request_body['EntityType']
        entity_version = request_body.get('EntityVersion') or None
    except KeyError:
        raise BadRequestError('The request body properties `Catalog`, `EntityId` and `EntityType` are required')

    IndexName.validate_catalog_name(catalog, exception=BadRequestError)

    try:
        item_id = CartItemManager().add_cart_item(catalog=catalog,
                                                  user_id=user_id,
                                                  cart_id=cart_id,
                                                  entity_id=entity_id,
                                                  entity_type=entity_type,
                                                  entity_version=entity_version)
    except ResourceAccessError as e:
        raise NotFoundError(e.msg)
    return {
        'CartItemId': item_id
    }


@app.test_route('/resources/carts/{cart_id}/items/{item_id}', methods=['DELETE'], cors=True, authorizer=jwt_auth)
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


@app.test_route('/resources/carts/{cart_id}/items/batch', methods=['POST'], cors=True, authorizer=jwt_auth)
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
        catalog = request_body['catalog']
    except KeyError:
        raise BadRequestError('The request body properties `catalog`, `entityType` and `filters` are required')

    IndexName.validate_catalog_name(catalog, exception=BadRequestError)

    if entity_type not in {'files', 'samples', 'projects'}:
        raise BadRequestError('entityType must be one of files, samples, or projects')

    try:
        filters = json.loads(filters or '{}')
    except json.JSONDecodeError:
        raise BadRequestError('Invalid filters given')
    service = ElasticsearchService()
    hits, search_after = service.transform_cart_item_request(catalog=catalog,
                                                             entity_type=entity_type,
                                                             filters=filters,
                                                             size=1)
    item_count = hits.total

    token = CartItemManager().start_batch_cart_item_write(catalog=catalog,
                                                          user_id=user_id,
                                                          cart_id=cart_id,
                                                          entity_type=entity_type,
                                                          filters=filters,
                                                          item_count=item_count,
                                                          batch_size=10000)
    status_url = app.self_url(f'/resources/carts/status/{token}')

    return {'count': item_count, 'statusUrl': status_url}


@app.lambda_function(name=config.cart_item_write_lambda_basename)
def cart_item_write_batch(event, _context):
    """
    Write a single batch to Dynamo and return pagination information for next
    batch to write.
    """
    catalog = event['catalog']
    entity_type = event['entity_type']
    filters = event['filters']
    cart_id = event['cart_id']
    batch_size = event['batch_size']
    if 'write_result' in event:
        search_after = event['write_result']['search_after']
    else:
        search_after = None
    num_written, next_search_after = CartItemManager().write_cart_item_batch(
        catalog,
        entity_type,
        filters,
        cart_id,
        batch_size,
        search_after
    )
    return {
        'search_after': next_search_after,
        'count': num_written
    }


@app.test_route('/resources/carts/status/{token}', methods=['GET'], cors=True, authorizer=jwt_auth)
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
        response['statusUrl'] = app.self_url()
    else:
        response['success'] = status == 'SUCCEEDED'
    return response


def assert_jwt_ttl(expected_ttl):
    remaining_ttl = math.floor(int(app.current_request.context['authorizer']['exp']) - time.time())
    if remaining_ttl < expected_ttl:
        raise BadRequestError('The TTL of the access token is too short.')


@app.test_route('/resources/carts/{cart_id}/export', methods=['GET', 'POST'], cors=True, authorizer=jwt_auth)
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
          description: An opaque string describing the cart export job

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


@app.test_route('/fetch/resources/carts/{cart_id}/export', methods=['GET', 'POST'], cors=True, authorizer=jwt_auth)
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
          description: An opaque string describing the cart export job

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
        headers['Location'] = f'{app.self_url()}?token={token}'
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


drs_spec_description = format_description('''
    This is a partial implementation of the
    [DRS 1.0.0 spec](https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/).
    Not all features are implemented. This endpoint acts as a DRS-compliant
    proxy for accessing files in the underlying repository.

    Any errors encountered from the underlying repository are forwarded on as
    errors from this endpoint.
''')


@app.route(drs.drs_object_url_path('{file_uuid}'), methods=['GET'], cors=True, method_spec={
    'summary': 'Get file DRS object',
    'tags': ['DRS'],
    'description': format_description('''
        This endpoint returns object metadata, and a list of access methods that can
        be used to fetch object bytes.
    ''') + drs_spec_description,
    'parameters': file_fqid_parameters_spec,
    'responses': {
        '200': {
            'description': format_description('''
                A DRS object is returned. Two
                [`AccessMethod`s](https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/#_accessmethod)
                are included:

                {access_methods}

                If the object is not immediately ready, an `access_id` will be
                returned instead of an `access_url`.
            ''', access_methods='\n'.join(f'- {am!s}' for am in AccessMethod)),
            **app.drs_controller.get_object_response_schema()
        }
    },
})
def get_data_object(file_uuid):
    """
    Return a DRS data object dictionary for a given DSS file UUID and version.

    If the file is already checked out, we can return a drs_object with a URL
    immediately. Otherwise, we need to send the request through the /access
    endpoint.
    """
    query_params = app.current_request.query_params or {}
    validate_params(query_params, version=str)
    return app.drs_controller.get_object(file_uuid, query_params)


@app.route(drs.drs_object_url_path('{file_uuid}', access_id='{access_id}'), methods=['GET'], cors=True, method_spec={
    'summary': 'Get a file with an access ID',
    'description': format_description('''
        This endpoint returns a URL that can be used to fetch the bytes of a DRS
        object.

        This method only needs to be called when using an `AccessMethod` that
        contains an `access_id`.

        An `access_id` is returned when the underlying file is not ready. When
        the underlying repository is the DSS, the 202 response allowed time for
        the DSS to do a checkout.
    ''') + drs_spec_description,
    'parameters': [
        *file_fqid_parameters_spec,
        params.path('access_id', str, description='Access ID returned from a previous request')
    ],
    'responses': {
        '202': {
            'description': format_description('''
                This response is issued if the object is not yet ready. Respect
                the `Retry-After` header, then try again.
            '''),
            'headers': {
                'Retry-After': responses.header(str, description=format_description('''
                    Recommended number of seconds to wait before requesting the
                    URL specified in the Location header.
                '''))
            }
        },
        '200': {
            'description': format_description('''
                The object is ready. The URL is in the response object.
            '''),
            **responses.json_content(schema.object(url=str))
        }
    },
    'tags': ['DRS']
})
def get_data_object_access(file_uuid, access_id):
    query_params = app.current_request.query_params or {}
    validate_params(query_params, version=str)
    return app.drs_controller.get_object_access(access_id, file_uuid, query_params)


# TODO: Remove when DOS support is dropped
@app.route(drs.dos_object_url_path('{file_uuid}'), methods=['GET'], cors=True)
def dos_get_data_object(file_uuid):
    """
    Return a DRS data object dictionary for a given DSS file UUID and version.
    """
    query_params = app.current_request.query_params or {}
    validate_params(query_params,
                    version=str,
                    catalog=IndexName.validate_catalog_name)
    catalog = app.catalog
    file_version = query_params.get('version')
    return app.drs_controller.dos_get_object(catalog, file_uuid, file_version)
