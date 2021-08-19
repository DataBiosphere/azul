import base64
import hashlib
from inspect import (
    signature,
)
import json
import logging.config
import os
import re
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
    BadRequestError,
    ChaliceViewError,
    NotFoundError,
    Response,
    UnauthorizedError,
)
import chevron
from furl import (
    furl,
)
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    IndexName,
    RequirementError,
    cache,
    cached_property,
    config,
    drs,
)
from azul.auth import (
    OAuth2,
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
from azul.plugins.metadata.hca.transform import (
    value_and_unit,
)
from azul.portal_service import (
    PortalService,
)
from azul.service import (
    BadArgumentException,
)
from azul.service.catalog_controller import (
    CatalogController,
)
from azul.service.drs_controller import (
    DRSController,
)
from azul.service.elasticsearch_service import (
    IndexNotFoundError,
    Pagination,
)
from azul.service.index_query_service import (
    EntityNotFoundError,
    IndexQueryService,
)
from azul.service.manifest_controller import (
    ManifestController,
)
from azul.service.manifest_service import (
    CurlManifestGenerator,
    ManifestFormat,
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
    AnyJSON,
    JSON,
    LambdaContext,
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
            both experimental and analysis data from a data repository. In order
            to deliver response times that make it suitable for interactive use
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
            different sources. It is only guaranteed that the body of a
            response by any given endpoint adheres to one schema,
            independently of what catalog was specified in the request.

            Azul provides the ability to download data and metadata via the
            [Manifests](#operations-tag-Manifests) endpoints. The
            `{ManifestFormat.curl.value}` format manifests can be used to
            download data files. Other formats provide various views of the
            metadata. Manifests can be generated for a selection of files using
            filters. These filters are interchangeable with the filters used by
            the [Index](#operations-tag-Index) endpoints.

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
    ]
}


class ServiceApp(AzulChaliceApp):

    def spec(self) -> JSON:
        return {
            **super().spec(),
            **self._oauth2_spec()
        }

    def _oauth2_spec(self) -> JSON:
        scopes = ('email',)
        return {
            'components': {
                'securitySchemes': {
                    self.app_name: {
                        'type': 'oauth2',
                        'flows': {
                            'implicit': {
                                'authorizationUrl': 'https://accounts.google.com/o/oauth2/auth',
                                'scopes': {scope: scope for scope in scopes},
                            }
                        }
                    }
                }
            },
            'security': [
                {},
                {self.app_name: scopes}
            ],
        }

    @property
    def drs_controller(self) -> DRSController:
        return self._create_controller(DRSController)

    @property
    def health_controller(self) -> HealthController:
        # Don't cache. Health controller is meant to be short-lived since it
        # applies it's own caching. If we cached the controller, we'd never
        # observe any changes in health.
        return HealthController(lambda_name='service')

    @cached_property
    def catalog_controller(self) -> CatalogController:
        return self._create_controller(CatalogController)

    @property
    def repository_controller(self) -> RepositoryController:
        return self._create_controller(RepositoryController)

    @cached_property
    def manifest_controller(self) -> ManifestController:
        return self._create_controller(ManifestController,
                                       step_function_lambda_name=generate_manifest.name,
                                       manifest_url_func=self.manifest_url)

    def _create_controller(self, controller_cls, **kwargs):
        return controller_cls(lambda_context=self.lambda_context,
                              file_url_func=self.file_url,
                              **kwargs)

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

    @cache
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
        pagination = Pagination(order=query_params.get('order', default_order),
                                size=int(query_params.get('size', '10')),
                                sort=query_params.get('sort', default_sort),
                                self_url=app.self_url())  # For `_generate_paging_dict()`
        sa = query_params.get('search_after')
        sb = query_params.get('search_before')
        sa_uid = query_params.get('search_after_uid')
        sb_uid = query_params.get('search_before_uid')

        if not sb and sa:
            pagination.search_after = [json.loads(sa), sa_uid]
        elif not sa and sb:
            pagination.search_before = [json.loads(sb), sb_uid]
        elif sa and sb:
            raise BadArgumentException("Bad arguments, only one of search_after or search_before can be set")
        return pagination

    def file_url(self,
                 catalog: CatalogName,
                 file_uuid: str,
                 fetch: bool = True,
                 **params: str) -> str:
        file_uuid = urllib.parse.quote(file_uuid, safe='')
        view_function = fetch_repository_files if fetch else repository_files
        path = one(view_function.path)
        return furl(url=self.self_url(path.format(file_uuid=file_uuid)),
                    args=dict(catalog=catalog,
                              **params)).url

    def _authenticate(self) -> Optional[OAuth2]:
        try:
            header = self.current_request.headers['Authorization']
        except KeyError:
            return None
        else:
            try:
                auth_type, auth_token = header.split()
            except ValueError:
                raise UnauthorizedError(header)
            else:
                if auth_type.lower() == 'bearer':
                    return OAuth2(auth_token)
                else:
                    raise UnauthorizedError(header)

    def manifest_url(self,
                     fetch: bool,
                     catalog: CatalogName,
                     format_: ManifestFormat,
                     **params: str) -> str:
        view_function = fetch_file_manifest if fetch else file_manifest
        return furl(url=self.self_url(one(view_function.path)),
                    args=dict(catalog=catalog,
                              format=format_.value,
                              **params)).url


app = ServiceApp()
configure_app_logging(app, log)

sort_defaults = {
    'files': ('fileName', 'asc'),
    'samples': ('sampleId', 'asc'),
    'projects': ('projectTitle', 'asc'),
    'bundles': ('bundleVersion', 'desc')
}

pkg_root = os.path.dirname(os.path.abspath(__file__))


def vendor_html(*path: str) -> str:
    local_path = os.path.join(pkg_root, 'vendor')
    dir_name = local_path if os.path.exists(local_path) else pkg_root
    with open(os.path.join(dir_name, 'static', *path)) as f:
        html = f.read()
    return html


@app.route('/', cors=True)
def swagger_ui():
    swagger_ui_template = vendor_html('swagger-ui.html.template.mustache')
    swagger_ui_html = chevron.render(swagger_ui_template, {
        'OAUTH2_CLIENT_ID': json.dumps(config.google_oauth2_client_id),
        'OAUTH2_REDIRECT_URL': json.dumps(app.self_url('/oauth2_redirect'))
    })
    return Response(status_code=200,
                    headers={"Content-Type": "text/html"},
                    body=swagger_ui_html)


@app.route('/oauth2_redirect', enabled=config.google_oauth2_client_id is not None)
def oauth2_redirect():
    oauth2_redirec_html = vendor_html('oauth2-redirect.html')
    return Response(status_code=200,
                    headers={"Content-Type": "text/html"},
                    body=oauth2_redirec_html)


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
                    **{
                        k: schema.object()
                        for k in ('info', 'tags', 'servers', 'paths', 'components')
                    }
                )
            )
        }
    },
    'tags': ['Auxiliary']
})
def openapi():
    return Response(status_code=200,
                    headers={'content-type': 'application/json'},
                    body=app.spec())


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


@app.schedule('rate(1 minute)', name='servicecachehealth')
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
            if facet == 'organismAge':
                validate_organism_age_filter(value)


def validate_organism_age_filter(values):
    for value in values:
        try:
            value_and_unit.to_index(value)
        except RequirementError as e:
            raise BadRequestError(e)


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
    chalice.app.BadRequestError: BadRequestError: Invalid value for `size`

    >>> validate_params({'order': 'asc', 'foo': 'bar'}, order=str)
    Traceback (most recent call last):
        ...
    chalice.app.BadRequestError: BadRequestError: Unknown query parameter `foo`

    >>> validate_params({'order': 'asc', 'foo': 'bar'}, order=str, allow_extra_params=True)

    >>> validate_params({}, foo=str)

    >>> validate_params({}, foo=Mandatory(str))
    Traceback (most recent call last):
        ...
    chalice.app.BadRequestError: BadRequestError: Missing required query parameter `foo`

    """

    def fmt_error(err_description, params):
        # Sorting is to produce a deterministic error message
        joined = ', '.join(f'`{p}`' for p in sorted(params))
        return f'{err_description} {pluralize("query parameter", len(params))} {joined}'

    provided_params = query_params.keys()
    validation_params = validators.keys()
    mandatory_params = {p for p, v in validators.items() if isinstance(v, Mandatory)}

    if not allow_extra_params:
        extra_params = provided_params - validation_params
        if extra_params:
            raise BadRequestError(msg=fmt_error('Unknown', extra_params))

    if mandatory_params:
        missing_params = mandatory_params - provided_params
        if missing_params:
            raise BadRequestError(msg=fmt_error('Missing required', missing_params))

    for param_name, param_value in query_params.items():
        try:
            validator = validators[param_name]
        except KeyError:
            pass
        else:
            try:
                validator(param_value)
            except (TypeError, ValueError, RequirementError):
                raise BadRequestError(msg=f'Invalid value for `{param_name}`')


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


@app.route(
    '/index/catalogs',
    methods=['GET'],
    cors=True,
    method_spec={
        'summary': 'List all available catalogs',
        'tags': ['Index'],
        'responses': {
            '200': {
                'description': format_description('''
                    The name of the default catalog and a list of all available
                    catalogs. For each catalog, the response includes the name
                    of the atlas the catalog belongs to, a flag indicating
                    whether the catalog is for internal use only as well as the
                    names and types of plugins currently active for the catalog.
                    For some plugins, the response includes additional
                    configuration properties, such as the source used by the
                    repository plugin to populate the catalog.
                '''),
                **responses.json_content(
                    # The custom return type annotation is an experiment. Please
                    # don't adopt this just yet elsewhere in the program.
                    signature(app.catalog_controller.list_catalogs).return_annotation
                )
            }
        }
    }
)
def list_catalogs():
    return app.catalog_controller.list_catalogs()


def repository_search(entity_type: str,
                      item_id: Optional[str],
                      *,
                      filter_sources: bool
                      ) -> JSON:
    request = app.current_request
    query_params = request.query_params or {}
    validate_repository_search(query_params)
    catalog = app.catalog
    filters = query_params.get('filters')
    source_ids = app.repository_controller.list_source_ids(catalog,
                                                           request.authentication)
    try:
        service = IndexQueryService()
        return service.get_data(catalog=catalog,
                                entity_type=entity_type,
                                file_url_func=app.file_url,
                                item_id=item_id,
                                filters=filters,
                                source_ids=source_ids,
                                filter_sources=filter_sources,
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
    sources=array_of_object_spec,
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
    # FIXME: Spec for `filters` argument should be driven by field types
    #        https://github.com/DataBiosphere/azul/issues/2254
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
        ranges, i.e., is exactly 5.

        The organismAge facet is special in that it contains two property keys:
        value and unit. For example, `{"organismAge": {"is": [{"value": "20",
        "unit": "year"}]}}`. Both keys are required. `{"organismAge": {"is":
        [null]}}` selects entities that have no organism age.''' + f'''

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
    return repository_search('files', file_id, filter_sources=True)


@app.route('/index/samples', methods=['GET'], method_spec=repository_search_spec('samples'), cors=True)
@app.route('/index/samples', methods=['HEAD'], method_spec=repository_head_search_spec('samples'), cors=True)
@app.route('/index/samples/{sample_id}', methods=['GET'], method_spec=repository_id_spec('sample'), cors=True)
def get_sample_data(sample_id: Optional[str] = None) -> JSON:
    return repository_search('samples', sample_id, filter_sources=True)


@app.route('/index/bundles', methods=['GET'], method_spec=repository_search_spec('bundles'), cors=True)
@app.route('/index/bundles', methods=['HEAD'], method_spec=repository_head_search_spec('bundles'), cors=True)
@app.route('/index/bundles/{bundle_id}', methods=['GET'], method_spec=repository_id_spec('bundle'), cors=True)
def get_bundle_data(bundle_id: Optional[str] = None) -> JSON:
    return repository_search('bundles', bundle_id, filter_sources=True)


@app.route('/index/projects', methods=['GET'], method_spec=repository_search_spec('projects'), cors=True)
@app.route('/index/projects', methods=['HEAD'], method_spec=repository_head_search_spec('projects'), cors=True)
@app.route('/index/projects/{project_id}', methods=['GET'], method_spec=repository_id_spec('project'), cors=True)
def get_project_data(project_id: Optional[str] = None) -> JSON:
    return repository_search('projects', project_id, filter_sources=False)


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
                  format of the manifests see [documentation here][2].

                - `{ManifestFormat.terra_pfb.value}` for a manifest in the [PFB format][3]. This
                  format is mainly used for exporting data to Terra.

                - `{ManifestFormat.curl.value}` for a [curl configuration file][4] manifest.
                This manifest can be used with the curl program to download all the files listed
                in the manifest.

                [1]: http://bd2k.ini.usc.edu/tools/bdbag/

                [2]: https://software.broadinstitute.org/firecloud/documentation/article?id=10954

                [3]: https://github.com/uc-cdis/pypfb

                [4]: https://curl.haxx.se/docs/manpage.html#-K
            ''',
        ),
        params.query('objectKey',
                     schema.optional(str),
                     description='Reserved. Do not pass explicitly.'),
        token_param_spec
    ],
}


@app.route('/manifest/files', methods=['GET'], cors=True, path_spec=manifest_path_spec, method_spec={
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
def file_manifest():
    return _file_manifest(fetch=False)


keys = CurlManifestGenerator.command_lines('').keys()
command_line_spec = schema.object(**{key: str for key in keys})


@app.route('/fetch/manifest/files', methods=['GET'], cors=True, path_spec=manifest_path_spec, method_spec={
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
                    **{'Retry-After': schema.optional(int)},
                    CommandLine=command_line_spec
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
def fetch_file_manifest():
    return _file_manifest(fetch=True)


def _file_manifest(fetch: bool):
    query_params = app.current_request.query_params or {}
    query_params.setdefault('filters', '{}')
    query_params.setdefault('format', ManifestFormat.compact.value)
    validate_params(query_params,
                    format=ManifestFormat,
                    catalog=IndexName.validate_catalog_name,
                    filters=str,
                    token=str,
                    objectKey=str)
    validate_filters(query_params['filters'])
    return app.manifest_controller.get_manifest_async(self_url=app.self_url(),
                                                      catalog=app.catalog,
                                                      query_params=query_params,
                                                      fetch=fetch)


@app.lambda_function(name='manifest')
def generate_manifest(event: AnyJSON, context: LambdaContext):
    assert isinstance(event, Mapping)
    assert all(isinstance(k, str) for k in event.keys())
    return app.manifest_controller.get_manifest(event)


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
                Emulates the response code and headers of {one(repository_files.path)}
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
    request = app.current_request
    query_params = request.query_params or {}
    headers = request.headers

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

    # FIXME: Prevent duplicate filenames from files in different subgraphs by
    #        prepending the subgraph UUID to each filename when downloaded
    #        https://github.com/DataBiosphere/azul/issues/2682

    catalog = app.catalog
    return app.repository_controller.download_file(catalog=catalog,
                                                   fetch=fetch,
                                                   file_uuid=file_uuid,
                                                   query_params=query_params,
                                                   headers=headers,
                                                   authentication=request.authentication)


@app.route('/repository/sources', methods=['GET'], cors=True, method_spec={
    'summary': 'List available data sources',
    'tags': ['Repository'],
    'parameters': [catalog_param_spec],
    'responses': {
        '200': {
            'description': format_description('''
                List the sources the currently authenticated user is authorized
                to access in the underlying data repository.
            '''),
            **responses.json_content(
                schema.object(sources=schema.array(
                    schema.object(
                        sourceId=str,
                        sourceSpec=str,
                    )
                ))
            )
        },
    }
})
def list_sources() -> Response:
    validate_params(app.current_request.query_params or {},
                    catalog=validate_catalog)
    sources = app.repository_controller.list_sources(app.catalog,
                                                     app.current_request.authentication)
    return Response(body={'sources': sources}, status_code=200)


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


drs_spec_description = format_description('''
    This is a partial implementation of the
    [DRS 1.0.0 spec](https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/).
    Not all features are implemented. This endpoint acts as a DRS-compliant
    proxy for accessing files in the underlying repository.

    Any errors encountered from the underlying repository are forwarded on as
    errors from this endpoint.
''')


@app.route(
    drs.drs_object_url_path('{file_uuid}'),
    methods=['GET'],
    enabled=config.is_dss_enabled(),
    cors=True,
    method_spec={
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
    }
)
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


@app.route(
    drs.drs_object_url_path('{file_uuid}', access_id='{access_id}'),
    methods=['GET'],
    enabled=config.is_dss_enabled(),
    cors=True,
    method_spec={
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
    }
)
def get_data_object_access(file_uuid, access_id):
    query_params = app.current_request.query_params or {}
    validate_params(query_params, version=str)
    return app.drs_controller.get_object_access(access_id, file_uuid, query_params)


@app.route(
    drs.dos_object_url_path('{file_uuid}'),
    methods=['GET'],
    enabled=config.is_dss_enabled(),
    cors=True
)
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
