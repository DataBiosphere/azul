import base64
from collections.abc import (
    Mapping,
    Sequence,
)
from functools import (
    partial,
)
import hashlib
from inspect import (
    signature,
)
import json
import logging.config
from typing import (
    Any,
    Callable,
    Optional,
    Type,
    Union,
)
import urllib.parse

import attr
import chalice
from chalice import (
    BadRequestError as BRE,
    ChaliceViewError,
    NotFoundError,
    Response,
    UnauthorizedError,
)
from furl import (
    furl,
)
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    RequirementError,
    cache,
    cached_property,
    config,
    drs,
    mutable_furl,
    require,
)
from azul.auth import (
    OAuth2,
)
from azul.chalice import (
    AzulChaliceApp,
    C,
)
from azul.drs import (
    AccessMethod,
)
from azul.health import (
    HealthController,
)
from azul.indexer.document import (
    FieldType,
    Nested,
)
from azul.logging import (
    configure_app_logging,
)
from azul.openapi import (
    application_json,
    format_description as fd,
    params,
    responses,
    schema,
)
from azul.openapi.spec import (
    CommonEndpointSpecs,
)
from azul.plugins import (
    ManifestFormat,
    MetadataPlugin,
    RepositoryPlugin,
)
from azul.plugins.metadata.hca.indexer.transform import (
    value_and_unit,
)
from azul.portal_service import (
    PortalService,
)
from azul.service.catalog_controller import (
    CatalogController,
)
from azul.service.drs_controller import (
    DRSController,
)
from azul.service.elasticsearch_service import (
    Pagination,
)
from azul.service.manifest_controller import (
    ManifestController,
)
from azul.service.manifest_service import (
    CurlManifestGenerator,
    ManifestUrlFunc,
)
from azul.service.repository_controller import (
    RepositoryController,
)
from azul.strings import (
    pluralize,
)
from azul.types import (
    AnyJSON,
    JSON,
    LambdaContext,
    MutableJSON,
    PrimitiveJSON,
    reify,
)

log = logging.getLogger(__name__)

spec = {
    'openapi': '3.0.1',
    'info': {
        'title': config.service_name,
        'description': fd(f'''
            # Overview

            Azul is a REST web service for querying metadata associated with
            both experimental and analysis data from a data repository. In order
            to deliver response times that make it suitable for interactive use
            cases, the set of metadata properties that it exposes for sorting,
            filtering, and aggregation is limited. Azul provides a uniform view
            of the metadata over a range of diverse schemas, effectively
            shielding clients from changes in the schemas as they occur over
            time. It does so, however, at the expense of detail in the set of
            metadata properties it exposes and in the accuracy with which it
            aggregates them.

            Azul denormalizes and aggregates metadata into several different
            indices for selected entity types. Metadata entities can be queried
            using the [Index](#operations-tag-Index) endpoints.

            A set of indices forms a catalog. There is a default catalog called
            `{config.default_catalog}` which will be used unless a
            different catalog name is specified using the `catalog` query
            parameter. Metadata from different catalogs is completely
            independent: a response obtained by querying one catalog does not
            necessarily correlate to a response obtained by querying another
            one. Two catalogs can contain metadata from the same sources or
            different sources. It is only guaranteed that the body of a
            response by any given endpoint adheres to one schema,
            independently of which catalog was specified in the request.

            Azul provides the ability to download data and metadata via the
            [Manifests](#operations-tag-Manifests) endpoints. The
            `{ManifestFormat.curl.value}` format manifests can be used to
            download data files. Other formats provide various views of the
            metadata. Manifests can be generated for a selection of files using
            filters. These filters are interchangeable with the filters used by
            the [Index](#operations-tag-Index) endpoints.

            Azul also provides a [summary](#operations-Index-get_index_summary)
            view of indexed data.

            ## Data model

            Any index, when queried, returns a JSON array of hits. Each hit
            represents a metadata entity. Nested in each hit is a summary of the
            properties of entities associated with the hit. An entity is
            associated either by a direct edge in the original metadata graph,
            or indirectly as a series of edges. The nested properties are
            grouped by the type of the associated entity. The properties of all
            data files associated with a particular sample, for example, are
            listed under `hits[*].files` in a `/index/samples` response. It is
            important to note that while each _hit_ represents a discrete
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

            This example hit contains two kinds of nested entities (a hit in an
            actual Azul response will contain more): There are the two projects
            entities, and the file itself. These nested entities contain
            selected metadata properties extracted in a consistent way. This
            makes filtering and sorting simple.

            Also notice that there is only one file. When querying a particular
            index, the corresponding entity will always be a singleton like
            this.
        '''),
        # This property should be updated in any PR connected to an issue
        # labeled `API`. Increment the major version for backwards incompatible
        # changes and reset the minor version to zero. Otherwise, increment only
        # the minor version for backwards compatible changes. A backwards
        # compatible change is one that does not require updates to clients.
        'version': '1.0'
    },
    'tags': [
        {
            'name': 'Index',
            'description': fd('''
                Query the indices for entities of interest
            ''')
        },
        {
            'name': 'Manifests',
            'description': fd('''
                Complete listing of files matching a given filter in TSV and
                other formats
            ''')
        },
        {
            'name': 'Repository',
            'description': fd('''
                Access to data files in the underlying repository
            ''')
        },
        {
            'name': 'DSS',
            'description': fd('''
                Access to files maintained in the Data Store
            ''')
        },
        {
            'name': 'DRS',
            'description': fd('''
                DRS-compliant proxy of the underlying repository
            ''')
        },
        {
            'name': 'Auxiliary',
            'description': fd('''
                Describes various aspects of the Azul service
            ''')
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
                                'scopes': {scope: scope for scope in scopes}
                            }
                        }
                    }
                }
            },
            'security': [
                {},
                {self.app_name: scopes}
            ]
        }

    @property
    def drs_controller(self) -> DRSController:
        return self._service_controller(DRSController)

    @cached_property
    def health_controller(self) -> HealthController:
        return self._controller(HealthController, lambda_name='service')

    @cached_property
    def catalog_controller(self) -> CatalogController:
        return self._service_controller(CatalogController)

    @cached_property
    def repository_controller(self) -> RepositoryController:
        return self._service_controller(RepositoryController)

    @cached_property
    def manifest_controller(self) -> ManifestController:
        manifest_url_func: ManifestUrlFunc = self.manifest_url
        return self._service_controller(ManifestController,
                                        step_function_lambda_name=generate_manifest.name,
                                        manifest_url_func=manifest_url_func)

    def _service_controller(self, controller_cls: Type[C], **kwargs) -> C:
        return self._controller(controller_cls, file_url_func=self.file_url, **kwargs)

    @property
    def metadata_plugin(self) -> MetadataPlugin:
        return self._metadata_plugin(self.catalog)

    @cache
    def _metadata_plugin(self, catalog: CatalogName):
        return MetadataPlugin.load(catalog).create()

    @property
    def repository_plugin(self) -> RepositoryPlugin:
        return self._repository_plugin(self.catalog)

    @cache
    def _repository_plugin(self, catalog: CatalogName):
        return RepositoryPlugin.load(catalog).create(catalog)

    @property
    def fields(self) -> Sequence[str]:
        return sorted(self.metadata_plugin.field_mapping.keys())

    def __init__(self):
        super().__init__(app_name=config.service_name,
                         app_module_path=__file__,
                         # see LocalAppTestCase.setUpClass()
                         unit_test=globals().get('unit_test', False),
                         spec=spec)

    @attr.s(kw_only=True, auto_attribs=True, frozen=True)
    class Pagination(Pagination):
        self_url: furl

        def link(self, *, previous: bool, **params: str) -> Optional[furl]:
            search_key = self.search_before if previous else self.search_after
            if search_key is None:
                return None
            else:
                before_or_after = 'before' if previous else 'after'
                params = {
                    **params,
                    f'search_{before_or_after}': json.dumps(search_key),
                    'sort': self.sort,
                    'order': self.order,
                    'size': self.size
                }
            return furl(url=self.self_url, args=params)

    def get_pagination(self, entity_type: str) -> Pagination:
        default_sorting = self.metadata_plugin.exposed_indices[entity_type]
        params = self.current_request.query_params or {}
        sb, sa = params.get('search_before'), params.get('search_after')
        if sb is None:
            if sa is not None:
                sa = tuple(json.loads(sa))
        else:
            if sa is None:
                sb = tuple(json.loads(sb))
            else:
                raise BRE('Only one of search_after or search_before may be set')
        try:
            return self.Pagination(order=params.get('order', default_sorting.order),
                                   size=int(params.get('size', '10')),
                                   sort=params.get('sort', default_sorting.field_name),
                                   search_before=sb,
                                   search_after=sa,
                                   self_url=self.self_url)
        except RequirementError as e:
            raise ChaliceViewError(repr(e.args))

    def file_url(self,
                 *,
                 catalog: CatalogName,
                 file_uuid: str,
                 fetch: bool = True,
                 **params: str
                 ) -> mutable_furl:
        file_uuid = urllib.parse.quote(file_uuid, safe='')
        view_function = fetch_repository_files if fetch else repository_files
        path = one(view_function.path)
        url = self.base_url.add(path=path.format(file_uuid=file_uuid))
        return url.set(args=dict(catalog=catalog, **params))

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
                     *,
                     fetch: bool,
                     format_: ManifestFormat,
                     **params: str
                     ) -> mutable_furl:
        view_function = fetch_file_manifest if fetch else file_manifest
        path = one(view_function.path)
        url = self.base_url.add(path=path)
        return url.set(args=dict(format=format_.value, **params))


app = ServiceApp()
configure_app_logging(app, log)


@app.route(
    '/',
    cors=True
)
def swagger_ui():
    return app.swagger_ui()


@app.route(
    '/static/{file}',
    cors=True
)
def static_resource(file):
    return app.swagger_resource(file)


@app.route(
    '/oauth2_redirect',
    enabled=config.google_oauth2_client_id is not None
)
def oauth2_redirect():
    oauth2_redirect_html = app.load_static_resource('swagger', 'oauth2-redirect.html')
    return Response(status_code=200,
                    headers={"Content-Type": "text/html"},
                    body=oauth2_redirect_html)


common_specs = CommonEndpointSpecs(app_name='service')


@app.route(
    '/openapi',
    methods=['GET'],
    cors=True,
    **common_specs.openapi
)
def openapi():
    return Response(status_code=200,
                    headers={'content-type': 'application/json'},
                    body=app.spec())


@app.route(
    '/health',
    methods=['GET'],
    cors=True,
    **common_specs.full_health
)
def health():
    return app.health_controller.health()


@app.route(
    '/health/basic',
    methods=['GET'],
    cors=True,
    **common_specs.basic_health
)
def basic_health():
    return app.health_controller.basic_health()


@app.route(
    '/health/cached',
    methods=['GET'],
    cors=True,
    **common_specs.cached_health
)
def cached_health():
    return app.health_controller.cached_health()


@app.route(
    '/health/fast',
    methods=['GET'],
    cors=True,
    **common_specs.fast_health
)
def fast_health():
    return app.health_controller.fast_health()


@app.route(
    '/health/{keys}',
    methods=['GET'],
    cors=True,
    **common_specs.custom_health
)
def custom_health(keys: Optional[str] = None):
    return app.health_controller.custom_health(keys)


# FIXME: Remove redundant prefix from name
#        https://github.com/DataBiosphere/azul/issues/5337
@app.schedule(
    'rate(1 minute)',
    name='servicecachehealth'
)
def update_health_cache(_event: chalice.app.CloudWatchEvent):
    app.health_controller.update_cache()


@app.route(
    '/version',
    methods=['GET'],
    cors=True,
    **common_specs.version
)
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
        'order': validate_order,
        'search_after': partial(validate_json_param, 'search_after'),
        'search_after_uid': str,
        'search_before': partial(validate_json_param, 'search_before'),
        'search_before_uid': str,
        'size': validate_size,
        'sort': validate_field,
        **validators
    })


def validate_entity_type(entity_type: str):
    entity_types = app.metadata_plugin.exposed_indices.keys()
    if entity_type not in entity_types:
        raise BRE(f'Entity type {entity_type!r} is invalid for catalog '
                  f'{app.catalog!r}. Must be one of {set(entity_types)}.')


min_page_size = 1
max_page_size = 1000


def validate_catalog(catalog):
    try:
        config.Catalog.validate_name(catalog)
    except RequirementError as e:
        raise BRE(e)
    else:
        if catalog not in config.catalogs:
            raise NotFoundError(f'Catalog name {catalog!r} does not exist. '
                                f'Must be one of {set(config.catalogs)}.')


def validate_size(size):
    try:
        size = int(size)
    except BaseException:
        raise BRE('Invalid value for parameter `size`')
    else:
        if size > max_page_size:
            raise BRE(f'Invalid value for parameter `size`, must not be greater than {max_page_size}')
        elif size < min_page_size:
            raise BRE('Invalid value for parameter `size`, must be greater than 0')


def validate_filters(filters):
    filters = validate_json_param('filters', filters)
    if type(filters) is not dict:
        raise BRE('The `filters` parameter must be a dictionary')
    field_types = app.repository_controller.field_types(app.catalog)
    for field, filter_ in filters.items():
        validate_field(field)
        try:
            relation, values = one(filter_.items())
        except Exception:
            raise BRE(f'The `filters` parameter entry for `{field}` '
                      f'must be a single-item dictionary')
        else:
            if field == app.metadata_plugin.source_id_field:
                valid_relations = ('is',)
            else:
                valid_relations = ('is', 'contains', 'within', 'intersects')
            if relation in valid_relations:
                if not isinstance(values, list):
                    raise BRE(f'The value of the `{relation}` relation in the `filters` '
                              f'parameter entry for `{field}` is not a list')
            else:
                raise BRE(f'The relation in the `filters` parameter entry '
                          f'for `{field}` must be one of {valid_relations}')
            if relation == 'is':
                value_types = reify(Union[JSON, PrimitiveJSON])
                if not all(isinstance(value, value_types) for value in values):
                    raise BRE(f'The value of the `is` relation in the `filters` '
                              f'parameter entry for `{field}` is invalid')
            if field == 'organismAge':
                validate_organism_age_filter(values)
            field_type = field_types[field]
            if isinstance(field_type, Nested):
                if relation != 'is':
                    raise BRE(f'The field `{field}` can only be filtered by the `is` relation')
                try:
                    nested = one(values)
                except ValueError:
                    raise BRE(f'The value of the `is` relation in the `filters` '
                              f'parameter entry for `{field}` is not a single-item list')
                try:
                    require(isinstance(nested, dict))
                except RequirementError:
                    raise BRE(f'The value of the `is` relation in the `filters` '
                              f'parameter entry for `{field}` must contain a dictionary')
                extra_props = nested.keys() - field_type.properties.keys()
                if extra_props:
                    raise BRE(f'The value of the `is` relation in the `filters` '
                              f'parameter entry for `{field}` has invalid properties `{extra_props}`')


def validate_organism_age_filter(values):
    for value in values:
        try:
            value_and_unit.to_index(value)
        except RequirementError as e:
            raise BRE(e)


def validate_field(field: str):
    if field not in app.metadata_plugin.field_mapping:
        raise BRE(f'Unknown field `{field}`')


def validate_manifest_format(format_: str):
    supported_formats = {f.value for f in app.metadata_plugin.manifest_formats}
    try:
        ManifestFormat(format_)
    except ValueError:
        raise BRE(f'Unknown manifest format `{format_}`. '
                  f'Must be one of {supported_formats}')
    else:
        if format_ not in supported_formats:
            raise BRE(f'Manifest format `{format_}` is not supported for '
                      f'catalog {app.catalog}. Must be one of {supported_formats}')


def validate_order(order: str):
    supported_orders = ('asc', 'desc')
    if order not in supported_orders:
        raise BRE(f'Unknown order `{order}`. Must be one of {supported_orders}')


def validate_json_param(name: str, value: str) -> MutableJSON:
    try:
        return json.loads(value)
    except json.decoder.JSONDecodeError:
        raise BRE(f'The {name!r} parameter is not valid JSON')


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

        When False, only parameters specified via '**validators' are accepted,
        and validation fails if additional parameters are present. When True,
        additional parameters are allowed but their value is not validated.

    :param validators:

        A dictionary mapping the name of a parameter to a function that will be
        used to validate the parameter if it is provided. The callable will be
        called with a single argument, the parameter value to be validated, and
        is expected to raise ValueError, TypeError or azul.RequirementError if
        the value is invalid. Only these exceptions will yield a 4xx status
        response, all other exceptions will yield a 500 status response. If the
        validator is an instance of `Mandatory`, then validation will fail if
        its corresponding parameter is not provided.

    >>> validate_params({'order': 'asc'}, order=str)

    >>> validate_params({'size': 'foo'}, size=int)
    Traceback (most recent call last):
        ...
    chalice.app.BadRequestError: Invalid value for `size`

    >>> validate_params({'order': 'asc', 'foo': 'bar'}, order=str)
    Traceback (most recent call last):
        ...
    chalice.app.BadRequestError: Unknown query parameter `foo`

    >>> validate_params({'order': 'asc', 'foo': 'bar'}, order=str, allow_extra_params=True)

    >>> validate_params({}, foo=str)

    >>> validate_params({}, foo=Mandatory(str))
    Traceback (most recent call last):
        ...
    chalice.app.BadRequestError: Missing required query parameter `foo`

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
            raise BRE(fmt_error('Unknown', extra_params))

    if mandatory_params:
        missing_params = mandatory_params - provided_params
        if missing_params:
            raise BRE(fmt_error('Missing required', missing_params))

    for param_name, param_value in query_params.items():
        try:
            validator = validators[param_name]
        except KeyError:
            pass
        else:
            try:
                validator(param_value)
            except (TypeError, ValueError, RequirementError):
                raise BRE(f'Invalid value for `{param_name}`')


@app.route(
    '/integrations',
    methods=['GET'],
    cors=True
)
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
                    headers={'content-type': 'application/json'},
                    body=json.dumps(body))


@app.route(
    '/index/catalogs',
    methods=['GET'],
    cors=True,
    method_spec={
        'summary': 'List all available catalogs.',
        'tags': ['Index'],
        'responses': {
            '200': {
                'description': fd('''
                    The name of the default catalog and a list of all available
                    catalogs. For each catalog, the response includes the name
                    of the atlas the catalog belongs to, a flag indicating
                    whether the catalog is for internal use only as well as the
                    names and types of plugins currently active for the catalog.
                    For some plugins, the response includes additional
                    configuration properties, such as the sources used by the
                    repository plugin to populate the catalog or the set of
                    available [indices][1].

                    [1]: #operations-Index-get_index__entity_type_
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
    cellSuspensions=array_of_object_spec
)

page_spec = schema.object(
    hits=schema.array(hit_spec),
    pagination=generic_object_spec,
    termFacets=generic_object_spec
)


def _filter_schema(field_type: FieldType) -> JSON:
    relations = field_type.supported_filter_relations

    def filter_schema(relation: str) -> JSON:
        return schema.object_type(
            properties={relation: schema.array(field_type.api_filter_schema(relation))},
            required=[relation],
            additionalProperties=False
        )

    if len(relations) == 1:
        return filter_schema(one(relations))
    else:
        return {'oneOf': list(map(filter_schema, relations))}


types = app.repository_controller.field_types(app.catalog)

filters_param_spec = params.query(
    'filters',
    schema.optional(application_json(schema.object_type(
        default='{}',
        example={'cellCount': {'within': [[10000, 1000000000]]}},
        properties={
            field: _filter_schema(types[field])
            for field in app.fields
        }
    ))),
    description=fd('''
        Criteria to filter entities from the search results.

        Each filter consists of a field name, a relation (relational operator),
        and an array of field values. The available relations are "is",
        "within", "contains", and "intersects". Multiple filters are combined
        using "and" logic. An entity must match all filters to be included in
        the response. How multiple field values within a single filter are
        combined depends on the relation.

        For the "is" relation, multiple values are combined using "or" logic.
        For example, `{"fileFormat": {"is": ["fastq", "fastq.gz"]}}` selects
        entities where the file format is either "fastq" or "fastq.gz". For the
        "within", "intersects", and "contains" relations, the field values must
        come in nested pairs specifying upper and lower bounds, and multiple
        pairs are combined using "and" logic. For example, `{"donorCount":
        {"within": [[1,5], [5,10]]}}` selects entities whose donor organism
        count falls within both ranges, i.e., is exactly 5.

        The accessions field supports filtering for a specific accession and/or
        namespace within a project. For example, `{"accessions": {"is": [
        {"namespace":"array_express"}]}}` will filter for projects that have an
        `array_express` accession. Similarly, `{"accessions": {"is": [
        {"accession":"ERP112843"}]}}` will filter for projects that have the
        accession `ERP112843` while `{"accessions": {"is": [
        {"namespace":"array_express", "accession": "E-AAAA-00"}]}}` will filter
        for projects that match both values.

        The organismAge field is special in that it contains two property keys:
        value and unit. For example, `{"organismAge": {"is": [{"value": "20",
        "unit": "year"}]}}`. Both keys are required. `{"organismAge": {"is":
        [null]}}` selects entities that have no organism age.''' + f'''

        Supported field names are: {', '.join(app.fields)}
    ''')
)

catalog_param_spec = params.query(
    'catalog',
    schema.optional(schema.with_default(app.catalog,
                                        type_=schema.enum(*config.catalogs))),
    description='The name of the catalog to query.')


def repository_search_params_spec():
    return [
        catalog_param_spec,
        filters_param_spec,
        params.path(
            'entity_type',
            schema.enum(*app.metadata_plugin.exposed_indices.keys()),
            description='Which index to search.'
        ),
        params.query(
            'size',
            schema.optional(schema.with_default(10, type_=schema.in_range(min_page_size, max_page_size))),
            description='The number of hits included per page.'),
        params.query(
            'sort',
            schema.optional(schema.enum(*app.fields)),
            description='The field to sort the hits by. '
                        'The default value depends on the entity type.'
        ),
        params.query(
            'order',
            schema.optional(schema.enum('asc', 'desc')),
            description='The ordering of the sorted hits, either ascending '
                        'or descending. The default value depends on the entity '
                        'type.'
        ),
        *[
            params.query(
                param,
                schema.optional(str),
                description=fd('''
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


def repository_search_spec():
    id_spec_link = '#operations-Index-get_index__entity_type___entity_id_'
    return {
        'summary': 'Search an index for entities of interest.',
        'tags': ['Index'],
        'parameters': repository_search_params_spec(),
        'responses': {
            '200': {
                'description': fd(f'''
                    Paginated list of entities that meet the search criteria
                    ("hits"). The structure of these hits is documented under
                    the [corresponding endpoint for a specific
                    entity]({id_spec_link}).

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


def repository_id_spec():
    search_spec_link = '#operations-Index-get_index__entity_type_'
    return {
        'summary': 'Detailed information on a particular entity.',
        'tags': ['Index'],
        'parameters': [
            catalog_param_spec,
            params.path('entity_type', str, description='The type of the desired entity'),
            params.path('entity_id', str, description='The UUID of the desired entity')
        ],
        'responses': {
            '200': {
                'description': fd(f'''
                    This response describes a single entity. To search the index
                    for multiple entities, see the [corresponding search
                    endpoint]({search_spec_link}).

                    The properties that are common to all entity types are
                    listed in the schema below; however, additional properties
                    may be present for certain entity types. With the exception
                    of the entity's unique identifier, all properties are
                    arrays, even in cases where only one value is present.

                    The structures of the objects within these arrays are not
                    perfectly consistent, since they may represent either
                    singleton entities or aggregations depending on context.

                    For example, any biomaterial that yields a cell suspension
                    which yields a sequence file will be considered a "sample".
                    Therefore, the `samples` field is polymorphic, and each
                    sample may be either a specimen, an organoid, or a cell line
                    (the field `sampleEntityType` can be used to discriminate
                    between these cases).
                '''),
                **responses.json_content(hit_spec)
            }
        }
    }


def repository_head_spec(for_summary: bool = False):
    search_spec_link = f'#operations-Index-get_index_{"summary" if for_summary else "_entity_type_"}'
    return {
        'summary': 'Perform a query without returning its result.',
        'tags': ['Index'],
        'responses': {
            '200': {
                'description': fd(f'''
                    The HEAD method can be used to test whether an index is
                    operational, or to check the validity of query parameters
                    for the [GET method]({search_spec_link}).
                ''')
            }
        }
    }


def repository_head_search_spec():
    return {
        **repository_head_spec(),
        'parameters': repository_search_params_spec()
    }


repository_summary_spec = {
    'tags': ['Index'],
    'parameters': [catalog_param_spec, filters_param_spec]
}


@app.route(
    '/index/{entity_type}',
    methods=['GET'],
    method_spec=repository_search_spec(),
    cors=True
)
@app.route(
    '/index/{entity_type}',
    methods=['HEAD'],
    method_spec=repository_head_search_spec(),
    cors=True
)
@app.route(
    '/index/{entity_type}/{entity_id}',
    methods=['GET'],
    method_spec=repository_id_spec(),
    cors=True
)
def repository_search(entity_type: str, entity_id: Optional[str] = None) -> JSON:
    request = app.current_request
    query_params = request.query_params or {}
    validate_repository_search(query_params)
    validate_entity_type(entity_type)
    return app.repository_controller.search(catalog=app.catalog,
                                            entity_type=entity_type,
                                            item_id=entity_id,
                                            filters=query_params.get('filters'),
                                            pagination=app.get_pagination(entity_type),
                                            authentication=request.authentication)


@app.route(
    '/index/summary',
    methods=['GET'],
    cors=True,
    method_spec={
        'summary': 'Statistics on the data present across all entities.',
        'responses': {
            '200': {
                # FIXME: Add 'projects' to API documentation & schema
                #        https://github.com/DataBiosphere/azul/issues/3917
                'description': fd('''
                    Counts the total number and total size in bytes of assorted
                    entities, subject to the provided filters.

                    `fileTypeSummaries` provides the count and total size in
                    bytes of files grouped by their format, e.g. "fastq" or
                    "matrix." `fileCount` and `totalFileSize` compile these
                    figures across all file formats. Likewise,
                    `cellCountSummaries` counts cells and their associated
                    documents grouped by organ type, with `organTypes` listing
                    all referenced organs.

                    Total counts of unique entities are also provided for other
                    entity types such as projects and tissue donors. These
                    values are not grouped/aggregated.
                '''),
                **responses.json_content(
                    schema.object(
                        additional_properties=True,
                        organTypes=schema.array(str),
                        totalFileSize=float,
                        fileTypeSummaries=array_of_object_spec,
                        cellCountSummaries=array_of_object_spec,
                        donorCount=int,
                        fileCount=int,
                        labCount=int,
                        projectCount=int,
                        speciesCount=int,
                        specimenCount=int
                    )
                )
            }
        },
        **repository_summary_spec
    }
)
@app.route(
    '/index/summary',
    methods=['HEAD'],
    method_spec={
        **repository_head_spec(for_summary=True),
        **repository_summary_spec
    }
)
def get_summary():
    """
    Returns a summary based on the filters passed on to the call. Based on the
    ICGC endpoint.
    :return: Returns a jsonified Summary API response
    """
    request = app.current_request
    query_params = request.query_params or {}
    validate_params(query_params,
                    filters=str,
                    catalog=validate_catalog)
    filters = query_params.get('filters', '{}')
    validate_filters(filters)
    return app.repository_controller.summary(catalog=app.catalog,
                                             filters=filters,
                                             authentication=request.authentication)


token_param_spec = params.query('token',
                                schema.optional(str),
                                description='Reserved. Do not pass explicitly.')


def manifest_path_spec(*, fetch: bool):
    return {
        'parameters': [
            catalog_param_spec,
            filters_param_spec,
            params.query(
                'format',
                schema.optional(
                    schema.enum(
                        *[
                            format_.value
                            for format_ in app.metadata_plugin.manifest_formats
                        ],
                        type_=str
                    )
                ),
                description=f'''
                    The desired format of the output.

                        - `{ManifestFormat.compact.value}` (the default) for a
                          compact, tab-separated manifest

                        - `{ManifestFormat.terra_bdbag.value}` for a manifest in
                          the [BDBag format][1]. This provides a ZIP file
                          containing two manifests: one for Participants (aka
                          Donors) and one for Samples (aka Specimens). For more
                          on the format of the manifests see [documentation
                          here][2].

                        - `{ManifestFormat.terra_pfb.value}` for a manifest in
                          the [PFB format][3]. This format is mainly used for
                          exporting data to Terra.

                        - `{ManifestFormat.curl.value}` for a [curl
                          configuration file][4] manifest. This manifest can be
                          used with the curl program to download all the files
                          listed in the manifest.

                    [1]: https://bd2k.ini.usc.edu/tools/bdbag/

                    [2]: https://software.broadinstitute.org/firecloud/documentation/article?id=10954

                    [3]: https://github.com/uc-cdis/pypfb

                    [4]: https://curl.haxx.se/docs/manpage.html#-K
                '''
            ),
            *(
                [] if fetch else [
                    params.query('objectKey',
                                 schema.optional(str),
                                 description='Reserved. Do not pass explicitly.')
                ]
            ),
            token_param_spec
        ]
    }


@app.route(
    '/manifest/files',
    methods=['GET'],
    interactive=False,
    cors=True,
    path_spec=manifest_path_spec(fetch=False),
    method_spec={
        'tags': ['Manifests'],
        'summary': 'Request a download link to a manifest file and redirect',
        'description': fd('''
            Initiate and check status of a manifest generation job, returning
            either a 301 response redirecting to a URL to re-check the status of
            the manifest generation or a 302 response redirecting to the
            location of the completed manifest.

            This endpoint is not suitable for interactive use via the Swagger
            UI. Please use the [/fetch endpoint][1] instead.

            [1]: #operations-Manifests-get_fetch_manifest_files
            '''),
        'responses': {
            '301': {
                'description': fd('''
                    The manifest generation has been started or is ongoing. The
                    response is a redirect back to this endpoint, so the client
                    should expect a subsequent response of the same kind.
                '''),
                'headers': {
                    'Location': {
                        'description': fd('''
                            URL to recheck the status of the manifest
                            generation.
                        '''),
                        'schema': {'type': 'string', 'format': 'url'}
                    },
                    'Retry-After': {
                        'description': fd('''
                            Recommended number of seconds to wait before
                            requesting the URL specified in the Location header.
                        '''),
                        'schema': {'type': 'string'}
                    }
                }
            },
            '302': {
                'description': fd('''
                    The manifest generation is complete and ready for download.
                '''),
                'headers': {
                    'Location': {
                        'description': fd('''
                            URL that will yield the actual manifest file.
                        '''),
                        'schema': {'type': 'string', 'format': 'url'}
                    },
                    'Retry-After': {
                        'description': fd('''
                            Recommended number of seconds to wait before
                            requesting the URL specified in the `Location`
                            header.
                        '''),
                        'schema': {'type': 'string'}
                    }
                }
            },
            '410': {
                'description': fd('''
                    The manifest associated with the `objectKey` in this request
                    has expired. Request a new manifest.
                ''')
            }
        }
    }
)
def file_manifest():
    return _file_manifest(fetch=False)


keys = CurlManifestGenerator.command_lines(url=furl(''),
                                           file_name='',
                                           authentication=None).keys()
command_line_spec = schema.object(**{key: str for key in keys})


@app.route(
    '/fetch/manifest/files',
    methods=['GET'],
    cors=True,
    path_spec=manifest_path_spec(fetch=True),
    method_spec={
        'tags': ['Manifests'],
        'summary': 'Request a download link to a manifest file and check status',
        'description': fd('''
            Initiate a manifest generation or check the status of an already
            ongoing generation, returning a 200 response with simulated HTTP
            headers in the body.
        '''),
        'responses': {
            '200': {
                'description': fd('''
                    Manifest generation with status report, emulating the
                    response code and headers of the `/manifest/files` endpoint.
                    Note that the actual HTTP response will have status 200
                    while the `Status` field of the body will be 301 or 302. The
                    intent is to emulate HTTP while bypassing the default client
                    behavior, which (in most web browsers) is to ignore
                    `Retry-After`. The response described here is intended to be
                    processed by client-side Javascript such that the
                    recommended delay in `Retry-After` can be handled in
                    Javascript rather than relying on the native implementation
                    by the web browser.

                    For a detailed description of the fields in the response see
                    the documentation for the headers they emulate in the
                    [`/manifest/files`][1] endpoint response.

                    [1]: #operations-Manifests-get_manifest_files
                '''),
                **responses.json_content(
                    schema.object(
                        Status=int,
                        Location={'type': 'string', 'format': 'url'},
                        **{'Retry-After': schema.optional(int)},
                        CommandLine=command_line_spec
                    )
                ),
            }
        }
    }
)
def fetch_file_manifest():
    return _file_manifest(fetch=True)


def _file_manifest(fetch: bool):
    catalog = app.catalog
    request = app.current_request
    query_params = request.query_params or {}
    query_params.setdefault('filters', '{}')
    # FIXME: Remove `object_key` when Swagger validation lands
    #        https://github.com/DataBiosphere/azul/issues/1465
    # The objectKey query parameter is not allowed in /fetch/manifest/files
    object_key = {} if fetch else {'objectKey': str}
    validate_params(query_params,
                    format=validate_manifest_format,
                    catalog=validate_catalog,
                    filters=str,
                    token=str,
                    **object_key)
    # Wait to load metadata plugin until we've validated the catalog
    default_format = app.metadata_plugin.manifest_formats[0].value
    query_params.setdefault('format', default_format)
    validate_filters(query_params['filters'])
    return app.manifest_controller.get_manifest_async(self_url=app.self_url,
                                                      catalog=catalog,
                                                      query_params=query_params,
                                                      fetch=fetch,
                                                      authentication=request.authentication)


@app.lambda_function(
    name='manifest'
)
def generate_manifest(event: AnyJSON, _context: LambdaContext):
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
        description=fd('''
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
            description=fd('''
                The desired name of the file. The given value will be included
                in the Content-Disposition header of the response. If absent, a
                best effort to determine the file name from metadata will be
                made. If that fails, the UUID of the file will be used instead.
            ''')
        ),
        params.query(
            'wait',
            schema.optional(int),
            description=fd('''
                If 0, the client is responsible for honoring the waiting period
                specified in the Retry-After response header. If 1, the server
                will delay the response in order to consume as much of that
                waiting period as possible. This parameter should only be set to
                1 by clients who can't honor the `Retry-After` header,
                preventing them from quickly exhausting the maximum number of
                redirects. If the server cannot wait the full amount, any amount
                of wait time left will still be returned in the Retry-After
                header of the response.
            ''')
        ),
        params.query(
            'replica',
            schema.optional(str),
            description=fd('''
                If the underlying repository offers multiple replicas of the
                requested file, use the specified replica. Otherwise, this
                parameter is ignored. If absent, the only replica — for
                repositories that don't support replication — or the default
                replica — for those that do — will be used.
            ''')
        ),
        params.query(
            'requestIndex',
            schema.optional(int),
            description='Do not use. Reserved for internal purposes.'
        ),
        params.query(
            'drsUri',
            schema.optional(str),
            description='Do not use. Reserved for internal purposes.'
        ),
        token_param_spec
    ]
}


@app.route(
    '/repository/files/{file_uuid}',
    methods=['GET'],
    interactive=False,
    cors=True,
    method_spec={
        **repository_files_spec,
        'summary': 'Redirect to a URL for downloading a given data file from the '
                   'underlying repository',
        'description': fd('''
            This endpoint is not suitable for interactive use via the Swagger
            UI. Please use the [/fetch endpoint][1] instead.

            [1]: #operations-Repository-get_fetch_repository_files__file_uuid_
        '''),
        'responses': {
            '301': {
                'description': fd('''
                    A URL to the given file is still being prepared. Retry by
                    waiting the number of seconds specified in the `Retry-After`
                    header of the response and the requesting the URL specified
                    in the `Location` header.
                '''),
                'headers': {
                    'Location': responses.header(str, description=fd('''
                        A URL pointing back at this endpoint, potentially with
                        different or additional request parameters.
                    ''')),
                    'Retry-After': responses.header(int, description=fd('''
                        Recommended number of seconds to wait before requesting
                        the URL specified in the `Location` header. The response
                        may carry this header even if server-side waiting was
                        requested via `wait=1`.
                    '''))
                }
            },
            '302': {
                'description': fd('''
                    The file can be downloaded from the URL returned in the
                    `Location` header.
                '''),
                'headers': {
                    'Location': responses.header(str, description=fd('''
                            A URL that will yield the actual content of the file.
                    ''')),
                    'Content-Disposition': responses.header(str, description=fd('''
                        Set to a value that makes user agents download the file
                        instead of rendering it, suggesting a meaningful name
                        for the downloaded file stored on the user's file
                        system. The suggested file name is taken  from the
                        `fileName` request parameter or, if absent, from
                        metadata describing the file. It generally does not
                        correlate with the path component of the URL returned in
                        the `Location` header.
                    '''))
                }
            }
        }
    }
)
def repository_files(file_uuid: str) -> Response:
    result = _repository_files(file_uuid, fetch=False)
    status_code = result.pop('Status')
    return Response(body='',
                    headers={k: str(v) for k, v in result.items()},
                    status_code=status_code)


@app.route(
    '/fetch/repository/files/{file_uuid}',
    methods=['GET'],
    cors=True,
    method_spec={
        **repository_files_spec,
        'summary': 'Request a URL for downloading a given data file',
        'responses': {
            '200': {
                'description': fd(f'''
                    Emulates the response code and headers of
                    {one(repository_files.path)} while bypassing the default
                    user agent behavior. Note that the status code of a
                    successful response will be 200 while the `Status` field of
                    its body will be 302.

                    The response described here is intended to be processed by
                    client-side Javascript such that the emulated headers can be
                    handled in Javascript rather than relying on the native
                    implementation by the web browser.
                '''),
                **responses.json_content(
                    schema.object(
                        Status=int,
                        Location=str
                    )
                )
            }
        }
    }
)
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

    def validate_version(version: str) -> None:
        # This function exists so the repository plugin can be lazily loaded
        # instead of being loaded before `validate_params()` can run. This is
        # desired since `validate_params()` validates the params in the order
        # given, and we want the catalog to be validated before the repository
        # plugin is loaded, which is an action that requires a valid catalog.
        app.repository_plugin.validate_version(version)

    validate_params(query_params,
                    catalog=validate_catalog,
                    version=validate_version,
                    fileName=str,
                    wait=validate_wait,
                    requestIndex=int,
                    replica=validate_replica,
                    drsUri=str,
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


@app.route(
    '/repository/sources',
    methods=['GET'],
    cors=True,
    method_spec={
        'summary': 'List available data sources',
        'tags': ['Repository'],
        'parameters': [catalog_param_spec],
        'responses': {
            '200': {
                'description': fd('''
                    List the sources the currently authenticated user is
                    authorized to access in the underlying data repository.
                '''),
                **responses.json_content(
                    schema.object(sources=schema.array(
                        schema.object(
                            sourceId=str,
                            sourceSpec=str
                        )
                    ))
                )
            }
        }
    }
)
def list_sources() -> Response:
    validate_params(app.current_request.query_params or {},
                    catalog=validate_catalog)
    sources = app.repository_controller.list_sources(app.catalog,
                                                     app.current_request.authentication)
    return Response(body={'sources': sources}, status_code=200)


def hash_url(url):
    url_hash = hashlib.sha1(bytes(url, encoding='utf-8')).digest()
    return base64.urlsafe_b64encode(url_hash).decode()


drs_spec_description = fd('''
    This is a partial implementation of the [DRS 1.0.0 spec][1]. Not all
    features are implemented. This endpoint acts as a DRS-compliant proxy for
    accessing files in the underlying repository.

    [1]: https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/

    Any errors encountered from the underlying repository are forwarded on as
    errors from this endpoint.
''')


@app.route(
    drs.drs_object_url_path(object_id='{file_uuid}'),
    methods=['GET'],
    enabled=config.is_dss_enabled(),
    cors=True,
    method_spec={
        'summary': 'Get file DRS object',
        'tags': ['DRS'],
        'description': fd('''
            This endpoint returns object metadata, and a list of access methods
            that can be used to fetch object bytes.
        ''') + drs_spec_description,
        'parameters': file_fqid_parameters_spec,
        'responses': {
            '200': {
                'description': fd(
                    '''
                    A DRS object is returned. Two [`AccessMethod`s][1] are
                    included:

                    [1]: {link}

                    {access_methods}

                    If the object is not immediately ready, an `access_id` will
                    be returned instead of an `access_url`.
                    ''',
                    access_methods='\n'.join(f'- {am!s}' for am in AccessMethod),
                    link='https://ga4gh.github.io/data-repository-service-schemas'
                         '/preview/release/drs-1.1.0/docs/#_accessmethod'),
                **app.drs_controller.get_object_response_schema()
            }
        }
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
    drs.drs_object_url_path(object_id='{file_uuid}', access_id='{access_id}'),
    methods=['GET'],
    enabled=config.is_dss_enabled(),
    cors=True,
    method_spec={
        'summary': 'Get a file with an access ID',
        'description': fd('''
            This endpoint returns a URL that can be used to fetch the bytes of a
            DRS object.

            This method only needs to be called when using an `AccessMethod`
            that contains an `access_id`.

            An `access_id` is returned when the underlying file is not ready.
            When the underlying repository is the DSS, the 202 response allowed
            time for the DSS to do a checkout.
        ''') + drs_spec_description,
        'parameters': [
            *file_fqid_parameters_spec,
            params.path('access_id', str, description='Access ID returned from a previous request')
        ],
        'responses': {
            '202': {
                'description': fd('''
                    This response is issued if the object is not yet ready.
                    Respect the `Retry-After` header, then try again.
                '''),
                'headers': {
                    'Retry-After': responses.header(str, description=fd('''
                        Recommended number of seconds to wait before requesting
                        the URL specified in the Location header.
                    '''))
                }
            },
            '200': {
                'description': fd('''
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
    request = app.current_request
    query_params = request.query_params or {}
    validate_params(query_params,
                    version=str,
                    catalog=validate_catalog)
    catalog = app.catalog
    file_version = query_params.get('version')
    return app.drs_controller.dos_get_object(catalog,
                                             file_uuid,
                                             file_version,
                                             request.authentication)
