from typing import (
    Any,
    Callable,
    Mapping,
)

from chalice.app import (
    BadRequestError as BRE,
    NotFoundError,
)

from azul import (
    CatalogName,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.filters import (
    Filters,
    FiltersJSON,
    normalize_filters,
    parse_filters,
    validate_filters,
)
from azul.lib import (
    R,
    mutable_furl,
)
from azul.lib.strings import (
    format_and_dedent as fd,
    pluralize,
)
from azul.openapi import (
    params,
    schema,
)
from azul.service.source_controller import (
    SourceController,
)


class ServiceController(SourceController):
    _file_fqid_parameters_spec = [
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

    @property
    def _catalog_param_spec(self):
        return params.query(
            'catalog',
            schema.optional(schema.default(self.app.catalog,
                                           form=schema.enum(*config.catalogs))),
            description='The name of the catalog to query.')

    def _file_url(self,
                  *,
                  catalog: CatalogName,
                  file_uuid: str,
                  fetch: bool = True,
                  **params: str
                  ) -> mutable_furl:
        path = self._file_path(fetch=fetch, file_uuid=file_uuid)
        url = self.app.base_url.add(path=path)
        return url.set(args=dict(catalog=catalog, **params))

    def _file_path(self, *, fetch: bool, file_uuid: str) -> tuple[str, ...]:
        path: tuple[str, ...] = ('repository', 'files', file_uuid)
        if fetch:
            path = ('fetch', *path)
        return path

    def _prepare_filters(self,
                         catalog: CatalogName,
                         authentication: Authentication | None,
                         filters: str | None = None
                         ) -> Filters:
        return Filters(explicit=self._parse_filters(filters),
                       source_ids=self._list_source_ids(catalog, authentication))

    def _parse_filters(self, filters: str | None) -> FiltersJSON:
        try:
            return normalize_filters(validate_filters(parse_filters(filters)))
        except AssertionError as e:
            if R.caused(e):
                raise R.propagate(e, BRE)
            else:
                raise

    def _validate_catalog(self, catalog: CatalogName):
        try:
            config.Catalog.validate_name(catalog)
        except AssertionError as e:
            if R.caused(e):
                raise R.propagate(e, BRE)
            else:
                raise
        else:
            if catalog not in config.catalogs:
                raise NotFoundError(f'Catalog name {catalog!r} does not exist. '
                                    f'Must be one of {set(config.catalogs)}.')


type Validator = Callable[[Any], Any]


class Mandatory:
    """
    Validation wrapper signifying that a parameter is mandatory.
    """

    def __init__(self, validator: Validator) -> None:
        super().__init__()
        self._validator = validator

    def __call__(self, param):
        return self._validator(param)


def validate_params(query_params: Mapping[str, str],
                    allow_extra_params: bool = False,
                    **validators: Validator) -> None:
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
        is expected to raise ValueError, TypeError or AssertionError if
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
    mandatory_params = {
        param_name
        for param_name, validator in validators.items()
        if isinstance(validator, Mandatory)
    }

    if not allow_extra_params:
        extra_params = provided_params - validation_params
        if extra_params:
            raise BRE(fmt_error('Unknown', extra_params))

    if mandatory_params:
        missing_params = mandatory_params - provided_params
        if missing_params:
            raise BRE(fmt_error('Missing required', missing_params))

    for param_name, validator in validators.items():
        try:
            param_value = query_params[param_name]
        except KeyError:
            pass
        else:
            try:
                validator(param_value)
            except (TypeError, ValueError, AssertionError):
                raise BRE(f'Invalid value for `{param_name}`')
