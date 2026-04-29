from abc import (
    ABCMeta,
    abstractmethod,
)
from collections.abc import (
    Sequence,
)
import json
import logging
from typing import (
    Mapping,
)

from chalice.app import (
    BadRequestError as BRE,
    MultiDict,
    Request,
)
import jsonschema
import jsonschema.protocols
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
)
from azul.field_type import (
    FieldType,
    Mode,
    pass_thru_bool,
)
from azul.lib import (
    cache,
)
from azul.lib.collections import (
    OrderedSet,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    JSON,
    MutableJSON,
)
from azul.openapi import (
    application_json,
    params,
    schema,
)
from azul.plugins import (
    MetadataPlugin,
)
from azul.service.controller import (
    ServiceController,
)
from azul.service.query_service import (
    QueryService,
)

log = logging.getLogger(__name__)


class QueryController(ServiceController, metaclass=ABCMeta):

    @property
    @abstractmethod
    def _service(self) -> QueryService:
        raise NotImplementedError

    @property
    def _metadata_plugin(self) -> MetadataPlugin:
        return self._service.metadata_plugin(self.app.catalog)

    @property
    def _fields(self) -> Sequence[str]:
        organic, synthetic = self._organic_fields, self._synthetic_fields
        all = OrderedSet(organic)
        all.update(synthetic)
        assert len(all) == len(organic) + len(synthetic)
        return tuple(all)

    @property
    def _organic_fields(self) -> Sequence[str]:
        return sorted(self._metadata_plugin.field_mapping.keys())

    @property
    def _synthetic_fields(self) -> Sequence[str]:
        return self._metadata_plugin.special_fields.accessible.name,

    def _hoist_parameters(self, request: Request) -> MultiDict:
        query_params = self._query_params(request)
        if request.method in ('POST', 'PUT'):
            body = request.json_body
            if body is not None:
                if not isinstance(body, dict):
                    raise BRE('Request body is not a JSON object')
                elif body.keys() & query_params.keys():
                    raise BRE('Conflicting keys between body and query parameters')
                else:
                    query_params.update(body)
        return query_params

    def _parameter_hoisting_note(self,
                                 method: str,
                                 endpoint: str,
                                 equivalent_method: str
                                 ) -> str:
        return fd('''
            Any of the query parameters documented below can alternatively be passed
            as a property of a JSON object in the body of the request. This can be
            useful in case the value of the `filters` query parameter causes the URL
            to exceed the maximum length of 8192 characters, resulting in a 413
            Request Entity Too Large response.

            The request `%s %s?filters={…}`, for example, is equivalent to  `%s %s`
            with the body `{"filters": "{…}"}` in which any double quotes or
            backslash characters inside `…` are escaped with another backslash. That
            escaping is the requisite procedure for embedding one JSON structure
            inside another.
        ''' % (method, endpoint, equivalent_method, endpoint))

    @property
    def _filters_param_spec(self):
        filter_schema = self._filter_schema(self.app.catalog, Mode.openapi)
        return params.query(
            'filters',
            schema.optional(application_json(filter_schema)),
            description=fd('''
                Criteria to filter entities from the search results.

                Each filter consists of a field name, an operator, and an array of field
                values. The available operators are "is", "within", "contains", and
                "intersects". Multiple filters are combined using "and" logic. For an
                entity to be included in the response, it must match all filters. How
                multiple field values within a single filter are combined depends on the
                operator.

                For the "is" operator, multiple values are combined using "or" logic.
                For example, `{"fileFormat": {"is": ["fastq", "fastq.gz"]}}` selects
                entities where the file format is either "fastq" or "fastq.gz". For the
                "within", "intersects", and "contains" operators, the field values must
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

                Supported field names are: {', '.join(self._fields)}
            ''')
        )

    @cache
    def _filter_schema(self, catalog: CatalogName, mode: Mode) -> JSON:
        types = self._field_types(catalog)

        def _filter_schema(field_type):
            operators = field_type.supported_filter_operators

            def filter_schema(operator):
                return schema.object(
                    properties={
                        operator: field_type.api_filter_values_schema(operator, mode)
                    },
                    required=[operator],
                    additionalProperties=False
                )

            if len(operators) == 1:
                return filter_schema(one(operators))
            else:
                return {'oneOf': list(map(filter_schema, operators))}

        filter_schema = schema.object(default='{}',
                                      example={'cellCount': {'within': [[10000, 1000000000]]}},
                                      additionalProperties=False,
                                      properties={
                                          field: _filter_schema(types[field])
                                          for field in self._fields
                                      })
        return filter_schema

    def _validate_json_param(self, name: str, value: str) -> MutableJSON:
        try:
            return json.loads(value)
        except json.decoder.JSONDecodeError:
            raise BRE(f'The {name!r} parameter is not valid JSON')

    def _validate_field(self, field: str, *, include_synthetic: bool = False):
        fields = self._fields if include_synthetic else self._organic_fields
        if field not in fields:
            raise BRE(f'Unknown field `{field}`')

    def _validate_filters(self, filters):
        filters = self._validate_json_param('filters', filters)

        validator = self._filter_schema_validator(self.app.catalog)
        try:
            validator.validate(filters)
        except jsonschema.exceptions.ValidationError as e:
            raise BRE(f'The value of the `filters` parameter is '
                      f'invalid against the schema: {e.message} '
                      f'at path {e.json_path}')

    @cache
    def _filter_schema_validator(self,
                                 catalog: CatalogName
                                 ) -> jsonschema.protocols.Validator:
        schema = self._filter_schema(catalog, Mode.jsonschema)
        return jsonschema.validators.validator_for(schema)(schema)

    @cache
    def _field_types(self, catalog: CatalogName) -> Mapping[str, FieldType]:
        """
        Returns the field type for each supported sort and filter field, using
        the name of the field as provided by clients.
        """
        result = {}
        plugin = self._metadata_plugin
        for field, path in plugin.field_mapping.items():
            field_type = self._service.field_type(catalog, path)
            if isinstance(field_type, FieldType):
                result[field] = field_type
        # This field is a synthetic element of the response and will never be
        # null. Including it here helps to streamline request validation.
        accessible_field = plugin.special_fields.accessible.name
        assert accessible_field not in result, result
        result[accessible_field] = pass_thru_bool
        return result
