from typing import (
    Dict,
    List,
    cast,
)

import attr
from furl import (
    furl,
)

from azul import (
    JSON,
    cache,
    config,
)
from azul.indexer import (
    Prefix,
    SourceSpec,
)
from azul.terra import (
    SAMClient,
)
from azul.types import (
    MutableJSONs,
)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class TerraSourceSpec(SourceSpec):
    name: str
    namespace: str

    @property
    def qualname(self):
        return f'{self.namespace}.{self.name}'

    _service_name = 'terra'

    @classmethod
    def parse(cls, spec: str) -> 'TerraSourceSpec':
        """
        Construct an instance from its string representation, using the syntax
        'terra:{namespace}:{name}:{prefix}' ending with an optional
        '/{partition_prefix_length}'.

        >>> s = TerraSourceSpec.parse('terra:foo:bar:')
        >>> s # doctest: +NORMALIZE_WHITESPACE
        TerraSourceSpec(prefix=Prefix(common='', partition=None),
                        name='bar',
                        namespace='foo')

        >>> str(s)
        'terra:foo:bar:'

        >>> s = TerraSourceSpec.parse('terra:with:prefix:42/2')
        >>> s # doctest: +NORMALIZE_WHITESPACE
        TerraSourceSpec(prefix=Prefix(common='42', partition=2),
                        name='prefix',
                        namespace='with')

        >>> TerraSourceSpec.parse('foo:bar:baz:')
        Traceback (most recent call last):
        ...
        AssertionError: foo

        >>> TerraSourceSpec.parse('terra:foo:bar:x')
        Traceback (most recent call last):
        ...
        azul.uuids.InvalidUUIDPrefixError: 'x' is not a valid UUID prefix.
        """
        service, namespace, name, prefix = spec.split(':')
        assert service == cls._service_name, service
        self = cls(prefix=Prefix.parse(prefix),
                   namespace=namespace,
                   name=name)
        assert spec == str(self), (spec, str(self), self)
        return self

    def __str__(self) -> str:
        """
        The inverse of :meth:`parse`.
        """
        return ':'.join([
            self._service_name,
            self.namespace,
            self.name,
            str(self.prefix)
        ])

    def contains(self, other: SourceSpec) -> bool:
        return (
            isinstance(other, TerraSourceSpec)
            and self.namespace == other.namespace
            and self.name == other.name
            and super().contains(other)
        )


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class Workspace:
    id: str
    namespace: str
    name: str
    version: str
    created_date: str
    update_date: str
    attributes: JSON

    fields = {
        'id': 'workspaceId',
        'version': 'workspaceVersion',
        'name': 'name',
        'namespace': 'namespace',
        'created_date': 'createdDate',
        'update_date': 'lastModified',
        'attributes': 'attributes'
    }

    @property
    def tags(self) -> JSON:
        return self.attributes.get('tag:tags', {})

    @property
    def qualname(self):
        return f'{self.namespace}.{self.name}'

    @classmethod
    def from_response(cls, workspace: JSON) -> 'Workspace':
        return cls(**{
            cls_field: workspace[workspace_field]
            for cls_field, workspace_field in cls.fields.items()
        })


class WorkspaceClient(SAMClient):

    @property
    def _workspace_fields(self) -> Dict[str, str]:
        return {
            'fields': ','.join(
                f'workspace.{field}'
                for field in Workspace.fields.values()
            )
        }

    def _workspace_endpoint(self, *path: str) -> str:
        return str(furl(config.terra_workspace_url, path=('api', 'workspaces', *path)))

    @cache
    def get_workspace(self, source: TerraSourceSpec) -> Workspace:
        endpoint = self._workspace_endpoint(source.namespace, source.name)
        response = self._request('GET', endpoint, fields=self._workspace_fields)
        workspace = self._check_response(endpoint, response)
        return Workspace.from_response(workspace['workspace'])

    def list_workspaces(self) -> List[Workspace]:
        endpoint = self._workspace_endpoint()
        response = self._request('GET', endpoint, fields=self._workspace_fields)
        workspaces = cast(MutableJSONs, self._check_response(endpoint, response))
        return [
            Workspace.from_response(item['workspace'])
            for item in workspaces
        ]
