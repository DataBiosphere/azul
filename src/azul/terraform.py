from itertools import (
    chain,
)
import json
from pathlib import (
    Path,
)
import subprocess
from typing import (
    Dict,
    Iterable,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import attr

from azul import (
    cached_property,
    config,
    require,
)
from azul.template import (
    emit,
)
from azul.types import (
    AnyJSON,
    JSON,
    JSONs,
)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class TerraformSchema:
    versions: Sequence[str]
    document: JSON
    path: Path

    @classmethod
    def load(cls, path: Path):
        with path.open() as f:
            doc = json.load(f)
        return cls(versions=doc['versions'],
                   document=doc['schema'],
                   path=path)

    def store(self):
        with self.path.open('w') as f:
            json.dump(dict(versions=self.versions,
                           schema=self.document), f, indent=4)


class Terraform:

    def taggable_resource_types(self) -> Sequence[str]:
        schema = self.schema.document
        require(schema['format_version'] == '0.1')
        resources = chain.from_iterable(
            schema['provider_schemas'][provider]['resource_schemas'].items()
            for provider in schema['provider_schemas']
        )
        return [
            resource_type
            for resource_type, resource in resources
            if 'tags' in resource['block']['attributes']
        ]

    def run(self, *args: str) -> str:
        terraform_dir = Path(config.project_root) / 'terraform'
        cmd = subprocess.run(['terraform', *args],
                             cwd=terraform_dir,
                             check=True,
                             stdout=subprocess.PIPE,
                             text=True,
                             shell=False)
        return cmd.stdout

    schema_path = Path(config.project_root) / 'terraform' / '_schema.json'

    @cached_property
    def schema(self):
        return TerraformSchema.load(self.schema_path)

    def update_schema(self):
        schema = self.run('providers', 'schema', '-json')
        schema = TerraformSchema(versions=self.versions,
                                 document=json.loads(schema),
                                 path=self.schema_path)
        schema.store()
        # Reset the cache
        try:
            # noinspection PyPropertyAccess
            del self.schema
        except AttributeError:
            pass

    @cached_property
    def versions(self) -> Sequence[str]:
        # `terraform -version` prints a warning if you are not running the latest
        # release of Terraform; we discard it, otherwise, we would need to update
        # the tracked schema every time a new version of Terraform is released
        versions, footer = self.run('-version').split('\n\n')
        return sorted(versions.splitlines())


terraform = Terraform()
del Terraform


def _sanitize_tf(tf_config: JSON) -> JSON:
    """
    Avoid errors like

        Error: Missing block label

          on api_gateway.tf.json line 12:
          12:     "resource": []

        At least one object property is required, whose name represents the resource
        block's type.
    """
    return {k: v for k, v in tf_config.items() if v}


def _normalize_tf(tf_config: Union[JSON, JSONs]) -> Iterable[Tuple[str, AnyJSON]]:
    """
    Certain levels of a Terraform JSON structure can either be a single
    dictionary or a list of dictionaries. For example, these are equivalent:

        {"resource": {"resource_type": {"resource_id": {"foo": ...}}}}
        {"resource": [{"resource_type": {"resource_id": {"foo": ...}}}]}

    So are these:

        {"resource": {"type": {"id": {"foo": ...}, "id2": {"bar": ...}}}}
        {"resource": {"type": [{"id": {"foo": ...}}, {"id2": {"bar": ...}}]}}

    This function normalizes input to prefer the second form of both cases to
    make parsing Terraform configuration simpler. It returns an iterator of the
    dictionary entries in the argument, regardless which form is used.

    >>> list(_normalize_tf({'foo': 'bar'}))
    [('foo', 'bar')]

    >>> list(_normalize_tf([{'foo': 'bar'}]))
    [('foo', 'bar')]

    >>> list(_normalize_tf({"foo": "bar", "baz": "qux"}))
    [('foo', 'bar'), ('baz', 'qux')]

    >>> list(_normalize_tf([{"foo": "bar"}, {"baz": "qux"}]))
    [('foo', 'bar'), ('baz', 'qux')]

    >>> list(_normalize_tf([{"foo": "bar", "baz": "qux"}]))
    [('foo', 'bar'), ('baz', 'qux')]
    """
    if isinstance(tf_config, dict):
        return tf_config.items()
    elif isinstance(tf_config, list):
        return chain.from_iterable(d.items() for d in tf_config)
    else:
        assert False, type(tf_config)


def populate_tags(tf_config: JSON) -> JSON:
    """
    Add tags to all taggable resources and change the `name` tag to `Name`
    for tagged AWS resources.
    """
    taggable_resource_types = terraform.taggable_resource_types()
    try:
        resources = tf_config['resource']
    except KeyError:
        return tf_config
    else:
        return {
            k: v if k != 'resource' else [
                {
                    resource_type: [
                        {
                            resource_name: {
                                **arguments,
                                'tags': _adjust_name_tag(resource_type,
                                                         _tags(resource_name, **arguments.get('tags', {})))
                            } if resource_type in taggable_resource_types else arguments
                        }
                        for resource_name, arguments in _normalize_tf(resource)
                    ]
                }
                for resource_type, resource in _normalize_tf(resources)
            ]
            for k, v in tf_config.items()
        }


def emit_tf(tf_config: Optional[JSON]):
    if tf_config is None:
        return emit(tf_config)
    else:
        return emit(_sanitize_tf(populate_tags(tf_config)))


def _tags(resource_name: str, **overrides: str) -> Dict[str, str]:
    """
    Return tags named for cloud resources based on :class:`azul.Config`.

    :param resource_name: The Terraform name of the resource.

    :param overrides: Additional tags that override the defaults.

    >>> from azul.doctests import assert_json
    >>> assert_json(_tags('service'))  #doctest: +ELLIPSIS
    {
        "project": "dcp",
        "service": "azul",
        "deployment": "...",
        "owner": ...,
        "name": "azul-service-...",
        "component": "azul-service"
    }

    >>> from azul.doctests import assert_json
    >>> assert_json(_tags('service', project='foo'))  #doctest: +ELLIPSIS
    {
        "project": "foo",
        "service": "azul",
        "deployment": "...",
        "owner": ...,
        "name": "azul-service-...",
        "component": "azul-service"
    }
    """
    return {
        'project': 'dcp',
        'service': config.resource_prefix,
        'deployment': config.deployment_stage,
        'owner': config.owner,
        'name': config.qualified_resource_name(resource_name),
        'component': f'{config.resource_prefix}-{resource_name}',
        **overrides
    }


def _adjust_name_tag(resource_type: str, tags: Dict[str, str]) -> Dict[str, str]:
    return {
        'Name' if k == 'name' and resource_type.startswith('aws_') else k: v
        for k, v in tags.items()
    }
