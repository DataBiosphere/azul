from itertools import (
    chain,
)
import json
import logging
from pathlib import (
    Path,
)
import subprocess
from typing import (
    Iterable,
    Optional,
    Sequence,
    TypeVar,
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

log = logging.getLogger(__name__)


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
        args = ['terraform', *args]
        log.info('Running %r', args)
        cmd = subprocess.run(args,
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
        output = self.run('-version')
        log.info('Terraform output:\n%s', output)
        versions, footer = output.split('\n\n')
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


def _normalize_tf(tf_config: Union[JSON, JSONs]) -> Iterable[tuple[str, AnyJSON]]:
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


def _tags(resource_name: str, **overrides: str) -> dict[str, str]:
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


def _adjust_name_tag(resource_type: str, tags: dict[str, str]) -> dict[str, str]:
    return {
        'Name' if k == 'name' and resource_type.startswith('aws_') else k: v
        for k, v in tags.items()
    }


U = TypeVar('U', bound=AnyJSON)


class Chalice:

    def package_dir(self, lambda_name):
        return Path(config.project_root) / 'lambdas' / lambda_name / '.chalice' / 'terraform'

    def module_dir(self, lambda_name):
        return Path(config.project_root) / 'terraform' / lambda_name

    package_zip_name = 'deployment.zip'

    tf_config_name = 'chalice.tf.json'

    resource_name_suffix = '-event'

    def resource_name_mapping(self, tf_config: JSON) -> dict[tuple[str, str], str]:
        """
        Some Chalice-generated resources have names that are incompatible with
        our convention for generating fully qualified resource names. This
        method returns a dictionary that, for each affected resource in the
        given configuration, maps the resource's type and current name to a name
        that's compatible with the convention.
        """
        mapping = {}
        for resource_type, resources in tf_config['resource'].items():
            for name in resources:
                if name.endswith(self.resource_name_suffix):
                    new_name = name[:-len(self.resource_name_suffix)]
                    mapping[resource_type, name] = new_name
        return mapping

    def patch_resource_names(self, tf_config: JSON) -> JSON:
        """
        Some Chalice-generated resources have names that are incompatible with
        our convention for generating fully qualified resource names. This
        method transforms the given Terraform configuration to use names that
        are compatible with the convention.

        >>> from azul.doctests import assert_json
        >>> assert_json(chalice.patch_resource_names({
        ...     "resource": {
        ...         "aws_cloudwatch_event_rule": {
        ...             "indexercachehealth-event": {  # patch
        ...                 "name": "indexercachehealth-event"  # leave
        ...             }
        ...         },
        ...         "aws_cloudwatch_event_target": {
        ...             "indexercachehealth-event": {  # patch
        ...                 "rule": "${aws_cloudwatch_event_rule.indexercachehealth-event.name}",  # patch
        ...                 "target_id": "indexercachehealth-event",  # leave
        ...                 "arn": "${aws_lambda_function.indexercachehealth.arn}"
        ...             }
        ...         },
        ...         "aws_lambda_permission": {
        ...             "indexercachehealth-event": {  # patch
        ...                 "function_name": "azul-indexer-prod-indexercachehealth",
        ...                 "source_arn": "${aws_cloudwatch_event_rule.indexercachehealth-event.arn}"  # patch
        ...             }
        ...         },
        ...         "aws_lambda_event_source_mapping": {
        ...             "contribute-sqs-event-source": {
        ...                 "batch_size": 1
        ...             }
        ...         }
        ...     }
        ... }))
        {
            "resource": {
                "aws_cloudwatch_event_rule": {
                    "indexercachehealth": {
                        "name": "indexercachehealth-event"
                    }
                },
                "aws_cloudwatch_event_target": {
                    "indexercachehealth": {
                        "rule": "${aws_cloudwatch_event_rule.indexercachehealth.name}",
                        "target_id": "indexercachehealth-event",
                        "arn": "${aws_lambda_function.indexercachehealth.arn}"
                    }
                },
                "aws_lambda_permission": {
                    "indexercachehealth": {
                        "function_name": "azul-indexer-prod-indexercachehealth",
                        "source_arn": "${aws_cloudwatch_event_rule.indexercachehealth.arn}"
                    }
                },
                "aws_lambda_event_source_mapping": {
                    "contribute-sqs-event-source": {
                        "batch_size": 1
                    }
                }
            }
        }
        """
        mapping = self.resource_name_mapping(tf_config)

        tf_config = {
            block_name: {
                resource_type: {
                    mapping.get((resource_type, name), name): resource
                    for name, resource in resources.items()
                }
                for resource_type, resources in block.items()
            } if block_name == 'resource' else block
            for block_name, block in tf_config.items()
        }

        def ref(resource_type, name):
            return '${' + resource_type + '.' + name + '.'

        ref_map = {
            ref(resource_type, name): ref(resource_type, new_name)
            for (resource_type, name), new_name in mapping.items()
        }

        def patch_refs(v: U) -> U:
            if isinstance(v, dict):
                return {k: patch_refs(v) for k, v in v.items()}
            elif isinstance(v, str):
                for old_ref, new_ref in ref_map.items():
                    if old_ref in v:
                        return v.replace(old_ref, new_ref)
                return v
            else:
                return v

        return patch_refs(tf_config)


chalice = Chalice()
