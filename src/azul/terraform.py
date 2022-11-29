from collections.abc import (
    Iterable,
    Sequence,
)
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
    Optional,
    TypeVar,
    Union,
)

import attr

from azul import (
    cached_property,
    config,
    require,
)
from azul.json import (
    copy_json,
)
from azul.template import (
    emit,
)
from azul.types import (
    AnyJSON,
    JSON,
    JSONs,
    MutableJSON,
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
        version = schema['format_version']
        require(version == '1.0', 'Unexpected format version', version)
        resources = chain.from_iterable(
            provider['resource_schemas'].items()
            for provider in schema['provider_schemas'].values()
            if 'resource_schemas' in provider
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
    def versions(self) -> MutableJSON:
        output = self.run('version', '-json')
        log.info('Terraform output:\n%s', output)
        versions = json.loads(output)
        return {
            'terraform': versions['terraform_version'],
            'providers': versions['provider_selections']
        }


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

    >>> list(_normalize_tf({}))
    []

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
                _sanitize_tf({
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
                })
                for resource_type, resource in _normalize_tf(resources)
            ]
            for k, v in tf_config.items()
        }


def emit_tf(config: Optional[JSON], *, tag_resources: bool = True) -> None:
    if config is not None:
        if tag_resources:
            config = populate_tags(config)
        config = _sanitize_tf(config)
    emit(config)


def _tags(resource_name: str, **overrides: str) -> dict[str, str]:
    """
    Return tags named for cloud resources based on :class:`azul.Config`.

    :param resource_name: The Terraform name of the resource.

    :param overrides: Additional tags that override the defaults.

    >>> from azul.doctests import assert_json
    >>> assert_json(_tags('service'))  #doctest: +ELLIPSIS
    {
        "billing": "...",
        "service": "azul",
        "deployment": "...",
        "owner": ...,
        "name": "azul-service-...",
        "component": "azul-service"
    }

    >>> from azul.doctests import assert_json
    >>> assert_json(_tags('service', billing='foo'))  #doctest: +ELLIPSIS
    {
        "billing": "foo",
        "service": "azul",
        "deployment": "...",
        "owner": ...,
        "name": "azul-service-...",
        "component": "azul-service"
    }
    """
    component = f'{config.resource_prefix}-{resource_name}'
    return {
        'billing': config.billing,
        'service': config.resource_prefix,
        'deployment': config.deployment_stage,
        'owner': config.owner,
        **(
            {
                'name': component,
                'component': component,
                'terraform_component': config.terraform_component
            }
            if config.terraform_component else
            {
                'name': config.qualified_resource_name(resource_name),
                'component': component
            }
        ),
        **overrides
    }


def _adjust_name_tag(resource_type: str, tags: dict[str, str]) -> dict[str, str]:
    return {
        'Name' if k == 'name' and resource_type.startswith('aws_') else k: v
        for k, v in tags.items()
    }


def provider_fragment(region: str) -> JSON:
    """
    Return a fragment of Terraform configuration JSON that specifies a
    resource's provider. Empty JSON will be returned if the resource's region
    is the same as the default region.
    A non-default region must first be configured by adding a matching provider
    for that region in `providers.tf.json`.
    """
    if region == config.region:
        return {}
    else:
        return {'provider': f'aws.{region}'}


def block_public_s3_bucket_access(tf_config: JSON) -> JSON:
    """
    Return a shallow copy of the given TerraForm configuration embellished with
    an aws_s3_bucket_public_access_block resource for each of the aws_s3_bucket
    resources in the argument. This is a convenient way to block public access
    to every bucket in a given Terraform configuration. The argument is not
    modified but the return value may share parts of the argument.
    """
    key = 'resource'
    tf_config = copy_json(tf_config, key)
    tf_config[key]['aws_s3_bucket_public_access_block'] = {
        resource_name: {
            **(
                {'provider': resource['provider']}
                if 'provider' in resource else {}
            ),
            'bucket': '${aws_s3_bucket.%s.id}' % resource_name,
            'block_public_acls': True,
            'block_public_policy': True,
            'ignore_public_acls': True,
            'restrict_public_buckets': True
        } for resource_name, resource in tf_config[key]['aws_s3_bucket'].items()
    }
    return tf_config


U = TypeVar('U', bound=AnyJSON)


class Chalice:

    @property
    def private_api_stage_config(self):
        """
        Returns the stage-specific fragment of Chalice configuration JSON that
        configures the Lambda function to be invoked by a private API Gateway, if
        enabled.
        """
        return {
            'api_gateway_endpoint_type': 'PRIVATE',
            'api_gateway_endpoint_vpce': [
                '${var.%s}' % config.var_vpc_endpoint_id
            ]
        } if config.private_api else {
        }

    @property
    def vpc_lambda_config(self):
        """
        Returns the Lambda-specific fragment of Chalice configuration JSON that
        configures the Lambda function to connect to the VPC.
        """
        return {
            'subnet_ids': '${var.%s}' % config.var_vpc_subnet_ids,
            'security_group_ids': [
                '${var.%s}' % config.var_vpc_security_group_id
            ],
        }

    def vpc_lambda_iam_policy(self, for_tf: bool = False):
        """
        Returns the fragment of IAM policy JSON needed for placing a Lambda function
        into a VPC.
        """
        actions = [
            'ec2:CreateNetworkInterface',
            'ec2:DescribeNetworkInterfaces',
            'ec2:DeleteNetworkInterface',
        ]
        return [
            {
                'actions': actions,
                'resources': ['*'],
            } if for_tf else {
                'Effect': 'Allow',
                'Action': actions,
                'Resource': ['*']
            }
        ]

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


class VPC:
    num_zones = 2  # An ALB needs at least two availability zones

    @classmethod
    def subnet_name(cls, public: bool) -> str:
        return 'public' if public else 'private'

    @classmethod
    def subnet_number(cls, zone: int, public: bool) -> int:
        # Returns even numbers for private subnets, odd numbers for public
        # subnets. The advantage of this numbering scheme is that it won't be
        # perturbed by adding zones.
        return 2 * zone + int(public)

    @classmethod
    def security_rule(cls, **rule):
        return {
            'cidr_blocks': None,
            'ipv6_cidr_blocks': None,
            'prefix_list_ids': None,
            'from_port': None,
            'protocol': None,
            'security_groups': None,
            'self': None,
            'to_port': None,
            'description': None,
            **rule
        }


vpc = VPC()
del VPC
