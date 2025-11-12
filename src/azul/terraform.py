from collections import (
    defaultdict,
)
from collections.abc import (
    Iterable,
    Sequence,
)
import gzip
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
    Mapping,
)

import attr

from azul import (
    cache,
    cached_property,
    config,
    require,
)
from azul.chalice import (
    AzulChaliceApp,
)
from azul.deployment import (
    aws,
)
from azul.json import (
    copy_any_json,
    copy_json,
)
from azul.template import (
    emit,
)
from azul.types import (
    AnyMutableJSON,
    CompositeJSON,
    JSON,
    JSONs,
    MutableJSON,
    json_composite,
    json_dict,
    json_element_dicts,
    json_item_dicts,
    json_item_mappings,
    json_mapping,
    json_str,
    not_none,
)

log = logging.getLogger(__name__)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class TerraformSchema:
    versions: JSON
    document: JSON
    path: Path

    @classmethod
    def load(cls, path: Path):
        with gzip.open(path, 'rt') as f:
            doc = json.load(f)
        return cls(versions=doc['versions'],
                   document=doc['schema'],
                   path=path)

    def store(self):
        with gzip.open(self.path, 'wt') as f:
            doc = dict(versions=self.versions, schema=self.document)
            json.dump(doc, f)


class Terraform:

    def taggable_resource_types(self) -> set[str]:
        schema = self.schema.document
        version = schema['format_version']
        require(version == '1.0', 'Unexpected format version', version)
        resources = chain.from_iterable(
            provider['resource_schemas'].items()
            for provider in schema['provider_schemas'].values()
            if 'resource_schemas' in provider
        )
        return {
            resource_type
            for resource_type, resource in resources
            if 'tags' in resource['block']['attributes']
        }

    def run(self, *args: str, **kwargs) -> str:
        args = ['terraform', *args]
        log.info('Running %r', args)
        cmd = subprocess.run(args,
                             check=True,
                             stdout=subprocess.PIPE,
                             text=True,
                             shell=False,
                             **kwargs)
        return cmd.stdout

    def run_state_list(self) -> list[str]:
        try:
            stdout = self.run('state', 'list', stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            if e.returncode == 1 and 'No state file was found' in e.stderr:
                log.info('No state file was found, assuming empty list of resources.')
                return []
            else:
                raise
        else:
            return stdout.splitlines()

    schema_path = Path(config.project_root) / 'terraform' / '_schema.json.gz'

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


def emit_tf(config: JSON | None, *, tag_resources: bool = True) -> None:
    if config is None:
        emit(config)
    else:
        emit(_transform_tf(config, tag_resources=tag_resources))


def _sanitize_tf(tf_config: CompositeJSON) -> CompositeJSON:
    """
    Avoid errors like

        Error: Missing block label

          on api_gateway.tf.json line 12:
          12:     "resource": []

        At least one object property is required, whose name represents the resource
        block's type.
    """
    if isinstance(tf_config, Mapping):
        return {k: v for k, v in tf_config.items() if v}
    elif isinstance(tf_config, Sequence):
        return [v for v in tf_config if v]
    else:
        assert False, type(tf_config)


def _normalize_tf(tf_config: CompositeJSON) -> Iterable[tuple[str, JSON]]:
    """
    Certain levels of a Terraform JSON structure can either be a single
    dictionary or a list of dictionaries. For example, these are equivalent:

        {"resource": {"<type>": {"<name>": {"foo": ...}}}}
        {"resource": [{"<type>": {"<name>": {"foo": ...}}}]}

    So are these:

        {"resource": {"<type>": {"<name>": {"foo": ...}, "<name2>": {"bar": ...}}}}
        {"resource": {"<type>": [{"<name>": {"foo": ...}}, {"<name2>": {"bar": ...}}]}}

    This function normalizes input to prefer the second form of both cases to
    make parsing Terraform configuration simpler. It returns an iterator of the
    dictionary entries in the argument, regardless which form is used.

    >>> def n(c):
    ...     return list(_normalize_tf(c))

    >>> n({})
    []

    A Singleton dict:

    >>> n({'t': {'r':{}}})
    [('t', {'r': {}})]

    A singleton list of a singleton dict:

    >>> n([{'t': {'r': {}}}])
    [('t', {'r': {}})]

    A two-entry dict:

    >>> n({"t1": {"r1": {}}, "t2": {"r2" :{}}})
    [('t1', {'r1': {}}), ('t2', {'r2': {}})]

    A two-entry list of singleton dicts:

    >>> n([{"t1": {"r1": {}}}, {"t2": {"r2": {}}}])
    [('t1', {'r1': {}}), ('t2', {'r2': {}})]

    A singleton list of a two-entry dict:

    >>> n([{"t1": {"r1": {}}, "t2": {"r2": {}}}])
    [('t1', {'r1': {}}), ('t2', {'r2': {}})]

    A two-entry list of two-entry dicts:

    >>> n([{"t1": {"r1": {}}, "t2": {"r2": {}}}, {"t1": {"r3": {}}, "t2": {"r4": {}}}])
    [('t1', {'r1': {}}), ('t2', {'r2': {}}), ('t1', {'r3': {}}), ('t2', {'r4': {}})]
    """
    if isinstance(tf_config, Mapping):
        return json_item_mappings(tf_config)
    elif isinstance(tf_config, Sequence):
        return chain.from_iterable(map(json_item_mappings, tf_config))
    else:
        assert False, type(tf_config)


def _transform_tf(tf_config: JSON, *, tag_resources: bool = True) -> JSON:
    """
    Add tags to all taggable resources and change the `name` tag to `Name`
    for tagged AWS resources.
    """
    taggable_types = terraform.taggable_resource_types() if tag_resources else {}
    return json_mapping(_sanitize_tf({
        block_name: _sanitize_tf([
            _sanitize_tf({
                resource_type: _sanitize_tf([
                    {
                        resource_name: {
                            **resource,
                            **(
                                _tagged_resource(resource_type, resource_name, resource)
                                if block_name == 'resource' and resource_type in taggable_types else
                                {}
                            )
                        }
                    }
                    for resource_name, resource in _normalize_tf(json_composite(resources))
                ])
            })
            for resource_type, resources in _normalize_tf(json_composite(block))
        ])
        if block_name in {'data', 'resource'} else
        block
        for block_name, block in tf_config.items()
    }))


def _tagged_resource(resource_type: str, resource_name: str, resource: JSON) -> JSON:
    tags = json_mapping(resource.get('tags', {}))
    return {
        'tags': _tags(resource_type, resource_name, tags)
    }


def _tags(resource_type: str, resource_name: str, tags: JSON) -> JSON:
    """
    Return tags named for cloud resources based on :class:`azul.Config`.

    :param resource_type: The Terraform resource type

    :param resource_name: The Terraform name of the resource

    :param tags: Additional tags that override the defaults

    >>> from azul.doctests import assert_json
    >>> from test.azul_test_case import patch_config

    >>> with patch_config('terraform_component', 'foo'):
    ...     assert_json(_tags('aws_instance', 'service', {}))
    ...     #doctest: +ELLIPSIS
    {
        "billing": "...",
        "service": "azul",
        "deployment": "...",
        "owner": "...",
        "Name": "azul-...",
        "component": "azul-service",
        "terraform_component": "foo"
    }

    >>> with patch_config('terraform_component', None):
    ...     assert_json(_tags('aws_instance', 'service', {'billing' : 'foo'}))
    ...     #doctest: +ELLIPSIS
    {
        "billing": "foo",
        "service": "azul",
        "deployment": "...",
        "owner": "...",
        "Name": "azul-service-...",
        "component": "azul-service"
    }
    """
    component = f'{config.resource_prefix}-{resource_name}'
    tags = {
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
        **tags
    }
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
    tf_config = copy_json(tf_config, 'resource')
    resources = json_dict(tf_config['resource'])
    bucket_resources = json_dict(resources['aws_s3_bucket'])
    resources['aws_s3_bucket_public_access_block'] = {
        resource_name: {
            **(
                {'provider': resource['provider']}
                if 'provider' in resource else
                {}
            ),
            'bucket': '${aws_s3_bucket.%s.id}' % resource_name,
            'block_public_acls': True,
            'block_public_policy': True,
            'ignore_public_acls': True,
            'restrict_public_buckets': True
        } for resource_name, resource in json_item_dicts(bucket_resources)
    }
    return tf_config


def enable_s3_bucket_inventory(tf_config: JSON,
                               dest_bucket_ref: str = 'data.aws_s3_bucket.logs',
                               /,
                               ) -> JSON:
    tf_config = copy_json(tf_config, 'resource')
    resources = json_dict(tf_config['resource'])
    bucket_resources = json_dict(resources['aws_s3_bucket'])
    resources['aws_s3_bucket_inventory'] = {
        resource_name: {
            **(
                {'provider': resource['provider']}
                if 'provider' in resource else
                {}
            ),
            'bucket': '${aws_s3_bucket.%s.id}' % resource_name,
            'name': config.qualified_resource_name('inventory'),
            'included_object_versions': 'All',
            'destination': {
                'bucket': {
                    'format': 'CSV',
                    'bucket_arn': '${%s.arn}' % dest_bucket_ref,
                    'prefix': 'inventory'
                }
            },
            'schedule': {
                'frequency': 'Daily'
            },
            'optional_fields': [
                'Size',
                'LastModifiedDate',
                'StorageClass',
                'ETag',
                'IsMultipartUploaded',
                'ReplicationStatus',
                'EncryptionStatus',
                'ChecksumAlgorithm',
                'BucketKeyStatus',
                'IntelligentTieringAccessTier',
                'ObjectLockMode',
                'ObjectLockRetainUntilDate',
                'ObjectLockLegalHoldStatus'
            ]
        } for resource_name, resource in json_item_dicts(bucket_resources)
    }
    return tf_config


def set_empty_s3_bucket_lifecycle_config(tf_config: JSON) -> JSON:
    """
    Return a shallow copy of the given TerraForm configuration embellished with
    an `aws_s3_bucket_lifecycle_configuration` resource for each of the
    `aws_s3_bucket` resources in the argument that lack an explicit lifecycle
    configuration. The argument is not modified but the return value may share
    parts of the argument.
    """
    tf_config = copy_json(tf_config, 'resource')
    resources = json_dict(tf_config['resource'])
    lifecycles = resources.get('aws_s3_bucket_lifecycle_configuration', {})
    explicit = {
        json_str(lifecycle_config['bucket']).split('.')[1]
        for _, lifecycle_config in json_item_dicts(lifecycles)
    }
    buckets = resources.get('aws_s3_bucket', {})
    for resource_name, bucket in json_item_dicts(buckets):
        if resource_name not in explicit:
            # We can't create a completely empty policy, but a disabled policy
            # achieves the goal of preventing/removing policies that originate
            # from outside TF.
            bucket.setdefault('lifecycle_rule', {
                'id': config.qualified_resource_name('dummy'),
                'enabled': False,
                'expiration': {'days': 36500}
            })
    return tf_config


class Chalice:

    def private_api_stage_config(self, app_name: str) -> JSON:
        """
        Returns the stage-specific fragment of Chalice configuration JSON that
        configures the Lambda function to be invoked by a private API Gateway,
        if enabled.
        """
        return {
            'api_gateway_endpoint_type': 'PRIVATE',
            'api_gateway_endpoint_vpce': ['${aws_vpc_endpoint.%s.id}' % app_name]
        } if config.private_api else {
        }

    def vpc_lambda_config(self, app_name: str) -> JSON:
        """
        Returns the Lambda-specific fragment of Chalice configuration JSON that
        configures the Lambda function to connect to the VPC.
        """
        return {
            'subnet_ids': [
                '${data.aws_subnet.gitlab_%s_%s.id}' % (vpc.subnet_name(public=False), zone)
                for zone in range(vpc.num_zones)
            ],
            'security_group_ids': ['${aws_security_group.%s.id}' % app_name],
        }

    def vpc_lambda_iam_policy(self, for_tf: bool = False) -> JSONs:
        """
        Returns the fragment of IAM policy JSON needed for placing a Lambda
        function into a VPC.
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

    def package_dir_path(self, app_name) -> Path:
        root = Path(config.project_root)
        return root / 'lambdas' / app_name / '.chalice' / 'terraform'

    def package_zip_path(self, app_name) -> Path:
        return self.package_dir_path(app_name) / 'deployment.zip'

    def tf_config_path(self, app_name) -> Path:
        return self.package_dir_path(app_name) / 'chalice.tf.json'

    def patch_resource_names(self, app_name: str, tf_config: JSON) -> MutableJSON:
        """
        Patch the names of local variables, resources and data source in the
        given Chalice-generated Terraform config. Definitions and references
        will be patched.

        >>> from azul.doctests import assert_json

        >>> assert_json(chalice.patch_resource_names('indexer', {
        ...     'locals': {
        ...         'foo': ''
        ...     },
        ...     'data': {
        ...         'aws_foo': {
        ...             'bar': {}
        ...         }
        ...     },
        ...     "resource": {
        ...         "aws_lambda_function": {
        ...              "indexercachehealth": {  # patch
        ...                 "foo": "${data.aws_foo.bar}${md5(local.foo)}"
        ...              }
        ...         },
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
            "locals": {
                "indexer_foo": ""
            },
            "data": {
                "aws_foo": {
                    "indexer_bar": {}
                }
            },
            "resource": {
                "aws_lambda_function": {
                    "indexer_indexercachehealth": {
                        "foo": "${data.aws_foo.indexer_bar}${md5(local.indexer_foo)}"
                    }
                },
                "aws_cloudwatch_event_rule": {
                    "indexer_indexercachehealth": {
                        "name": "indexercachehealth-event"
                    }
                },
                "aws_cloudwatch_event_target": {
                    "indexer_indexercachehealth": {
                        "rule": "${aws_cloudwatch_event_rule.indexer_indexercachehealth.name}",
                        "target_id": "indexercachehealth-event",
                        "arn": "${aws_lambda_function.indexer_indexercachehealth.arn}"
                    }
                },
                "aws_lambda_permission": {
                    "indexer_indexercachehealth": {
                        "function_name": "azul-indexer-prod-indexercachehealth",
                        "source_arn": "${aws_cloudwatch_event_rule.indexer_indexercachehealth.arn}"
                    }
                },
                "aws_lambda_event_source_mapping": {
                    "indexer_contribute": {
                        "batch_size": 1
                    }
                }
            }
        }
        """

        renamed = {}

        def rename(block_name, resource_type, old):
            # Rename and track the renaming as a side effect
            new = self._rename_chalice_resource(app_name, old)
            renamed[(block_name, resource_type, old)] = new
            return new

        # Translate the definitions
        tf_result: MutableJSON = {
            block_name: {
                resource_type: {
                    rename(block_name, resource_type, resource_name): copy_json(resource)
                    for resource_name, resource in json_item_mappings(resources)
                }
                for resource_type, resources in json_item_mappings(block)
            }
            if block_name in ('resource', 'data') else
            {
                rename(block_name, None, name): copy_any_json(value)
                for name, value in json_mapping(block).items()
            }
            if block_name == 'locals' else
            copy_any_json(block)
            for block_name, block in tf_config.items()
        }

        def ref(block_name: str, resource_type: str, name: str) -> str:
            if block_name == 'resource':
                return '.'.join([resource_type, name])
            elif block_name == 'locals':
                return '.'.join(['local', name])
            else:
                return '.'.join([block_name, resource_type, name])

        ref_map = {
            ref(block_name, resource_type, name): ref(block_name, resource_type, new_name)
            for (block_name, resource_type, name), new_name in renamed.items()
        }
        assert len(ref_map) == len(renamed)
        # Sort in reverse so that keys that are prefixes of other keys go last
        rev_ref_map = sorted(ref_map.items(), reverse=True)

        def patch_refs[T: AnyMutableJSON](v: T) -> T:
            if isinstance(v, dict):
                return type(v)((k, patch_refs(v)) for k, v in json_dict(v).items())
            elif isinstance(v, list):
                return type(v)(map(patch_refs, v))
            elif isinstance(v, str):
                r: str = v
                for old_ref, new_ref in rev_ref_map:
                    r = r.replace(old_ref, new_ref)
                assert isinstance(r, type(v))
                return r
            else:
                return v

        return json_dict(patch_refs(tf_result))

    def rename_chalice_resource_in_tf_state(self, reference: str) -> str:
        """
        Translate the resource and data references found Terraform state that
        resulted from applying Terraform configuration generated by Chalice.
        The configuration is assumed to have been applied as a module,  which
        is how we used to incorporate the Chalice-generated TF config into our
        own. The returned references omit the module prefix and instead
        disambiguate between indexer and service lambda directly in the
        resource name, eliminating the need to apply the config as a module.

        >>> f = chalice.rename_chalice_resource_in_tf_state

        >>> f('module.chalice_indexer.aws_foo.rest_api')
        'aws_foo.indexer'

        >>> f('module.chalice_indexer.aws_foo.api_handler')
        'aws_foo.indexer'

        >>> f('module.chalice_indexer.aws_foo.rest_api_invoke')
        'aws_foo.indexer'

        >>> f('module.chalice_indexer.data.aws_foo.chalice')
        'data.aws_foo.indexer'

        >>> f('module.chalice_indexer.aws_foo.aggregate-sqs-event-source')
        'aws_foo.indexer_aggregate'
        """
        prefix, module, *reference = reference.split('.')
        assert prefix == 'module', prefix
        prefix, module = module.split('_')
        assert prefix == 'chalice'
        return self.rename_chalice_resource(module, reference)

    def rename_chalice_resource(self, app_name: str, reference: list[str]) -> str:
        """
        Translate the resource and data references found in Terraform
        configuration generated by Chalice.

        :param reference: the reference to translate

        :param app_name: the name of the Lambda function to which the resource
                            belongs.
        """
        assert app_name in ('service', 'indexer'), app_name
        *reference, resource_type, resource_name = reference
        if reference:
            assert reference == ['data']
        resource_name = self._rename_chalice_resource(app_name, resource_name)
        return '.'.join([*reference, resource_type, resource_name])

    def _rename_chalice_resource(self, app_name: str, resource_name: str) -> str:
        singletons = {
            'rest_api',
            'api_handler',
            'rest_api_invoke',
            'chalice',
            'chalice_api_swagger'
        }
        if resource_name in singletons:
            resource_name = app_name
        else:
            resource_name = resource_name.removesuffix('-sqs-event-source')
            resource_name = resource_name.removesuffix('-event')
            resource_name = app_name + '_' + resource_name
        return resource_name

    @cache
    def tf_config(self, app_name):
        with open(self.tf_config_path(app_name)) as f:
            tf_config = json.load(f)
        tf_config = self.patch_resource_names(app_name, tf_config)
        resources = json_dict(tf_config['resource'])
        data = json_dict(tf_config['data'])
        locals = json_dict(tf_config['locals'])

        # null_data_source has been deprecated and locals should be used instead.
        # However, the data sources defined underneath it aren't actually used
        # anywhere so we can just delete the entry.
        del data['null_data_source']

        if config.private_api:
            # Hack to inject the VPC endpoint IDs that Chalice doesn't (but should)
            # add when the `api_gateway_endpoint_vpce` config is used.
            rest_apis = json_dict(resources['aws_api_gateway_rest_api'])
            rest_api = json_dict(rest_apis[app_name])
            json_dict(rest_api['endpoint_configuration'])['vpc_endpoint_ids'] = [
                '${aws_vpc_endpoint.%s.id}' % app_name
            ]

        functions = json_item_dicts(resources['aws_lambda_function'])
        for resource_name, resource in functions:
            assert 'layers' not in resource
            resource['layers'] = ['${aws_lambda_layer_version.dependencies.arn}']
            # Publishing a new Lambda function version each time lets us perform
            # an atomic update of the function, avoiding a race condition
            # between the update of the function's configuration and its code.
            resource['publish'] = True
            env = config.es_endpoint_env(
                es_endpoint=(
                    aws.es_endpoint
                    if config.share_es_domain else
                    '${aws_opensearch_domain.index.endpoint}:443'
                ),
                es_instance_count=(
                    not_none(aws.es_instance_count)
                    if config.share_es_domain else
                    '${aws_opensearch_domain.index.cluster_config[0].instance_count}'
                )
            )
            json_dict(json_dict(resource['environment'])['variables']).update(env)
            package_zip = str(self.package_zip_path(app_name))
            resource['source_code_hash'] = '${filebase64sha256("%s")}' % package_zip
            resource['filename'] = package_zip

        # Replace any references to unqualified function ARNs emitted by Chalice
        # with references to the alias.
        #
        def function_to_alias[T: AnyMutableJSON](v: T) -> T:
            if isinstance(v, dict):
                return type(v)((k, function_to_alias(v)) for k, v in v.items())
            elif isinstance(v, list):
                return type(v)(map(function_to_alias, v))
            elif isinstance(v, str) and v.endswith(('.arn}', '.invoke_arn}')):
                r = v.replace('${aws_lambda_function', '${aws_lambda_alias')
                assert isinstance(r, type(v))
                return r
            else:
                return v

        openapi_spec = json_dict(json.loads(json_str(locals[app_name])))
        openapi_spec = function_to_alias(openapi_spec)
        resources = function_to_alias(resources)

        assert 'aws_cloudwatch_log_group' not in resources
        functions = json_item_dicts(resources['aws_lambda_function'])
        resources['aws_cloudwatch_log_group'] = {
            f'{resource_name}_lambda': {
                'name': f'/aws/lambda/{resource['function_name']}',
                'retention_in_days': config.audit_log_retention_days
            }
            for resource_name, resource in functions
        }

        for resource_type, argument in [
            ('aws_cloudwatch_event_rule', 'name'),
            ('aws_cloudwatch_event_target', 'target_id')
        ]:
            # Currently, Chalice fails to prefix the names of some resources. We
            # need them to be prefixed with `azul-` to allow for limiting the
            # scope of certain IAM permissions for Gitlab and, more importantly,
            # the deployment stage so these resources are segregated by deployment.
            for _, resource in json_item_dicts(resources[resource_type]):
                function_name, _, suffix = json_str(resource[argument]).partition('-')
                assert suffix == 'event', suffix
                assert function_name, function_name
                resource[argument] = config.qualified_resource_name(function_name)

        # Chalice-generated S3 bucket notifications include the bucket name in
        # the resource name, resulting in an invalid resource name when the
        # bucket name contains periods. Bucket names cannot include underscores
        # (https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html),
        # so replacing the periods with underscores results in valid resource
        # names while retaining the correlation with bucket names.
        try:
            bucket_notifications = resources['aws_s3_bucket_notification']
        except KeyError:
            pass
        else:
            resources['aws_s3_bucket_notification'] = {
                key.replace('.', '_'): value
                for key, value in json_item_dicts(bucket_notifications)
            }
            # To prevent a race condition by Terraform, we make the bucket
            # notifications depend on the related aws_lambda_permission.
            permissions_by_function = defaultdict(set)
            permissions = resources['aws_lambda_permission']
            for permission_name, permission in json_item_dicts(permissions):
                function_ref = permission['function_name']
                permissions_by_function[function_ref].add(permission_name)
            for _, notification in json_item_dicts(resources['aws_s3_bucket_notification']):
                assert 'depends_on' not in notification, notification
                notification['depends_on'] = [
                    f'aws_lambda_permission.{permission_name}'
                    for function in json_element_dicts(notification['lambda_function'])
                    for permission_name in permissions_by_function[function['lambda_function_arn']]
                ]

        # The fix for https://github.com/aws/chalice/issues/1237 introduced the
        # create_before_destroy hack and it may have helped but has far-ranging
        # implications such as pushing create-before-destroy semantics upstream
        # to the dependencies.
        #
        # This is what caused https://github.com/DataBiosphere/azul/issues/4752
        #
        # Managing the stage as an explicit resource as per TF recommendation
        #
        # https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/api_gateway_deployment
        #
        # and using the new `replace_triggered_by` lifecycle property introduced
        # in TF 1.2 to propagate the replacement downstream is a more intuitive
        # and less intrusive fix.
        #
        deployments = json_dict(resources['aws_api_gateway_deployment'])
        deployment = json_dict(deployments[app_name])
        stage_name = deployment.pop('stage_name')
        require(stage_name == config.deployment_stage,
                'The TF config from Chalice does not match the selected deployment',
                stage_name, config.deployment_stage)
        del json_dict(deployment['lifecycle'])['create_before_destroy']
        assert not deployment['lifecycle'], deployment
        del deployment['lifecycle']
        deployment['triggers'] = {'redeployment': deployment.pop('stage_description')}

        # Using Terraform to specify the REST API minimum compression size
        # proved to be problematic as it would first make an UpdateRestApi call
        # to set the property, followed by a PutRestApi call with mode=overwrite
        # which would reset the property back to its default value (disabled).
        # Setting this property using AWS API Gateway extensions to the OpenAPI
        # specification works around this issue.
        #
        # We ran into similar difficulties when using Terraform to configure
        # default responses for the API, so we use these extensions for that
        # purpose, too.
        #
        rest_apis = json_dict(resources['aws_api_gateway_rest_api'])
        rest_api = json_dict(rest_apis[app_name])
        assert 'minimum_compression_size' not in rest_api, rest_api
        key = 'x-amazon-apigateway-minimum-compression-size'
        openapi_spec[key] = config.minimum_compression_size

        # When mapping a static value to a response parameter, the value
        # must be enclosed within a pair of single quotes. Note that
        # azul.strings.single_quote() is not used here since API Gateway allows
        # internal single quotes, which that function would prohibit.
        #
        # https://docs.aws.amazon.com/apigateway/latest/developerguide/request-response-data-mappings.html#mapping-response-parameters
        #
        security_headers: MutableJSON = {
            f'gatewayresponse.header.{k}': f"'{v}'"
            for k, v in AzulChaliceApp.security_headers().items()
        }
        default_responses: MutableJSON = {
            f'DEFAULT_{response_type}': {'responseParameters': security_headers}
            for response_type in ['4XX', '5XX']
        }
        error_responses: MutableJSON = {
            'INTEGRATION_FAILURE': {
                'statusCode': '502',
                'responseParameters': security_headers,
                'responseTemplates': {
                    "application/json": json.dumps({
                        'message': '502 Bad Gateway. The server was unable '
                                   'to complete your request.'
                    })
                }
            },
            'INTEGRATION_TIMEOUT': {
                'statusCode': '504',
                'responseParameters': {
                    **security_headers,
                    'gatewayresponse.header.Retry-After': "'10'"
                },
                'responseTemplates': {
                    "application/json": json.dumps({
                        'message': '504 Gateway Timeout. Wait the number of '
                                   'seconds specified in the `Retry-After` '
                                   'header before retrying the request.'
                    })
                }
            }
        }
        gateway_responses = error_responses | default_responses
        assert 'aws_api_gateway_gateway_response' not in resources, resources
        openapi_spec['x-amazon-apigateway-gateway-responses'] = gateway_responses
        locals[app_name] = json.dumps(openapi_spec)

        # Replace the hard-coded ARN emitted by Chalice with a resource
        # reference so that the event source (the queue) is created before the
        # event source mapping depending on it.
        #
        if app_name == 'indexer':
            event_source_mappings = resources['aws_lambda_event_source_mapping']
            for _, resource in json_item_dicts(event_source_mappings):
                _, _, resource_name = json_str(resource['event_source_arn']).rpartition(':')
                suffix = '.fifo' if resource_name.endswith('.fifo') else ''
                sqs_name, _ = config.unqualified_resource_name(resource_name, suffix)
                resource['event_source_arn'] = f'${{aws_sqs_queue.{sqs_name}.arn}}'

        # Ensure that the Lambda permissions for the previous aliases aren't
        # deleted until after the permissions for the new aliases have been
        # created.
        #
        for name, resource in json_item_dicts(resources['aws_lambda_permission']):
            assert 'lifecycle' not in resource, resource
            resource['lifecycle'] = {'create_before_destroy': True}

        return {
            'resource': resources,
            'data': data,
            'locals': locals
        }


chalice = Chalice()


class VPC:
    num_zones = 2  # An ALB needs at least two availability zones

    # These are TF resource names, the real-world resource names are fixed by AWS.
    default_vpc_name = 'default'
    default_security_group_name = 'default'

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
