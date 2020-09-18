from argparse import (
    ArgumentParser,
)
import json
import logging
from pathlib import (
    Path,
)
import shutil
import sys
from typing import (
    TypeVar,
)

from azul import (
    config,
)
from azul.deployment import (
    populate_tags,
)
from azul.files import (
    write_file_atomically,
)
from azul.logging import (
    configure_script_logging,
)
from azul.types import (
    AnyJSON,
    JSON,
)

log = logging.getLogger(__name__)


def transform_tf(input_json):
    # Using the default provider makes switching deployments easier
    del input_json['provider']

    assert 'variable' not in input_json
    input_json['variable'] = {
        'role_arn': {},
        'layer_arn': {},
        'es_endpoint': {},
        'es_instance_count': {}
    }

    input_json['output']['rest_api_id'] = {
        'value': '${aws_api_gateway_rest_api.rest_api.id}'
    }

    for func in input_json['resource']['aws_lambda_function'].values():
        assert 'layers' not in func
        func['layers'] = ["${var.layer_arn}"]

        # Inject ES-specific environment from variables set by Terraform.
        for var, val in config.es_endpoint_env(
            es_endpoint='${var.es_endpoint[0]}:${var.es_endpoint[1]}',
            es_instance_count='${var.es_instance_count}'
        ).items():
            func['environment']['variables'][var] = val

    def patch_cloudwatch_resource(resource_type_name, property_name):
        # Currently, Chalice fails to prefix the names of some resources. We
        # need them to be prefixed with `azul-` to allow for limiting the
        # scope of certain IAM permissions for Gitlab and, more importantly,
        # the deployment stage so these resources are segregated by deployment.
        for resource in input_json['resource'][resource_type_name].values():
            function_name, _, suffix = resource[property_name].partition('-')
            assert suffix == 'event', suffix
            assert function_name, function_name
            resource[property_name] = config.qualified_resource_name(function_name)

    patch_cloudwatch_resource('aws_cloudwatch_event_rule', 'name')
    patch_cloudwatch_resource('aws_cloudwatch_event_target', 'target_id')

    return input_json


def patch_resource_names(tf_config: JSON) -> JSON:
    """
    Some Chalice-generated resources have named ending in `-event`. The
    dash prevents generation of a fully qualified resource name (i.e.,
    with `config.qualified_resource_name`. This function returns
    Terraform configuration with names and references updated to
    remove the trailing `-event`.

    >>> from azul.doctests import assert_json
    >>> assert_json(patch_resource_names({
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
    suffix = '-event'

    mapping = {}
    for resource_type, resources in tf_config['resource'].items():
        for name in resources:
            if name.endswith(suffix):
                new_name = name[:-len(suffix)]
                mapping[resource_type, name] = new_name

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


U = TypeVar('U', bound=AnyJSON)


def main(argv):
    parser = ArgumentParser(
        description='Prepare the Terraform config generated by `chalice package'
                    '--pkg-format terraform` and copy it into the terraform/ '
                    'directory.'
    )
    parser.add_argument('lambda_name', help='the lambda of the config that will be '
                                            'transformed and copied')
    options = parser.parse_args(argv)
    source_dir = Path(config.project_root) / 'lambdas' / options.lambda_name / '.chalice' / 'terraform'
    output_dir = Path(config.project_root) / 'terraform' / options.lambda_name
    output_dir.mkdir(exist_ok=True)

    deployment_src = source_dir / 'deployment.zip'
    deployment_dst = output_dir / 'deployment.zip'
    log.info('Copying %s to %s', deployment_src, deployment_dst)
    shutil.copyfile(deployment_src, deployment_dst)

    tf_src = source_dir / 'chalice.tf.json'
    tf_dst = output_dir / 'chalice.tf.json'
    log.info('Transforming %s to %s', tf_src, tf_dst)
    with open(tf_src, 'r') as f:
        output_json = json.load(f)
    output_json = populate_tags(patch_resource_names(transform_tf(output_json)))
    with write_file_atomically(tf_dst) as f:
        json.dump(output_json, f, indent=4)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
