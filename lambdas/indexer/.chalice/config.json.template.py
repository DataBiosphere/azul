from azul import (
    config,
)
from azul.modules import (
    load_app_module,
)
from azul.template import (
    emit,
)
from azul.terraform import (
    chalice,
)

suffix = '-' + config.deployment_stage
assert config.indexer_name.endswith(suffix)

app_name = 'indexer'

indexer = load_app_module(app_name)

emit({
    'version': '2.0',
    'app_name': config.indexer_name[:-len(suffix)],  # Chalice appends stage name implicitly
    'api_gateway_stage': config.deployment_stage,
    'manage_iam_role': False,
    'iam_role_arn': '${aws_iam_role.%s.arn}' % app_name,
    'environment_variables': config.lambda_env,
    'lambda_timeout': config.api_gateway_lambda_timeout,
    'lambda_memory_size': 128,
    'stages': {
        config.deployment_stage: {
            **chalice.private_api_stage_config(app_name),
            'lambda_functions': {
                'api_handler': chalice.vpc_lambda_config(app_name),
                indexer.contribute.name: {
                    'reserved_concurrency': config.contribution_concurrency(retry=False),
                    'lambda_memory_size': 256,
                    'lambda_timeout': config.contribution_lambda_timeout(retry=False),
                    **chalice.vpc_lambda_config(app_name)
                },
                indexer.contribute_retry.name: {
                    'reserved_concurrency': config.contribution_concurrency(retry=True),
                    'lambda_memory_size': 4096,  # FIXME https://github.com/DataBiosphere/azul/issues/2902
                    'lambda_timeout': config.contribution_lambda_timeout(retry=True),
                    **chalice.vpc_lambda_config(app_name)
                },
                indexer.aggregate.name: {
                    'reserved_concurrency': config.aggregation_concurrency(retry=False),
                    'lambda_memory_size': 256,
                    'lambda_timeout': config.aggregation_lambda_timeout(retry=False),
                    **chalice.vpc_lambda_config(app_name)
                },
                indexer.aggregate_retry.name: {
                    'reserved_concurrency': config.aggregation_concurrency(retry=True),
                    'lambda_memory_size': 6500,
                    'lambda_timeout': config.aggregation_lambda_timeout(retry=True),
                    **chalice.vpc_lambda_config(app_name)
                },
                **(
                    {
                        indexer.forward_alb_logs.name: chalice.vpc_lambda_config(app_name),
                        indexer.forward_s3_logs.name: chalice.vpc_lambda_config(app_name),
                    }
                    if config.enable_log_forwarding else
                    {}
                ),
                **(
                    {
                        indexer.mirror.name: {
                            'reserved_concurrency': config.mirroring_concurrency,
                            'lambda_memory_size': 512,
                            'lambda_timeout': config.mirror_lambda_timeout
                            # No VPC for this function so as to avoid paying for
                            # NAT Gateway traffic
                        },
                    }
                    if config.enable_mirroring else
                    {}
                ),
                indexer.update_health_cache.name: {
                    'lambda_memory_size': 128,
                    'lambda_timeout': config.health_cache_lambda_timeout,
                    **chalice.vpc_lambda_config(app_name)
                }
            }
        }
    }
})
