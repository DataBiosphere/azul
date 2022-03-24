from azul import (
    config,
)
from azul.modules import (
    load_app_module,
)
from azul.template import (
    emit,
)

suffix = '-' + config.deployment_stage
assert config.indexer_name.endswith(suffix)

indexer = load_app_module('indexer')

emit({
    "version": "2.0",
    "app_name": config.indexer_name[:-len(suffix)],  # Chalice appends stage name implicitly
    "api_gateway_stage": config.deployment_stage,
    "manage_iam_role": False,
    "iam_role_arn": "${var.role_arn}",
    "environment_variables": config.lambda_env,
    "minimum_compression_size": config.minimum_compression_size,
    "lambda_timeout": config.api_gateway_lambda_timeout,
    "lambda_memory_size": 128,
    "stages": {
        config.deployment_stage: {
            "lambda_functions": {
                indexer.contribute.name: {
                    "reserved_concurrency": config.contribution_concurrency(retry=False),
                    "lambda_memory_size": 256,
                    "lambda_timeout": config.contribution_lambda_timeout(retry=False),
                },
                indexer.contribute_retry.name: {
                    "reserved_concurrency": config.contribution_concurrency(retry=True),
                    "lambda_memory_size": 4096,  # FIXME https://github.com/DataBiosphere/azul/issues/2902
                    "lambda_timeout": config.contribution_lambda_timeout(retry=True)
                },
                indexer.aggregate.name: {
                    "reserved_concurrency": config.aggregation_concurrency(retry=False),
                    "lambda_memory_size": 256,
                    "lambda_timeout": config.aggregation_lambda_timeout(retry=False)
                },
                indexer.aggregate_retry.name: {
                    "reserved_concurrency": config.aggregation_concurrency(retry=True),
                    "lambda_memory_size": 6500,
                    "lambda_timeout": config.aggregation_lambda_timeout(retry=True)
                },
                indexer.update_health_cache.name: {
                    "lambda_memory_size": 128,
                    "lambda_timeout": config.health_lambda_timeout
                }
            }
        }
    }
})
