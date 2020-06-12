from azul import config
from azul.deployment import aws
from azul.template import emit

suffix = '-' + config.deployment_stage
assert config.indexer_name.endswith(suffix)

emit({
    "version": "2.0",
    "app_name": config.indexer_name[:-len(suffix)],  # Chalice appends stage name implicitly
    "api_gateway_stage": config.deployment_stage,
    "manage_iam_role": False,
    "iam_role_arn": "${var.role_arn}",
    "environment_variables": aws.lambda_env,
    'lambda_timeout': config.api_gateway_timeout + config.api_gateway_timeout_padding,
    "lambda_memory_size": 128,
    "stages": {
        config.deployment_stage: {
            "lambda_functions": {
                # FIXME: Brittle coupling between the string literal below and
                #        the handler function name in app.py
                #        https://github.com/DataBiosphere/azul/issues/1848
                "contribute": {
                    "reserved_concurrency": config.indexer_concurrency,
                    "lambda_memory_size": 256,
                    "lambda_timeout": config.indexer_lambda_timeout,
                },
                "aggregate": {
                    "reserved_concurrency": config.indexer_concurrency,
                    "lambda_memory_size": 256,
                    "lambda_timeout": config.indexer_lambda_timeout,
                },
                "aggregate_retry": {
                    "reserved_concurrency": config.indexer_concurrency,
                    "lambda_memory_size": 3008,
                    "lambda_timeout": config.indexer_lambda_timeout,
                },
                config.indexer_cache_health_lambda_basename: {
                    "lambda_memory_size": 128,
                    "lambda_timeout": config.indexer_lambda_timeout,
                }
            }
        }
    }
})
