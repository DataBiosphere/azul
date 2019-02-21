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
    "iam_role_arn": f"arn:aws:iam::{aws.account}:role/{config.indexer_name}",
    "environment_variables": config.lambda_env,
    "lambda_timeout": config.lambda_timeout,
    "lambda_memory_size": 256,
    "reserved_concurrency": config.indexer_concurrency,
    "stages": {
        config.deployment_stage: {
            "lambda_functions": {
                "write": {
                    "lambda_memory_size": 2048
                },
                "nudge": {
                    "lambda_memory_size": 128
                }
            }
        }
    }
})
