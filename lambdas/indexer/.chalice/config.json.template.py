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
    "lambda_timeout": 300,
    "lambda_memory_size": 128,
    "reserved_concurrency": 64
})
