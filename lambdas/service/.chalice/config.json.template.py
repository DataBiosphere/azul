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
assert config.service_name.endswith(suffix)

service = load_app_module('service')

emit({
    "version": "2.0",
    "app_name": config.service_name[:-len(suffix)],  # Chalice appends stage name implicitly
    "api_gateway_stage": config.deployment_stage,
    "manage_iam_role": False,
    "iam_role_arn": "${var.role_arn}",
    "environment_variables": config.lambda_env,
    "minimum_compression_size": config.minimum_compression_size,
    "lambda_timeout": config.api_gateway_lambda_timeout,
    "lambda_memory_size": 2048,
    "stages": {
        config.deployment_stage: {
            "lambda_functions": {
                service.generate_manifest.name: {
                    "lambda_timeout": config.service_lambda_timeout
                },
                service.update_health_cache.name: {
                    "lambda_memory_size": 128,
                    "lambda_timeout": config.health_lambda_timeout
                }
            }
        }
    }
})
