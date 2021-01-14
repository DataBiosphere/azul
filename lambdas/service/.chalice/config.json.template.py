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
    "lambda_timeout": config.api_gateway_timeout + config.api_gateway_timeout_padding,
    "lambda_memory_size": 1024,
    "stages": {
        config.deployment_stage: {
            "lambda_functions": {
                service.generate_manifest.lambda_name: {
                    "lambda_timeout": config.service_lambda_timeout
                },
                config.cart_item_write_lambda_basename: {
                    "lambda_timeout": config.service_lambda_timeout
                },
                config.cart_export_dss_push_lambda_basename: {
                    "lambda_timeout": config.service_lambda_timeout
                },
                service.update_health_cache.lambda_name: {
                    "lambda_memory_size": 128,
                    "lambda_timeout": config.health_lambda_timeout
                }
            }
        }
    }
})
