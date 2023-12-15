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
assert config.service_name.endswith(suffix)

app_name = 'service'

service = load_app_module(app_name)

emit({
    "version": "2.0",
    "app_name": config.service_name[:-len(suffix)],  # Chalice appends stage name implicitly
    "api_gateway_stage": config.deployment_stage,
    "manage_iam_role": False,
    "iam_role_arn": "${aws_iam_role.%s.arn}" % app_name,
    "environment_variables": config.lambda_env,
    "minimum_compression_size": config.minimum_compression_size,
    "lambda_timeout": config.api_gateway_lambda_timeout,
    "lambda_memory_size": 2048,
    **chalice.vpc_lambda_config(app_name),
    "stages": {
        config.deployment_stage: {
            **chalice.private_api_stage_config(app_name),
            "lambda_functions": {
                "api_handler": chalice.vpc_lambda_config(app_name),
                service.generate_manifest.name: {
                    "lambda_timeout": config.service_lambda_timeout
                },
                service.update_health_cache.name: {
                    "lambda_memory_size": 128,
                    "lambda_timeout": config.health_cache_lambda_timeout
                }
            }
        }
    }
})
