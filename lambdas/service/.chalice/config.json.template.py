from azul import (
    config,
)
from azul.infra.terraform import (
    chalice,
)
from azul.modules import (
    load_app_module,
)
from azul.template import (
    emit,
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
    "lambda_timeout": config.api_gateway_lambda_timeout,
    "lambda_memory_size": 768 if config.is_anvil_enabled() else 512,
    **chalice.vpc_lambda_config(app_name),
    "stages": {
        config.deployment_stage: {
            **chalice.private_api_stage_config(app_name),
            "lambda_functions": {
                "api_handler": chalice.vpc_lambda_config(app_name),
                service.generate_manifest.name: {
                    "lambda_timeout": config.service_lambda_timeout,
                    # Creating verbatim PFB manifests for large AnVIL datasets
                    # requires more memory than the default, so we raise it to
                    # the maximum.
                    "lambda_memory_size": (
                        10240 if config.deployment.is_stable and config.is_anvil_enabled()
                        else 3009 if config.deployment.is_stable
                        else 2048
                    ),
                },
                service.update_health_cache.name: {
                    "lambda_memory_size": 160,
                    "lambda_timeout": config.health_cache_lambda_timeout
                }
            }
        }
    }
})
