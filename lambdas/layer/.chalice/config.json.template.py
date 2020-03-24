from azul import config
from azul.template import emit

emit({
    "version": "2.0",
    "app_name": config.qualified_resource_name("azul_dependencies"),
    "api_gateway_stage": config.deployment_stage,
    "manage_iam_role": False,
    "iam_role_arn": "${var.role_arn}",
    'lambda_timeout': config.api_gateway_timeout + config.api_gateway_timeout_padding,
    "lambda_memory_size": 128,
})
