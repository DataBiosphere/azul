from azul import config
from azul.template import emit

emit({
    "version": "2.0",
    "app_name": config.qualified_resource_name("dependencies"),
    "api_gateway_stage": config.deployment_stage,
    "manage_iam_role": False,
    "lambda_memory_size": 128,
})
