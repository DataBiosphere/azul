from utils.deployment import aws
from utils.template import emit, env

api_gateway_id = aws.api_gateway_id(function_name=env.AZUL_INDEXER_NAME)

emit(None if api_gateway_id is None else {
    "dev": {
        "api_handler_name": env.AZUL_INDEXER_NAME,
        "api_handler_arn": f"arn:aws:lambda:{aws.region_name}:{aws.account}:function:{env.AZUL_INDEXER_NAME}",
        "rest_api_id": api_gateway_id,
        "lambda_functions": {},
        "backend": "api",
        "chalice_version": "",
        "api_gateway_stage": env.AZUL_DEPLOYMENT_STAGE,
        "region": aws.region_name
    }
})
