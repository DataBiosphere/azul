from utils import config
from utils.deployment import aws
from utils.template import emit

api_gateway_id = aws.api_gateway_id(function_name=config.indexer_name)

emit(None if api_gateway_id is None else {
    config.deployment_stage: {
        "api_handler_name": config.indexer_name,
        "api_handler_arn": f"arn:aws:lambda:{aws.region_name}:{aws.account}:function:{config.indexer_name}",
        "rest_api_id": api_gateway_id,
        "lambda_functions": {},
        "backend": "api",
        "chalice_version": "",
        "api_gateway_stage": config.deployment_stage,
        "region": aws.region_name
    }
})
