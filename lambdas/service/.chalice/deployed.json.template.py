import sys

from azul import config
from azul.deployment import aws
from azul.template import emit

api_gateway_id = aws.api_gateway_id(function_name=config.service_name)

if api_gateway_id is None:
    print("Cannot find existing deployment. This is expected during the first attempt at deploying. If Chalice fails "
          "with `Function already exists`, you may need to delete the function and try again.", file=sys.stderr)

emit(None if api_gateway_id is None else {
    "resources": [
        {
            "name": "api_handler",
            "resource_type": "lambda_function",
            "lambda_arn": f"arn:aws:lambda:{aws.region_name}:{aws.account}:function:{config.service_name}"
        },
        {
            "name": "rest_api",
            "resource_type": "rest_api",
            "rest_api_id": api_gateway_id,
            "rest_api_url": f"https://{api_gateway_id}.execute-api.{aws.region_name}.amazonaws.com/{config.deployment_stage}/"
        }
    ],
    "schema_version": "2.0",
    "backend": "api"
})
