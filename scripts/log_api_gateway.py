import json
import sys

# Converted to a string that expresses the structure of API log entries
# For more info see https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-logging.html
from azul.deployment import (
    aws,
)

# This script should only be called by Terraform.
# Do NOT run manually.

JSON_LOG_FORMAT = {
    "requestId": "$context.requestId",
    "ip": "$context.identity.sourceIp",
    "caller": "$context.identity.caller",
    "user": "$context.identity.user",
    "requestTime": "$context.requestTime",
    "httpMethod": "$context.httpMethod",
    "resourcePath": "$context.resourcePath",
    "status": "$context.status",
    "protocol": "$context.protocol",
    "responseLength": "$context.responseLength"
}


def clean_arn(arn: str) -> str:
    return arn[:-2] if arn.endswith(':*') else arn


def add_field(client, path: str, value: str, api_id: str, stage_name: str):
    client.update_stage(
        restApiId=api_id,
        stageName=stage_name,
        patchOperations=[
            {
                'op': 'add',
                'path': path,
                'value': value
            }
        ]
    )


def add_logging(api_id: str, stage_name: str, destination_arn: str):
    client = aws.client('apigateway')
    destination_arn = clean_arn(destination_arn)
    for path, value in [('/accessLogSettings/destinationArn', destination_arn),
                        ('/accessLogSettings/format', json.dumps(JSON_LOG_FORMAT))]:
        add_field(client, path, value, api_id, stage_name)


if __name__ == "__main__":
    add_logging(*sys.argv[1:])
