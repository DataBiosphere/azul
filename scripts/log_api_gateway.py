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
    "protocol": "$context.protocol",
    "httpMethod": "$context.httpMethod",
    "path": "$context.path",
    "requestId": "$context.requestId",
    "awsEndpointRequestId": "$context.awsEndpointRequestId",
    "awsEndpointRequestId2": "$context.awsEndpointRequestId2",
    "requestTime": "$context.requestTime",
    "status": "$context.status",
    "dataProcessed": "$context.dataProcessed",
    "error_message": "$context.error.message",
    "error_messageString": "$context.error.messageString",
    "error_responseType": "$context.error.responseType",
    "integration_error": "$context.integration.error",
    "integration_integrationStatus": "$context.integration.integrationStatus",
    "integration_latency": "$context.integration.latency",
    "integration_requestId": "$context.integration.requestId",
    "integration_status": "$context.integration.status",
    "integrationErrorMessage": "$context.integrationErrorMessage",
    "integrationLatency": "$context.integrationLatency",
    "integrationStatus": "$context.integrationStatus",
    "responseLatency": "$context.responseLatency",
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
    client = aws.apigateway
    destination_arn = clean_arn(destination_arn)
    for path, value in [('/accessLogSettings/destinationArn', destination_arn),
                        ('/accessLogSettings/format', json.dumps(JSON_LOG_FORMAT))]:
        add_field(client, path, value, api_id, stage_name)


if __name__ == "__main__":
    add_logging(*sys.argv[1:])
