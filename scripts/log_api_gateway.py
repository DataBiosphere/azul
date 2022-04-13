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
    "accountId": "$context.accountId",
    "apiId": "$context.apiId",
    "authorizer_claims_property": "$context.authorizer.claims.property",
    "authorizer_error": "$context.authorizer.error",
    "authorizer_principalId": "$context.authorizer.principalId",
    "authorizer_property": "$context.authorizer.property",
    "awsEndpointRequestId": "$context.awsEndpointRequestId",
    "awsEndpointRequestId2": "$context.awsEndpointRequestId2",
    "customDomain_basePathMatched": "$context.customDomain.basePathMatched",
    "dataProcessed": "$context.dataProcessed",
    "domainName": "$context.domainName",
    "domainPrefix": "$context.domainPrefix",
    "error_message": "$context.error.message",
    "error_messageString": "$context.error.messageString",
    "error_responseType": "$context.error.responseType",
    "extendedRequestId": "$context.extendedRequestId",
    "httpMethod": "$context.httpMethod",
    "identity_accountId": "$context.identity.accountId",
    "identity_caller": "$context.identity.caller",
    "identity_cognitoAuthenticationProvider": "$context.identity.cognitoAuthenticationProvider",
    "identity_cognitoAuthenticationType": "$context.identity.cognitoAuthenticationType",
    "identity_cognitoIdentityId": "$context.identity.cognitoIdentityId",
    "identity_cognitoIdentityPoolId": "$context.identity.cognitoIdentityPoolId",
    "identity_principalOrgId": "$context.identity.principalOrgId",
    "identity_clientCert_clientCertPem": "$context.identity.clientCert.clientCertPem",
    "identity_clientCert_subjectDN": "$context.identity.clientCert.subjectDN",
    "identity_clientCert_issuerDN": "$context.identity.clientCert.issuerDN",
    "identity_clientCert_serialNumber": "$context.identity.clientCert.serialNumber",
    "identity_clientCert_validity_notBefore": "$context.identity.clientCert.validity.notBefore",
    "identity_clientCert_validity_notAfter": "$context.identity.clientCert.validity.notAfter",
    "identity_sourceIp": "$context.identity.sourceIp",
    "identity_user": "$context.identity.user",
    "identity_userAgent": "$context.identity.userAgent",
    "identity_userArn": "$context.identity.userArn",
    "integration_error": "$context.integration.error",
    "integration_integrationStatus": "$context.integration.integrationStatus",
    "integration_latency": "$context.integration.latency",
    "integration_requestId": "$context.integration.requestId",
    "integration_status": "$context.integration.status",
    "integrationErrorMessage": "$context.integrationErrorMessage",
    "integrationLatency": "$context.integrationLatency",
    "integrationStatus": "$context.integrationStatus",
    "path": "$context.path",
    "protocol": "$context.protocol",
    "requestId": "$context.requestId",
    "requestTime": "$context.requestTime",
    "requestTimeEpoch": "$context.requestTimeEpoch",
    "responseLatency": "$context.responseLatency",
    "responseLength": "$context.responseLength",
    "routeKey": "$context.routeKey",
    "stage": "$context.stage",
    "status": "$context.status"
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
