import json

from azul import config
from azul.deployment import (
    aws,
    emit_tf,
)


def cart_item_states():
    return {
        "WriteBatch": {
            "Type": "Task",
            "Resource": aws.get_lambda_arn(config.service_name, config.cart_item_write_lambda_basename),
            "Next": "NextBatch",
            "ResultPath": "$.write_result"
        },
        "NextBatch": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.write_result.count",
                    "NumericEquals": 0,
                    "Next": "SuccessState",
                }
            ],
            "Default": "WriteBatch"
        },
        "SuccessState": {
            "Type": "Succeed"
        }
    }


emit_tf({
    "resource": {
        "aws_iam_role": {
            "state_machine_iam_role": {
                "name": config.qualified_resource_name("statemachine"),
                "assume_role_policy": json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "",
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "states.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                }),
                **aws.permissions_boundary_tf
            }
        },
        "aws_iam_role_policy": {
            "state_machine_iam_policy": {
                "name": config.qualified_resource_name("statemachine"),
                "role": "${aws_iam_role.state_machine_iam_role.id}",
                "policy": json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "lambda:InvokeFunction"
                            ],
                            "Resource": [
                                aws.get_lambda_arn(config.service_name, config.manifest_lambda_basename),
                                aws.get_lambda_arn(config.service_name, config.cart_item_write_lambda_basename),
                                aws.get_lambda_arn(config.service_name, config.cart_export_dss_push_lambda_basename)
                            ]
                        }
                    ]
                })
            }
        },
        "aws_sfn_state_machine": {
            "manifest_state_machine": {
                "name": config.manifest_state_machine_name,
                "role_arn": "${aws_iam_role.state_machine_iam_role.arn}",
                "definition": json.dumps({
                    "StartAt": "WriteManifest",
                    "States": {
                        "WriteManifest": {
                            "Type": "Task",
                            "Resource": aws.get_lambda_arn(config.service_name, config.manifest_lambda_basename),
                            "End": True
                        }
                    }
                }, indent=2)
            },
            "cart_item_state_machine": {
                "name": config.cart_item_state_machine_name,
                "role_arn": "${aws_iam_role.state_machine_iam_role.arn}",
                "definition": json.dumps({
                    "StartAt": "WriteBatch",
                    "States": cart_item_states()
                }, indent=2)
            },
            "cart_export_state_machine": {
                "name": config.cart_export_state_machine_name,
                "role_arn": "${aws_iam_role.state_machine_iam_role.arn}",
                "definition": json.dumps({
                    "StartAt": "SendToCollectionAPI",
                    "States": {
                        "SendToCollectionAPI": {
                            "Type": "Task",
                            "Resource": aws.get_lambda_arn(config.service_name,
                                                           config.cart_export_dss_push_lambda_basename),
                            "Next": "NextBatch",
                            "ResultPath": "$"
                        },
                        "NextBatch": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.resumable",
                                    "BooleanEquals": False,
                                    "Next": "SuccessState"
                                }
                            ],
                            "Default": "SendToCollectionAPI"
                        },
                        "SuccessState": {
                            "Type": "Succeed"
                        }
                    }
                }, indent=2)
            }
        }
    }
})
