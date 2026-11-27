import json

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.infra.terraform import (
    emit_tf,
)
from azul.modules import (
    load_app_module,
)
from azul.service.manifest_controller import (
    manifest_state_key,
)

service = load_app_module('service')

# In blue-green mode, the alias has none of these resources
#
emit_tf(None if config.deployment.is_alias else {
    "resource": {
        "aws_iam_role": {
            "states": {
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
            "states": {
                "name": config.qualified_resource_name("statemachine"),
                "role": "${aws_iam_role.states.id}",
                "policy": json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "lambda:InvokeFunction"
                            ],
                            "Resource": [
                                aws.get_lambda_arn(config.service_name, service.generate_manifest.name),
                            ]
                        }
                    ]
                })
            }
        },
        "aws_sfn_state_machine": {
            "manifest": {
                "name": config.qualified_resource_name(config.manifest_sfn),
                "role_arn": "${aws_iam_role.states.arn}",
                "definition": json.dumps({
                    "StartAt": "Loop",
                    "States": {
                        "Loop": {
                            "Type": "Choice",
                            "Default": "Manifest",
                            "Choices": [
                                {
                                    "Variable": f"$.{manifest_state_key}",
                                    "IsPresent": True,
                                    "Next": "Done"
                                }
                            ],
                        },
                        "Manifest": {
                            "Type": "Task",
                            "Resource": aws.get_lambda_arn(config.service_name, service.generate_manifest.name),
                            "Next": "Loop"
                        },
                        "Done": {
                            "Type": "Succeed"
                        }
                    }
                }, indent=2)
            }
        }
    }
})
