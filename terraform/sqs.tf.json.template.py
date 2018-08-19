import json

from azul import config
from azul.template import emit

emit(
    {
        "resource": [
            {
                "aws_sqs_queue": {
                    "notification_queue": {
                        "name": f"azul-notify-{config.deployment_stage}",
                        "visibility_timeout_seconds": 300,  # must match Lambda timeout
                        "message_retention_seconds": 24 * 60 * 60,
                        "redrive_policy": json.dumps({
                            "maxReceiveCount": 10,
                            "deadLetterTargetArn": "${aws_sqs_queue.failure_queue.arn}"
                        })
                    },
                    "failure_queue": {
                        "name": f"azul-fail-{config.deployment_stage}",
                        "message_retention_seconds": 14 * 24 * 60 * 60,
                    }
                }
            }
        ]
    }
)
