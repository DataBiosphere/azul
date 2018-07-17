from azul import config
from azul.template import emit

emit(
    {
        "resource": [
            {
                "aws_sqs_queue": {
                    "notification_queue": {
                        "name": f"azul-notify-{config.deployment_stage}",
                        "message_retention_seconds": 24 * 60 * 60,
                    }
                }
            }
        ]
    }
)
