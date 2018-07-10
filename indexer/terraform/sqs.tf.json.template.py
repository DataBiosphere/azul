from utils.template import emit, env

emit(
    {
        "resource": [
            {
                "aws_sqs_queue": {
                    "notification_queue": {
                        "name": f"azul-notify-{env.AZUL_DEPLOYMENT_STAGE}",
                        "message_retention_seconds": 24 * 60 * 60,
                    }
                }
            }
        ]
    }
)
