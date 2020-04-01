import json

from azul import config
from azul.deployment import emit_tf

emit_tf(
    {
        "resource": [
            {
                "aws_sqs_queue": {
                    "notification_queue": {
                        "name": config.notify_queue_name,
                        "visibility_timeout_seconds": config.indexer_lambda_timeout + 10,
                        "message_retention_seconds": 24 * 60 * 60,
                        "redrive_policy": json.dumps({
                            "maxReceiveCount": 10,
                            "deadLetterTargetArn": "${aws_sqs_queue.failure_queue.arn}"
                        })
                    },
                    "document_queue": {
                        "name": config.document_queue_name,
                        "fifo_queue": True,
                        "delay_seconds": config.es_refresh_interval + 9,
                        "visibility_timeout_seconds": config.indexer_lambda_timeout + 10,
                        "message_retention_seconds": 24 * 60 * 60,
                        "redrive_policy": json.dumps({
                            "maxReceiveCount": 10,
                            "deadLetterTargetArn": "${aws_sqs_queue.fifo_failure_queue.arn}"
                        })
                    },
                    "failure_queue": {
                        "name": config.qualified_resource_name('fail'),
                        "message_retention_seconds": 14 * 24 * 60 * 60,
                    },
                    "fifo_failure_queue": {
                        "fifo_queue": True,
                        "name": config.qualified_resource_name('fail', suffix='.fifo'),
                        "message_retention_seconds": 14 * 24 * 60 * 60,
                    }
                }
            }
        ]
    }
)
