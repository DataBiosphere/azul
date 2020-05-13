import json

from azul import config
from azul.deployment import emit_tf

emit_tf(
    {
        "resource": [
            {
                "aws_sqs_queue": {
                    "notifications_queue": {
                        "name": config.notifications_queue_name(),
                        "visibility_timeout_seconds": config.indexer_lambda_timeout + 10,
                        "message_retention_seconds": 24 * 60 * 60,
                        "redrive_policy": json.dumps({
                            "maxReceiveCount": 10,
                            "deadLetterTargetArn": "${aws_sqs_queue.notifications_fail_queue.arn}"
                        })
                    },
                    **{
                        "tallies_" + ("retry_" if retry else "") + "queue": {
                            "name": config.tallies_queue_name(retry=retry),
                            "fifo_queue": True,
                            "delay_seconds": config.es_refresh_interval + 9,
                            "visibility_timeout_seconds": config.indexer_lambda_timeout + 10,
                            "message_retention_seconds": 24 * 60 * 60,
                            "redrive_policy": json.dumps({
                                "maxReceiveCount": 9 if retry else 1,
                                "deadLetterTargetArn": "${aws_sqs_queue.%s.arn}" %
                                                       ("tallies_fail_queue" if retry else "tallies_retry_queue")
                            })
                        }
                        for retry in (False, True)
                    },
                    "notifications_fail_queue": {
                        "name": config.notifications_queue_name(fail=True),
                        "message_retention_seconds": 14 * 24 * 60 * 60,
                    },
                    "tallies_fail_queue": {
                        "fifo_queue": True,
                        "name": config.tallies_queue_name(fail=True),
                        "message_retention_seconds": 14 * 24 * 60 * 60,
                    }
                }
            }
        ]
    }
)
