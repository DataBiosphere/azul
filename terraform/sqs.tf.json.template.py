import json

from azul import config
from azul.deployment import emit_tf

emit_tf(
    {
        "resource": [
            {
                "aws_sqs_queue": {
                    config.unqual_notifications_queue_name(): {
                        "name": config.notifications_queue_name(),
                        "visibility_timeout_seconds": config.indexer_lambda_timeout + 10,
                        "message_retention_seconds": 24 * 60 * 60,
                        "redrive_policy": json.dumps({
                            "maxReceiveCount": 10,
                            "deadLetterTargetArn": "${aws_sqs_queue.%s.arn}"
                                                   % config.unqual_notifications_queue_name(fail=True)
                        })
                    },
                    **{
                        config.unqual_tallies_queue_name(retry=retry): {
                            "name": config.tallies_queue_name(retry=retry),
                            "fifo_queue": True,
                            "delay_seconds": config.es_refresh_interval + 9,
                            "visibility_timeout_seconds": config.indexer_lambda_timeout + 10,
                            "message_retention_seconds": 24 * 60 * 60,
                            "redrive_policy": json.dumps({
                                "maxReceiveCount": 9 if retry else 1,
                                "deadLetterTargetArn": "${aws_sqs_queue.%s.arn}"
                                                       % config.unqual_tallies_queue_name(retry=not retry, fail=retry)
                            })
                        }
                        for retry in (False, True)
                    },
                    config.unqual_notifications_queue_name(fail=True): {
                        "name": config.notifications_queue_name(fail=True),
                        "message_retention_seconds": 14 * 24 * 60 * 60,
                    },
                    config.unqual_tallies_queue_name(fail=True): {
                        "fifo_queue": True,
                        "name": config.tallies_queue_name(fail=True),
                        "message_retention_seconds": 14 * 24 * 60 * 60,
                    }
                }
            }
        ]
    }
)
