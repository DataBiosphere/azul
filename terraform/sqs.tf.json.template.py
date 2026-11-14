import json

from azul import (
    config,
)
from azul.infra.terraform import (
    emit_tf,
)

emit_tf(
    {
        'resource': [
            {
                'aws_sqs_queue': {
                    **{
                        config.notifications_queue.derive(retry=retry).unqual_name: {
                            'name': config.notifications_queue.derive(retry=retry).name,
                            'visibility_timeout_seconds': config.contribution_lambda_timeout(retry=retry) + 10,
                            'message_retention_seconds': 7 * 24 * 60 * 60,
                            'redrive_policy': json.dumps({
                                'maxReceiveCount': 9 if retry else 1,
                                'deadLetterTargetArn': '${aws_sqs_queue.%s.arn}'
                                                       % config.notifications_queue.derive(retry=not retry,
                                                                                           fail=retry).unqual_name
                            })
                        }
                        for retry in (False, True)
                    },
                    **{
                        config.tallies_queue.derive(retry=retry).unqual_name: {
                            'name': config.tallies_queue.derive(retry=retry).name,
                            'fifo_queue': True,
                            # High throughput mode lifts the per-queue limit on
                            # sends, which throttled the queueing of tallies for
                            # large bundles. Messages are grouped by entity and
                            # their deduplication IDs are random.
                            'deduplication_scope': 'messageGroup',
                            'fifo_throughput_limit': 'perMessageGroupId',
                            'delay_seconds': config.opensearch_refresh_interval + 9,
                            'visibility_timeout_seconds': config.aggregation_lambda_timeout(retry=retry) + 10,
                            'message_retention_seconds': 7 * 24 * 60 * 60,
                            'redrive_policy': json.dumps({
                                'maxReceiveCount': 9 if retry else 1,
                                'deadLetterTargetArn': '${aws_sqs_queue.%s.arn}'
                                                       % config.tallies_queue.derive(retry=not retry,
                                                                                     fail=retry).unqual_name
                            })
                        }
                        for retry in (False, True)
                    },
                    config.notifications_queue.to_fail.unqual_name: {
                        'name': config.notifications_queue.to_fail.name,
                        'message_retention_seconds': 14 * 24 * 60 * 60,
                    },
                    config.tallies_queue.to_fail.unqual_name: {
                        'fifo_queue': True,
                        'name': config.tallies_queue.to_fail.name,
                        'message_retention_seconds': 14 * 24 * 60 * 60,
                    },
                    **(
                        {
                            config.mirror_queue.unqual_name: {
                                'name': config.mirror_queue.name,
                                'fifo_queue': True,
                                'message_retention_seconds': 7 * 24 * 60 * 60,
                                'visibility_timeout_seconds': config.mirror_lambda_timeout + 10,
                                'redrive_policy': json.dumps({
                                    'maxReceiveCount': 10,
                                    'deadLetterTargetArn': '${aws_sqs_queue.%s.arn}'
                                                           % config.mirror_queue.to_fail.unqual_name
                                })
                            },
                            config.mirror_queue.to_fail.unqual_name: {
                                'name': config.mirror_queue.to_fail.name,
                                'fifo_queue': True,
                                'message_retention_seconds': 14 * 24 * 60 * 60,
                            }
                        }
                        if config.enable_mirroring else
                        {}
                    )
                }
            }
        ]
    }
)
