import json

from azul import (
    config,
)
from azul.deployment import (
    aws,
    emit_tf,
)

emit_tf(None if config.disable_monitoring else {
    "resource": [
        *([] if config.share_es_domain else [
            {
                "aws_cloudwatch_metric_alarm": {
                    "CPUUtilization": {
                        "alarm_name": config.es_domain + "-CPUUtilization",
                        "actions_enabled": True,
                        "comparison_operator": "GreaterThanOrEqualToThreshold",
                        "evaluation_periods": "2",
                        "metric_name": "CPUUtilization",
                        "namespace": "AWS/ES",
                        "period": "3600",
                        "statistic": "Average",
                        "threshold": "85",
                        "alarm_description": json.dumps({
                            "slack_channel": "dcp-ops-alerts",
                            "description": config.es_domain + " CPUUtilization alarm"
                        }),
                        "dimensions": {
                            "ClientId": aws.account,
                            "DomainName": config.es_domain
                        },
                        "alarm_actions": [
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                        ],
                        "ok_actions": [
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                        ],
                    }
                }
            },
            {
                "aws_cloudwatch_metric_alarm": {
                    "FreeStorageSpace": {
                        "alarm_name": config.es_domain + "-FreeStorageSpace",
                        "actions_enabled": True,
                        "comparison_operator": "LessThanOrEqualToThreshold",
                        "evaluation_periods": "1",
                        "metric_name": "FreeStorageSpace",
                        "namespace": "AWS/ES",
                        "period": "300",
                        "statistic": "Average",
                        "threshold": "14000",
                        "alarm_description": json.dumps({
                            "slack_channel": "dcp-ops-alerts",
                            "description": config.es_domain + " FreeStorageSpace alarm"
                        }),
                        "dimensions": {
                            "ClientId": aws.account,
                            "DomainName": config.es_domain
                        },
                        "alarm_actions": [
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                        ],
                        "ok_actions": [
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                        ],

                    }
                }
            },
            {
                "aws_cloudwatch_metric_alarm": {
                    "JVMMemoryPressure": {
                        "alarm_name": config.es_domain + "-JVMMemoryPressure",
                        "actions_enabled": True,
                        "comparison_operator": "GreaterThanOrEqualToThreshold",
                        "evaluation_periods": "1",
                        "metric_name": "JVMMemoryPressure",
                        "namespace": "AWS/ES",
                        "period": "300",
                        "statistic": "Minimum",
                        "threshold": "65",
                        "alarm_description": json.dumps({
                            "slack_channel": "dcp-ops-alerts",
                            "description": config.es_domain + " JVMMemoryPressure alarm"
                        }),
                        "dimensions": {
                            "ClientId": aws.account,
                            "DomainName": config.es_domain
                        },
                        "alarm_actions": [
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                        ],
                        "ok_actions": [
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                        ],
                    }
                }
            }
        ]),
        {
            "aws_cloudwatch_metric_alarm": {
                "azul_health": {
                    "alarm_name": f"azul-{config.deployment_stage}",
                    "actions_enabled": True,
                    "comparison_operator": "LessThanThreshold",
                    "evaluation_periods": "1",
                    "metric_name": "HealthCheckStatus",
                    "namespace": "AWS/Route53",
                    "period": "120",
                    "statistic": "Minimum",
                    "threshold": "1.0",
                    "alarm_description": json.dumps({
                        "slack_channel": "azul-dev",
                        "environment": config.deployment_stage,
                        "description": f"azul-{config.deployment_stage} HealthCheckStatus alarm"
                    }),
                    "alarm_actions": [
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                    ],
                    "ok_actions": [
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                    ],
                    "dimensions": {
                        "HealthCheckId": "${aws_route53_health_check.composite-azul.id}",
                    }
                },
                "data_portal_health": {
                    "alarm_name": f"data-browser-{config.deployment_stage}",
                    "actions_enabled": True,
                    "comparison_operator": "LessThanThreshold",
                    "evaluation_periods": "1",
                    "metric_name": "HealthCheckStatus",
                    "namespace": "AWS/Route53",
                    "period": "120",
                    "statistic": "Minimum",
                    "threshold": "1.0",
                    "alarm_description": json.dumps({
                        "slack_channel": "data-browser",
                        "environment": config.deployment_stage,
                        "description": f"data-browser-{config.deployment_stage} HealthCheckStatus alarm"
                    }),
                    "alarm_actions": [
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                    ],
                    "ok_actions": [
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                    ],
                    "dimensions": {
                        "HealthCheckId": "${aws_route53_health_check.composite-portal.id}",
                    }
                },
                **{
                    f"{queue.replace('.', '-')}-queue": {
                        "alarm_name": f"{queue}-message-count",
                        "actions_enabled": True,
                        "comparison_operator": "GreaterThanThreshold",
                        "evaluation_periods": "1",
                        "metric_name": "ApproximateNumberOfMessagesVisible",
                        "namespace": "AWS/SQS",
                        "period": "300",  # SQS pushes metrics at most every 5 min, lower periods wouldn't make sense
                        "statistic": "Maximum",
                        "threshold": "0.0",
                        "alarm_description": json.dumps({
                            "slack_channel": "azul-dev",
                            "environment": config.deployment_stage,
                            "description": f"{queue} ApproximateNumberOfMessagesVisible alarm"
                        }),
                        "dimensions": {
                            "QueueName": queue
                        },
                        "alarm_actions": [
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                        ],
                        "ok_actions": [
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                            f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                        ],
                    } for queue in config.fail_queue_names
                }
            }
        }
    ]
})
