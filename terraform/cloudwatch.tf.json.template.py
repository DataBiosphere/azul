from azul.template import emit
from azul import config
from azul.deployment import aws

emit({
    "resource": [
        {
            "aws_cloudwatch_metric_alarm": {
                "CPUUtilization": {
                    "alarm_name": config.es_domain+"-CPUUtilization",
                    "actions_enabled": True,
                    "comparison_operator": "GreaterThanOrEqualToThreshold",
                    "evaluation_periods": "2",
                    "metric_name": "CPUUtilization",
                    "namespace": "AWS/ES",
                    "period": "3600",
                    "statistic": "Average",
                    "threshold": "85",
                    "alarm_description": "This metric monitors ES CPU utilization",
                    "dimensions": {
                        "ClientId": aws.account,
                        "DomainName": config.es_domain
                    },
                    "alarm_actions": [
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                    ]
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
                    "alarm_description": "This metric monitors ES Disk usage",
                    "dimensions": {
                        "ClientId": aws.account,
                        "DomainName": config.es_domain
                    },
                    "alarm_actions": [
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                    ]
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
                    "alarm_description": "This metric monitors memory pressure, should not exceed 65%",
                    "dimensions": {
                        "ClientId": aws.account,
                        "DomainName": config.es_domain
                    },
                    "alarm_actions": [
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:cloudwatch-alarms",
                        f"arn:aws:sns:{aws.region_name}:{aws.account}:dcp-events"
                    ]
                }
            }
        }
    ]
} if config.enable_cloudwatch_alarms else None)

