import json

from azul import (
    config,
)
from azul.terraform import (
    emit_tf,
)

logs = {
    'index': ('INDEX_SLOW_LOGS', True),
    'search': ('SEARCH_SLOW_LOGS', True),
    'error': ('ES_APPLICATION_LOGS', True)
}

domain = config.es_domain

emit_tf(None if config.share_es_domain else {
    "resource": [
        *({
            "aws_cloudwatch_log_group": {
                f"{log}_log": {
                    "name": f"/aws/aes/domains/{domain}/{log}-logs",
                    "retention_in_days": 30 if log == 'error' else 1827
                }
            }
        } for log in logs.keys()),
        {
            "aws_cloudwatch_log_resource_policy": {
                "index": {
                    "policy_name": domain,
                    "policy_document": json.dumps(
                        {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Principal": {
                                        "Service": "es.amazonaws.com"
                                    },
                                    "Action": [
                                        "logs:PutLogEvents",
                                        "logs:CreateLogStream"
                                    ],
                                    "Resource": [
                                        "${aws_cloudwatch_log_group." + log + "_log.arn}" for log in logs.keys()
                                    ]
                                }
                            ]
                        }
                    )
                }
            }
        },
        {
            "aws_elasticsearch_domain": {
                "index": {
                    "access_policies": json.dumps({
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {
                                    "AWS": "arn:aws:iam::${local.account_id}:root"
                                },
                                "Action": "es:*",
                                "Resource": "arn:aws:es:${local.region}:${local.account_id}:domain/" + domain + "/*"
                            },
                            {
                                "Effect": "Allow",
                                "Principal": {
                                    "AWS": "*"
                                },
                                "Action": "es:*",
                                "Resource": "arn:aws:es:${local.region}:${local.account_id}:domain/" + domain + "/*",
                                "Condition": {
                                    "IpAddress": {
                                        "aws:SourceIp": []
                                    }
                                }
                            }
                        ]
                    }),
                    "advanced_options": {
                        "rest.action.multi.allow_explicit_index": "true"
                    },
                    "cluster_config": {
                        "instance_count": config.es_instance_count,
                        "instance_type": config.es_instance_type
                    },
                    "domain_name": domain,
                    "ebs_options": {
                        "ebs_enabled": "true",
                        "volume_size": config.es_volume_size,
                        "volume_type": "gp2"
                    },
                    "elasticsearch_version": "6.8",
                    "log_publishing_options": [
                        {
                            "cloudwatch_log_group_arn": "${aws_cloudwatch_log_group." + log + "_log.arn}",
                            "enabled": "true" if enabled else "false",
                            "log_type": log_type
                        } for log, (log_type, enabled) in logs.items()
                    ],
                    "snapshot_options": {
                        "automated_snapshot_start_hour": 8  # midnight PST
                    }
                }
            }
        } if domain else {}
    ]
})
