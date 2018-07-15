from utils import config
from utils.template import emit

emit({
    "data": [
        {
            "aws_iam_policy_document": {
                "dss_es_cloudwatch_policy_document": {
                    "statement": [
                        {
                            "actions": [
                                "logs:PutLogEvents",
                                "logs:CreateLogStream"
                            ],
                            "principals": {
                                "identifiers": [
                                    "es.amazonaws.com"
                                ],
                                "type": "Service"
                            },
                            "resources": [
                                "${aws_cloudwatch_log_group.dss_index_log.arn}",
                                "${aws_cloudwatch_log_group.dss_search_log.arn}"
                            ]
                        }
                    ]
                }
            }
        },
        {
            "aws_iam_policy_document": {
                "dss_es_access_policy_document": {
                    "statement": [
                        {
                            "actions": [
                                "es:*"
                            ],
                            "principals": {
                                "identifiers": [
                                    "arn:aws:iam::${local.account_id}:root"
                                ],
                                "type": "AWS"
                            },
                            "resources": [
                                "arn:aws:es:${local.region}:${local.account_id}:domain/" + config.es_domain + "/*"
                            ]
                        },
                        {
                            "actions": [
                                "es:*"
                            ],
                            "condition": {
                                "test": "IpAddress",
                                "values": [],
                                "variable": "aws:SourceIp"
                            },
                            "principals": {
                                "identifiers": [
                                    "*"
                                ],
                                "type": "AWS"
                            },
                            "resources": [
                                "arn:aws:es:${local.region}:${local.account_id}:domain/" + config.es_domain + "/*"
                            ]
                        }
                    ]
                }
            }
        }
    ],
    "resource": [
        {
            "aws_cloudwatch_log_group": {
                "dss_index_log": {
                    "name": "/aws/aes/domains/" + config.es_domain + "/index-logs",
                    "retention_in_days": 90
                }
            }
        },
        {
            "aws_cloudwatch_log_group": {
                "dss_search_log": {
                    "name": "/aws/aes/domains/" + config.es_domain + "/search-logs",
                    "retention_in_days": 90
                }
            }
        },
        {
            "aws_cloudwatch_log_resource_policy": {
                "dss_es_cloudwatch_policy": {
                    "policy_document": "${data.aws_iam_policy_document.dss_es_cloudwatch_policy_document.json}",
                    "policy_name": config.es_domain
                }
            }
        },
        {
            "aws_elasticsearch_domain": {
                "elasticsearch": {
                    "access_policies": "${data.aws_iam_policy_document.dss_es_access_policy_document.json}",
                    "advanced_options": {
                        "rest.action.multi.allow_explicit_index": "true"
                    },
                    "cluster_config": {
                        "instance_count": config.es_instance_count,
                        "instance_type": config.es_instance_type
                    },
                    "count": 1 if config.es_domain else 0,
                    "domain_name": config.es_domain,
                    "ebs_options": {
                        "ebs_enabled": "true",
                        "volume_size": config.es_volume_size,
                        "volume_type": "gp2"
                    },
                    "elasticsearch_version": "5.5",
                    "log_publishing_options": [
                        {
                            "cloudwatch_log_group_arn": "${aws_cloudwatch_log_group.dss_index_log.arn}",
                            "enabled": "true",
                            "log_type": "INDEX_SLOW_LOGS"
                        },
                        {
                            "cloudwatch_log_group_arn": "${aws_cloudwatch_log_group.dss_search_log.arn}",
                            "enabled": "true",
                            "log_type": "SEARCH_SLOW_LOGS"
                        }
                    ],
                    "snapshot_options": {
                        "automated_snapshot_start_hour": 23
                    }
                }
            }
        }
    ]
})
