from utils.template import emit, env

emit(
    {
        "data": [
            {
                "aws_caller_identity": {
                    "current": {}
                }
            },
            {
                "aws_region": {
                    "current": {}
                }
            },
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
                                    "arn:aws:es:${local.region}:${local.account_id}:domain/" + env.AZUL_ES_DOMAIN + "/*"
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
                                    "arn:aws:es:${local.region}:${local.account_id}:domain/" + env.AZUL_ES_DOMAIN + "/*"
                                ]
                            }
                        ]
                    }
                }
            }
        ],
        "locals": {
            "account_id": "${data.aws_caller_identity.current.account_id}",
            "region": "${data.aws_region.current.name}"
        },
        "resource": [
            {
                "aws_cloudwatch_log_group": {
                    "dss_index_log": {
                        "name": "/aws/aes/domains/" + env.AZUL_ES_DOMAIN + "/index-logs",
                        "retention_in_days": 90
                    }
                }
            },
            {
                "aws_cloudwatch_log_group": {
                    "dss_search_log": {
                        "name": "/aws/aes/domains/" + env.AZUL_ES_DOMAIN + "/search-logs",
                        "retention_in_days": 90
                    }
                }
            },
            {
                "aws_cloudwatch_log_resource_policy": {
                    "dss_es_cloudwatch_policy": {
                        "policy_document": "${data.aws_iam_policy_document.dss_es_cloudwatch_policy_document.json}",
                        "policy_name": env.AZUL_ES_DOMAIN
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
                            "instance_count": env.AZUL_ES_INSTANCE_COUNT,
                            "instance_type": env.AZUL_ES_INSTANCE_TYPE
                        },
                        "count": 1 if env.AZUL_ES_DOMAIN else 0,
                        "domain_name": env.AZUL_ES_DOMAIN,
                        "ebs_options": {
                            "ebs_enabled": "true",
                            "volume_size": env.AZUL_ES_VOLUME_SIZE,
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
    }
)
