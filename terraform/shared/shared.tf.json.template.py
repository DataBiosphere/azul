import json

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.terraform import (
    emit_tf,
    provider_fragment,
    block_public_s3_bucket_access,
)

emit_tf(block_public_s3_bucket_access({
    'resource': {
        'aws_s3_bucket': {
            'shared_cloudtrail': {
                **provider_fragment(config.cloudtrail_s3_bucket_region),
                'bucket': f'edu-ucsc-gi-{aws.account_name}-cloudtrail',
                'lifecycle': {
                    'prevent_destroy': True
                }
            },
            'versioned': {
                'bucket': config.versioned_bucket,
                'lifecycle': {
                    'prevent_destroy': True
                }
            },
            'aws_config': {
                'bucket': aws.qualified_bucket_name(config.aws_config_term),
                'lifecycle': {
                    'prevent_destroy': True
                }
            },
            'logs': {
                'bucket': aws.qualified_bucket_name(config.logs_term),
                'lifecycle': {
                    'prevent_destroy': True
                }
            }
        },
        'aws_s3_bucket_policy': {
            'shared_cloudtrail': {
                **provider_fragment(config.cloudtrail_s3_bucket_region),
                'bucket': '${aws_s3_bucket.shared_cloudtrail.id}',
                'policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': 'cloudtrail.amazonaws.com'
                            },
                            'Action': 's3:GetBucketAcl',
                            'Resource': '${aws_s3_bucket.shared_cloudtrail.arn}'
                        },
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': 'cloudtrail.amazonaws.com'
                            },
                            'Action': 's3:PutObject',
                            'Resource': '${aws_s3_bucket.shared_cloudtrail.arn}/AWSLogs/'
                                        f'{config.aws_account_id}/*',
                            'Condition': {
                                'StringEquals': {
                                    's3:x-amz-acl': 'bucket-owner-full-control'
                                }
                            }
                        }
                    ]
                })
            },
            'aws_config': {
                # https://docs.aws.amazon.com/config/latest/developerguide/s3-bucket-policy.html
                'bucket': '${aws_s3_bucket.aws_config.id}',
                'policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': 'config.amazonaws.com'
                            },
                            'Action': ['s3:GetBucketAcl', 's3:ListBucket'],
                            'Resource': '${aws_s3_bucket.aws_config.arn}',
                            'Condition': {
                                'StringEquals': {
                                    'AWS:SourceAccount': config.aws_account_id
                                }
                            }
                        },
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': 'config.amazonaws.com'
                            },
                            'Action': 's3:PutObject',
                            'Resource': '${aws_s3_bucket.aws_config.arn}'
                                        f'/*/AWSLogs/{config.aws_account_id}/Config/*',
                            'Condition': {
                                'StringEquals': {
                                    's3:x-amz-acl': 'bucket-owner-full-control',
                                    'AWS:SourceAccount': config.aws_account_id
                                }
                            }
                        }
                    ]
                })
            },
            'logs': {
                'bucket': '${aws_s3_bucket.logs.id}',
                'policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        *aws.elb_access_log_bucket_policy(
                            bucket_arn='${aws_s3_bucket.logs.arn}',
                            path_prefix=config.alb_access_log_path_prefix('*', deployment='*')
                        ),
                        *aws.s3_access_log_bucket_policy(
                            source_bucket_arn='arn:aws:s3:::*',
                            target_bucket_arn='${aws_s3_bucket.logs.arn}',
                            path_prefix=config.s3_access_log_path_prefix('*', deployment='*')
                        )
                    ]
                })
            }
        },
        'aws_s3_bucket_lifecycle_configuration': {
            'versioned': {
                'bucket': '${aws_s3_bucket.versioned.id}',
                'rule': {
                    'id': 'expire-tag',
                    'status': 'Enabled',
                    'filter': {
                        'tag': {
                            'key': 'expires',
                            'value': 'true'
                        }
                    },
                    'noncurrent_version_expiration': {
                        'noncurrent_days': 30
                    }
                }
            },
            'logs': {
                'bucket': '${aws_s3_bucket.logs.id}',
                'rule': {
                    'id': 'expire',
                    'status': 'Enabled',
                    'filter': {
                    },
                    'expiration': {
                        'days': 90
                    }
                }
            }
        },
        'aws_s3_bucket_versioning': {
            'versioned': {
                'bucket': '${aws_s3_bucket.versioned.id}',
                'versioning_configuration': {
                    'status': 'Enabled'
                }
            }
        },
        'aws_cloudtrail': {
            'shared': {
                **provider_fragment(config.cloudtrail_trail_region),
                'name': 'azul-shared',
                's3_bucket_name': '${aws_s3_bucket.shared_cloudtrail.id}',
                'enable_log_file_validation': True,
                'is_multi_region_trail': True,
                'cloud_watch_logs_group_arn': '${aws_cloudwatch_log_group.cloudtrail.arn}:*',
                'cloud_watch_logs_role_arn': '${aws_iam_role.cloudtrail.arn}'
            }
        },
        'aws_cloudwatch_log_group': {
            'cloudtrail': {
                **provider_fragment(config.cloudtrail_trail_region),
                'name': config.qualified_resource_name('cloudtrail'),
                'retention_in_days': config.audit_log_retention_days
            }
        },
        'aws_iam_role': {
            'api_gateway': {
                'name': 'azul-api_gateway',
                'assume_role_policy': json.dumps(
                    {
                        'Version': '2012-10-17',
                        'Statement': [
                            {
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'apigateway.amazonaws.com'
                                },
                                'Action': 'sts:AssumeRole'
                            }
                        ]
                    }
                )
            },
            'aws_config': {
                'name': 'azul-aws_config',
                'assume_role_policy': json.dumps(
                    {
                        'Version': '2012-10-17',
                        'Statement': [
                            {
                                'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'config.amazonaws.com'
                                }
                            }
                        ]
                    }
                )
            },
            'cloudtrail': {
                'name': 'azul-cloudtrail',
                'assume_role_policy': json.dumps(
                    {
                        'Version': '2012-10-17',
                        'Statement': [
                            {
                                'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'cloudtrail.amazonaws.com',
                                }
                            }
                        ]
                    }
                )
            }
        },
        'aws_iam_role_policy': {
            'api_gateway': {
                'name': 'azul-api_gateway',
                'role': '${aws_iam_role.api_gateway.id}',
                'policy': json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:DescribeLogGroups",
                                "logs:DescribeLogStreams",
                                "logs:PutLogEvents",
                                "logs:GetLogEvents",
                                "logs:FilterLogEvents"
                            ],
                            "Resource": "*"
                        }
                    ]
                })
            },
            'aws_config': {
                'name': 'azul-aws_config',
                'role': '${aws_iam_role.aws_config.id}',
                'policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Action': [
                                's3:*'
                            ],
                            'Effect': 'Allow',
                            'Resource': [
                                '${aws_s3_bucket.aws_config.arn}',
                                '${aws_s3_bucket.aws_config.arn}/*'
                            ]
                        }
                    ]
                })
            },
            'cloudtrail': {
                'name': 'azul-cloudtrail',
                'role': '${aws_iam_role.cloudtrail.id}',
                # Adapted from https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-required-policy-for-cloudwatch-logs.html
                'policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': [
                                'logs:CreateLogStream',
                                'logs:PutLogEvents'
                            ],
                            'Resource': [
                                '${aws_cloudwatch_log_group.cloudtrail.arn}:*'
                            ]
                        }
                    ]
                })
            }
        },
        "aws_iam_service_linked_role": {
            "opensearch": {
                "aws_service_name": "opensearchservice.amazonaws.com"
            }
        },
        'aws_api_gateway_account': {
            'shared': {
                'cloudwatch_role_arn': '${aws_iam_role.api_gateway.arn}'
            }
        },
        'aws_config_configuration_recorder': {
            'shared': {
                'name': config.qualified_resource_name(config.aws_config_term),
                'role_arn': '${aws_iam_role.aws_config.arn}',
                'recording_group': {
                    'all_supported': True,
                    'include_global_resource_types': True
                }
            }
        },
        'aws_config_configuration_recorder_status': {
            'shared': {
                'name': '${aws_config_configuration_recorder.shared.name}',
                'is_enabled': True,
                'depends_on': [
                    'aws_config_delivery_channel.shared'
                ]
            }
        },
        'aws_iam_role_policy_attachment': {
            'aws_config': {
                'role': '${aws_iam_role.aws_config.name}',
                'policy_arn': 'arn:aws:iam::aws:policy/service-role/AWS_ConfigRole'
            }
        },
        'aws_config_delivery_channel': {
            'shared': {
                'name': config.qualified_resource_name(config.aws_config_term),
                's3_bucket_name': '${aws_s3_bucket.aws_config.bucket}',
                'depends_on': [
                    'aws_config_configuration_recorder.shared'
                ]
            }
        },
        'aws_guardduty_detector': {
            'shared': {
                'enable': True,
                # All data sources are enabled in a new detector by default.
                'datasources': {
                    'kubernetes': {
                        'audit_logs': {
                            'enable': False
                        }
                    }
                }
            }
        },
        'aws_securityhub_account': {
            'shared': {}

        },
        'aws_securityhub_finding_aggregator': {
            'shared': {
                'linking_mode': 'ALL_REGIONS',
                'depends_on': [
                    'aws_securityhub_account.shared'
                ]
            }
        },
        'aws_securityhub_standards_subscription': {
            'best_practices': {
                'standards_arn': 'arn:aws:securityhub:us-east-1::standards'
                                 '/aws-foundational-security-best-practices/v/1.0.0',
                'depends_on': [
                    'aws_securityhub_account.shared'
                ]
            },
            'cis': {
                'standards_arn': 'arn:aws:securityhub:::ruleset'
                                 '/cis-aws-foundations-benchmark/v/1.2.0',
                'depends_on': [
                    'aws_securityhub_account.shared'
                ]
            }
        }
    }
}))
