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
)

emit_tf({
    'resource': {
        'aws_s3_bucket': {
            'shared_cloudtrail': {
                **provider_fragment(config.cloudtrail_s3_bucket_region),
                'bucket': f'edu-ucsc-gi-{aws.account_name}-cloudtrail'
            },
            'versioned': {
                'bucket': config.versioned_bucket,
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
                'is_multi_region_trail': True
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
            }
        },
        'aws_api_gateway_account': {
            'shared': {
                'cloudwatch_role_arn': '${aws_iam_role.api_gateway.arn}'
            }
        }
    }
})
