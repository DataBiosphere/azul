import json
import shlex
from typing import (
    NamedTuple,
)

from azul import (
    config,
    require,
)
from azul.deployment import (
    aws,
)
from azul.terraform import (
    block_public_s3_bucket_access,
    emit_tf,
    provider_fragment,
)

require(config.cloudtrail_s3_bucket_region == config.region
        or config.deployment_stage == 'dev',  # grand-father in an exception for `dev`
        'The Cloudtrail bucket must reside in the default region',
        config.cloudtrail_s3_bucket_region, config.region)


class CloudTrailAlarm(NamedTuple):
    name: str
    statistic: str
    filter_pattern: str

    @property
    def metric_name(self) -> str:
        return f'{self.name}_metric'


cis_alarms = [
    # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-3.1
    CloudTrailAlarm(name='api_unauthorized',
                    statistic='Average',
                    filter_pattern='{($.errorCode="*UnauthorizedOperation") || ($.errorCode="AccessDenied*")}'),
    # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-3.2
    CloudTrailAlarm(name='console_no_mfa',
                    statistic='Sum',
                    filter_pattern='{ ($.eventName = "ConsoleLogin") && ($.additionalEventData.MFAUsed != "Yes") && '
                                   '($.userIdentity.type = "IAMUser") && '
                                   '($.responseElements.ConsoleLogin = "Success") }'),
    # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-3.3
    CloudTrailAlarm(name='root_usage',
                    statistic='Sum',
                    filter_pattern='{$.userIdentity.type="Root" && $.userIdentity.invokedBy NOT EXISTS && $.eventType '
                                   '!="AwsServiceEvent"}'),
    # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-3.4
    CloudTrailAlarm(name='iam_policy_change',
                    statistic='Average',
                    filter_pattern='{($.eventName=DeleteGroupPolicy) || ($.eventName=DeleteRolePolicy) || '
                                   '($.eventName=DeleteUserPolicy) || ($.eventName=PutGroupPolicy) || '
                                   '($.eventName=PutRolePolicy) || ($.eventName=PutUserPolicy) || '
                                   '($.eventName=CreatePolicy) || ($.eventName=DeletePolicy) || '
                                   '($.eventName=CreatePolicyVersion) || ($.eventName=DeletePolicyVersion) || '
                                   '($.eventName=AttachRolePolicy) || ($.eventName=DetachRolePolicy) || '
                                   '($.eventName=AttachUserPolicy) || ($.eventName=DetachUserPolicy) || '
                                   '($.eventName=AttachGroupPolicy) || ($.eventName=DetachGroupPolicy)}'),
    # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-3.5
    CloudTrailAlarm(name='cloudtrail_config_change',
                    statistic='Sum',
                    filter_pattern='{($.eventName=CreateTrail) || ($.eventName=UpdateTrail) || '
                                   '($.eventName=DeleteTrail) || ($.eventName=StartLogging) || '
                                   '($.eventName=StopLogging)}'),
    # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-3.8
    CloudTrailAlarm(name='s3_policy_change',
                    statistic='Average',
                    filter_pattern='{($.eventSource=s3.amazonaws.com) && (($.eventName=PutBucketAcl) || '
                                   '($.eventName=PutBucketPolicy) || ($.eventName=PutBucketCors) || '
                                   '($.eventName=PutBucketLifecycle) || ($.eventName=PutBucketReplication) || '
                                   '($.eventName=DeleteBucketPolicy) || ($.eventName=DeleteBucketCors) || '
                                   '($.eventName=DeleteBucketLifecycle) || ($.eventName=DeleteBucketReplication))}'),
    # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-3.12
    CloudTrailAlarm(name='network_gateway_change',
                    statistic='Sum',
                    filter_pattern='{($.eventName=CreateCustomerGateway) || ($.eventName=DeleteCustomerGateway) || '
                                   '($.eventName=AttachInternetGateway) || ($.eventName=CreateInternetGateway) || '
                                   '($.eventName=DeleteInternetGateway) || ($.eventName=DetachInternetGateway)}'),
    # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-3.13
    CloudTrailAlarm(name='route_table_change',
                    statistic='Average',
                    filter_pattern='{($.eventName=CreateRoute) || ($.eventName=CreateRouteTable) || '
                                   '($.eventName=ReplaceRoute) || ($.eventName=ReplaceRouteTableAssociation) || '
                                   '($.eventName=DeleteRouteTable) || ($.eventName=DeleteRoute) || '
                                   '($.eventName=DisassociateRouteTable)}'),
    # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-3.14
    CloudTrailAlarm(name='vpc_change',
                    statistic='Average',
                    filter_pattern='{($.eventName=CreateVpc) || ($.eventName=DeleteVpc) || '
                                   '($.eventName=ModifyVpcAttribute) || ($.eventName=AcceptVpcPeeringConnection) || '
                                   '($.eventName=CreateVpcPeeringConnection) || '
                                   '($.eventName=DeleteVpcPeeringConnection) || '
                                   '($.eventName=RejectVpcPeeringConnection) || ($.eventName=AttachClassicLinkVpc) || '
                                   '($.eventName=DetachClassicLinkVpc) || ($.eventName=DisableVpcClassicLink) || '
                                   '($.eventName=EnableVpcClassicLink)}')
]

emit_tf(block_public_s3_bucket_access({
    'data': {
        'aws_iam_role': {
            f'support_{i}': {
                'name': role
            }
            for i, role in enumerate(config.aws_support_roles)
        }
    },
    'resource': {
        'aws_s3_bucket': {
            # FIXME: Disable original CloudTrail trail
            #        https://github.com/databiosphere/azul/issues/4832
            'shared_cloudtrail': {
                **provider_fragment(config.cloudtrail_s3_bucket_region),
                'bucket': f'edu-ucsc-gi-{aws.account_name}-cloudtrail',
                'lifecycle': {
                    'prevent_destroy': True
                }
            },
            'trail': {
                'bucket': f'edu-ucsc-gi-{aws.account_name}-trail.{aws.region_name}',
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
        **(
            {}
            if config.deployment_stage == 'dev' else
            {
                'aws_s3_bucket_logging': {
                    'trail': {
                        'bucket': '${aws_s3_bucket.trail.id}',
                        'target_bucket': '${aws_s3_bucket.logs.id}',
                        # Other S3 log deliveries, like ELB, implicitly put a slash
                        # after the prefix. S3 doesn't, so we add one explicitly.
                        'target_prefix': config.s3_access_log_path_prefix('cloudtrail') + '/'
                    }
                }
            }
        ),
        'aws_s3_bucket_policy': {
            **{
                bucket: {
                    **provider_fragment(region),
                    'bucket': '${aws_s3_bucket.%s.id}' % bucket,
                    'policy': json.dumps({
                        'Version': '2012-10-17',
                        'Statement': [
                            {
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'cloudtrail.amazonaws.com'
                                },
                                'Action': 's3:GetBucketAcl',
                                'Resource': '${aws_s3_bucket.%s.arn}' % bucket,
                            },
                            {
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'cloudtrail.amazonaws.com'
                                },
                                'Action': 's3:PutObject',
                                'Resource': '${aws_s3_bucket.%s.arn}/AWSLogs/%s/*' % (bucket, config.aws_account_id),
                                'Condition': {
                                    'StringEquals': {
                                        's3:x-amz-acl': 'bucket-owner-full-control'
                                    }
                                }
                            }
                        ]
                    })
                }
                for bucket, region in [
                    ('shared_cloudtrail', config.cloudtrail_s3_bucket_region),
                    ('trail', config.region)
                ]
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
            # FIXME: Disable original CloudTrail trail
            #        https://github.com/databiosphere/azul/issues/4832
            'shared': {
                **provider_fragment(config.cloudtrail_trail_region),
                'name': 'azul-shared',
                's3_bucket_name': '${aws_s3_bucket.shared_cloudtrail.id}',
                'enable_logging': False,
            },
            'trail': {
                'name': config.qualified_resource_name('trail'),
                's3_bucket_name': '${aws_s3_bucket.trail.id}',
                'enable_log_file_validation': True,
                'is_multi_region_trail': True,
                'cloud_watch_logs_group_arn': '${aws_cloudwatch_log_group.trail.arn}:*',
                'cloud_watch_logs_role_arn': '${aws_iam_role.trail.arn}'
            }
        },
        'aws_cloudwatch_log_group': {
            # FIXME: Disable original CloudTrail trail
            #        https://github.com/databiosphere/azul/issues/4832
            'cloudtrail': {
                **provider_fragment(config.cloudtrail_trail_region),
                'name': config.qualified_resource_name('cloudtrail'),
                'retention_in_days': config.audit_log_retention_days
            },
            'trail': {
                'name': config.qualified_resource_name('trail'),
                'retention_in_days': config.audit_log_retention_days
            }
        },
        'aws_cloudwatch_log_metric_filter': {
            a.name: {
                'name': config.qualified_resource_name(a.name, suffix='.filter'),
                'pattern': a.filter_pattern,
                'log_group_name': '${aws_cloudwatch_log_group.trail.name}',
                'metric_transformation': {
                    'name': a.metric_name,
                    'namespace': 'LogMetrics',
                    'value': 1
                }
            }
            for a in cis_alarms
        },
        'aws_cloudwatch_metric_alarm': {
            a.name: {
                'alarm_name': config.qualified_resource_name(a.name, suffix='.alarm'),
                'comparison_operator': 'GreaterThanOrEqualToThreshold',
                'evaluation_periods': 1,
                'metric_name': a.metric_name,
                'namespace': 'LogMetrics',
                'statistic': a.statistic,
                'threshold': 1,
                # The CIS documentation does not specify a period. 5 minutes is
                # the default value when creating the alarm via the console UI.
                'period': 5 * 60,
                'alarm_actions': ['${aws_sns_topic.monitoring.arn}'],
                'ok_actions': ['${aws_sns_topic.monitoring.arn}']
            }
            for a in cis_alarms
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
            'trail': {
                'name': config.qualified_resource_name('trail'),
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
            'trail': {
                'name': config.qualified_resource_name('trail'),
                'role': '${aws_iam_role.trail.id}',
                # noqa https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-required-policy-for-cloudwatch-logs.html
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
                                '${aws_cloudwatch_log_group.trail.arn}:*'
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
            },
            **{
                f'support_{i}': {
                    'role': '${data.aws_iam_role.support_%s.name}' % i,
                    'policy_arn': 'arn:aws:iam::aws:policy/AWSSupportAccess'
                }
                for i, role in enumerate(config.aws_support_roles)
            },
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
        },
        'aws_iam_account_password_policy': {
            'cis': {
                'require_uppercase_characters': True,
                'require_lowercase_characters': True,
                'require_symbols': True,
                'require_numbers': True,
                'minimum_password_length': 14,
                'password_reuse_prevention': 24,
                'max_password_age': 90,
            }
        },
        **(
            {
                'aws_account_alternate_contact': {
                    'security': {
                        **config.security_contact,
                        'alternate_contact_type': 'SECURITY'
                    }
                }
            }
            if config.security_contact else
            {}
        ),
        'aws_sns_topic': {
            'monitoring': {
                'name': aws.monitoring_topic_name
            }
        },
        'aws_sns_topic_subscription': {
            'monitoring': {
                'topic_arn': '${aws_sns_topic.monitoring.arn}',
                # The `email` protocol is only partially supported. Since
                # Terraform cannot confirm or delete pending subscriptions
                # (see link below), we use a separate script for this purpose.
                # https://registry.terraform.io/providers/hashicorp/aws/4.3.0/docs/resources/sns_topic_subscription#protocol-support
                'protocol': 'email',
                'endpoint': config.azul_monitoring_email,
                'provisioner': {
                    'local-exec': {
                        'command': ' '.join(map(shlex.quote, [
                            'python',
                            config.project_root + '/scripts/confirm_sns_subscription.py'
                        ]))
                    }
                }
            }
        }
    }
}))
