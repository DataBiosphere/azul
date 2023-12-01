import json
from typing import (
    NamedTuple,
)

from azul import (
    config,
    docker,
)
from azul.deployment import (
    aws,
)
from azul.terraform import (
    block_public_s3_bucket_access,
    emit_tf,
    enable_s3_bucket_inventory,
    vpc,
)


class CloudTrailAlarm(NamedTuple):
    name: str
    statistic: str
    filter_pattern: str

    @property
    def metric_name(self) -> str:
        return f'{self.name}_metric'


def conformance_pack(name: str) -> str:
    path = f'{config.project_root}/terraform/shared/{name}.yaml'
    with open(path) as f:
        body = f.read()
    return body


def paren(s: str) -> str:
    return '(' + s + ')'


cis_alarms = [
    # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-3.1
    CloudTrailAlarm(name='api_unauthorized',
                    statistic='Average',
                    filter_pattern='{' + ' && '.join([
                        paren(' || '.join([
                            '$.errorCode = "*UnauthorizedOperation"',
                            '$.errorCode = "AccessDenied*"',
                        ])),
                        # Filtering on the errorCode alone catches too many false
                        # positives, so we exclude those logs with the additional
                        # conditions below.
                        paren(' || '.join([
                            '$.eventSource != "s3.amazonaws.com"',
                            '$.eventName != "GetObject"',
                            '$.userIdentity.invokedBy != "cloudfront.amazonaws.com"',
                        ])),
                        paren(' || '.join([
                            '$.eventSource != "s3.amazonaws.com"',
                            '$.eventName != "HeadBucket"',
                            '$.userIdentity.invokedBy != "config.amazonaws.com"',
                        ])),
                        paren(' || '.join([
                            '$.eventSource != "s3.amazonaws.com"',
                            '$.eventName != "HeadObject"',
                            '$.userIdentity.invokedBy != "cloudfront.amazonaws.com"',
                        ])),
                        paren(' || '.join([
                            '$.eventSource != "inspector2.amazonaws.com"',
                            '$.eventName != "DescribeOrganizationConfiguration"',
                        ])),
                        paren(' || '.join([
                            '$.eventSource != "elasticloadbalancing.amazonaws.com"',
                            '$.eventName != "DescribeTargetGroupAttributes"',
                            '$.userIdentity.invokedBy != "inspector2.amazonaws.com"',
                        ])),
                        paren(' || '.join([
                            '$.eventSource != "ec2.amazonaws.com"',
                            '$.eventName != "CreateNetworkInterface"',
                        ]))
                    ]) + '}'
                    ),
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
                                   '($.eventName=EnableVpcClassicLink)}'),
    # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-1.1
    CloudTrailAlarm(name='root_user',
                    statistic='Average',
                    filter_pattern='{$.userIdentity.type="Root" && $.userIdentity.invokedBy NOT EXISTS && '
                                   '$.eventType !="AwsServiceEvent"}')
]

tf_config = {
    'data': {
        'aws_iam_role': {
            f'support_{i}': {
                'name': role
            }
            for i, role in enumerate(config.aws_support_roles)
        },
        'aws_vpc': {
            vpc.default_vpc_name: {
                'default': True
            }
        }
    },
    'resource': {
        'aws_default_vpc': {
            vpc.default_vpc_name: {}
        },
        'aws_flow_log': {
            vpc.default_vpc_name: {
                'iam_role_arn': '${aws_iam_role.%s.arn}' % vpc.default_vpc_name,
                'log_destination': '${aws_cloudwatch_log_group.%s.arn}' % vpc.default_vpc_name,
                'log_destination_type': 'cloud-watch-logs',
                'traffic_type': 'ALL',
                # https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/default_vpc#attributes-reference
                # While the `aws_default_vpc` resource doesn't list `.id` as an
                # attribute in the docs, its usage is valid.
                'vpc_id': '${aws_default_vpc.%s.id}' % vpc.default_vpc_name,
            }
        },
        'aws_default_security_group': {
            vpc.default_security_group_name: {
                'vpc_id': '${aws_default_vpc.%s.id}' % vpc.default_vpc_name,
                'egress': [],
                'ingress': []
            }
        },
        'aws_s3_bucket': {
            'trail': {
                'bucket': f'edu-ucsc-gi-{aws.account_name}-trail.{aws.region_name}',
                'lifecycle': {
                    'prevent_destroy': True
                }
            },
            'shared': {
                'bucket': aws.shared_bucket,
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
                'bucket': aws.logs_bucket,
                'lifecycle': {
                    'prevent_destroy': True
                }
            }
        },
        'aws_s3_bucket_logging': {
            bucket: {
                'bucket': '${aws_s3_bucket.%s.id}' % bucket,
                'target_bucket': '${aws_s3_bucket.logs.id}',
                # Other S3 log deliveries, like ELB, implicitly put a slash
                # after the prefix. S3 doesn't, so we add one explicitly.
                'target_prefix': config.s3_access_log_path_prefix(prefix) + '/'
            }
            for bucket, prefix in [
                ('trail', 'cloudtrail'),
                ('aws_config', 'aws_config'),
                ('shared', 'shared')
            ]
        },
        'aws_s3_account_public_access_block': {
            f'{aws.account_name}': {
                'block_public_acls': True,
                'block_public_policy': True,
                'ignore_public_acls': True,
                'restrict_public_buckets': True
            }
        },
        'aws_s3_bucket_policy': {
            'trail': {
                'bucket': '${aws_s3_bucket.trail.id}',
                'policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': 'cloudtrail.amazonaws.com'
                            },
                            'Action': 's3:GetBucketAcl',
                            'Resource': '${aws_s3_bucket.trail.arn}',
                        },
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': 'cloudtrail.amazonaws.com'
                            },
                            'Action': 's3:PutObject',
                            'Resource': '${aws_s3_bucket.trail.arn}/AWSLogs/%s/*' % config.aws_account_id,
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
                            path_prefix=config.alb_access_log_path_prefix('*', deployment=None)
                        ),
                        *aws.s3_access_log_bucket_policy(
                            source_bucket_arn='arn:aws:s3:::*',
                            target_bucket_arn='${aws_s3_bucket.logs.arn}',
                            path_prefix=config.s3_access_log_path_prefix('*', deployment=None)
                        ),
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': 's3.amazonaws.com'
                            },
                            'Action': [
                                's3:PutObject'
                            ],
                            'Resource': [
                                'arn:aws:s3:::${aws_s3_bucket.logs.id}/*'
                            ],
                            'Condition': {
                                'ArnLike': {
                                    'aws:SourceArn': f'arn:aws:s3:::{aws.qualified_bucket_name("*")}'
                                },
                                'StringEquals': {
                                    'aws:SourceAccount': config.aws_account_id,
                                    's3:x-amz-acl': 'bucket-owner-full-control'
                                }
                            }
                        }
                    ]
                })
            },
        },
        'aws_s3_bucket_lifecycle_configuration': {
            'shared': {
                'bucket': '${aws_s3_bucket.shared.id}',
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
                        'noncurrent_days': 90
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
            'shared': {
                'bucket': '${aws_s3_bucket.shared.id}',
                'versioning_configuration': {
                    'status': 'Enabled'
                }
            }
        },
        **(
            {
                'aws_cloudformation_stack': {
                    'chatbot': {
                        'name': config.qualified_resource_name('chatbot'),
                        'template_body': json.dumps({
                            'AWSTemplateFormatVersion': '2010-09-09',
                            'Description': 'Use AWS Chatbot to forward messages from monitoring SNS topic to Slack',
                            'Resources': {
                                'SlackChannelConfiguration': {
                                    'Type': 'AWS::Chatbot::SlackChannelConfiguration',
                                    'Properties': {
                                        'ConfigurationName': config.qualified_resource_name('chatbot'),
                                        'GuardrailPolicies': ['arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess'],
                                        'IamRoleArn': '${aws_iam_role.chatbot.arn}',
                                        'LoggingLevel': 'INFO',
                                        'SlackChannelId': config.slack_integration.channel_id,
                                        'SlackWorkspaceId': config.slack_integration.workspace_id,
                                        'SnsTopicArns': ['${aws_sns_topic.monitoring.arn}'],
                                        'UserRoleRequired': False
                                    }
                                }
                            }
                        })
                    }
                },
                'aws_cloudwatch_event_rule': {
                    'inspector': {
                        'name': 'inspector',
                        'event_pattern': json.dumps({
                            'source': ['aws.inspector2'],
                            'detail-type': ['Inspector2 Finding'],
                            'detail.severity': ['CRITICAL', 'HIGH'],
                            'detail.status': ['ACTIVE']
                        })
                    }
                },
                'aws_cloudwatch_event_target': {
                    'inspector_to_sns': {
                        'rule': '${aws_cloudwatch_event_rule.inspector.name}',
                        'arn': '${aws_sns_topic.monitoring.arn}',
                        'input_transformer': {
                            # AWS EventBridge transforms resemble JSON, but are not valid JSON
                            # https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-transform-target-input.html
                            'input_template': json.dumps({
                                'deployment': config.deployment_stage,
                                'event': {}
                            }).replace('{}', '<aws.events.event.json>')
                        }
                    }
                }
            }
            if config.slack_integration else
            {}
        ),
        'aws_cloudtrail': {
            'trail': {
                'name': config.qualified_resource_name('trail'),
                's3_bucket_name': '${aws_s3_bucket.trail.id}',
                'enable_log_file_validation': True,
                'is_multi_region_trail': True,
                'cloud_watch_logs_group_arn': '${aws_cloudwatch_log_group.trail.arn}:*',
                'cloud_watch_logs_role_arn': '${aws_iam_role.trail.arn}',
                'event_selector': {
                    'read_write_type': 'All',
                    'include_management_events': True,
                    'data_resource': {
                        'type': 'AWS::S3::Object',
                        'values': ['arn:aws:s3']
                    }
                }
            }
        },
        'aws_cloudwatch_log_group': {
            'trail': {
                'name': config.qualified_resource_name('trail'),
                'retention_in_days': config.audit_log_retention_days
            },
            vpc.default_vpc_name: {
                'name': '/aws/vpc/' + config.qualified_resource_name(vpc.default_vpc_name),
                'retention_in_days': config.audit_log_retention_days
            }
        },
        'aws_cloudwatch_log_metric_filter': {
            **{
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
            'trail_logs': {
                'name': config.qualified_resource_name('trail_logs', suffix='.filter'),
                'pattern': '',
                'log_group_name': '${aws_cloudwatch_log_group.trail.name}',
                'metric_transformation': {
                    'name': config.qualified_resource_name('trail_logs'),
                    'namespace': 'LogMetrics',
                    'value': 1,
                    'default_value': 0,
                }
            }
        },
        'aws_cloudwatch_metric_alarm': {
            **{
                a.name: {
                    'alarm_name': config.qualified_resource_name(a.name, suffix='.alarm'),
                    'comparison_operator': 'GreaterThanOrEqualToThreshold',
                    'evaluation_periods': 1,
                    'metric_name': a.metric_name,
                    'namespace': 'LogMetrics',
                    'statistic': a.statistic,
                    'treat_missing_data': 'notBreaching',
                    'threshold': 1,
                    # The CIS documentation does not specify a period. 5 minutes is
                    # the default value when creating the alarm via the console UI.
                    'period': 5 * 60,
                    'alarm_actions': ['${aws_sns_topic.monitoring.arn}'],
                    'ok_actions': ['${aws_sns_topic.monitoring.arn}']
                }
                for a in cis_alarms
            },
            **{
                'trail_logs': {
                    'alarm_name': config.qualified_resource_name('trail_logs', suffix='.alarm'),
                    'comparison_operator': 'LessThanThreshold',
                    'threshold': 1,
                    'datapoints_to_alarm': 1,
                    'evaluation_periods': 1,
                    'treat_missing_data': 'breaching',
                    'alarm_actions': ['${aws_sns_topic.monitoring.arn}'],
                    'ok_actions': ['${aws_sns_topic.monitoring.arn}'],
                    # CloudWatch uses an unconfigurable "evaluation range" when missing
                    # data is involved. In practice this means that an alarm on the
                    # absence of logs with an evaluation period of ten minutes would
                    # require thirty minutes of no logs before the alarm is raised.
                    # Using a metric query we can fill in missing datapoints with a
                    # value of zero and avoid the need for the evaluation range.
                    'metric_query': [
                        {
                            'id': 'log_count_filled',
                            'expression': 'FILL(log_count_raw, 0)',
                            'return_data': True
                        },
                        {
                            'id': 'log_count_raw',
                            'metric': {
                                'metric_name': config.qualified_resource_name('trail_logs'),
                                'namespace': 'LogMetrics',
                                'period': 10 * 60,
                                'stat': 'Sum',
                            }
                        }
                    ]
                }
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
            },
            vpc.default_vpc_name: {
                'name': config.qualified_resource_name(f'{vpc.default_vpc_name}_vpc'),
                'assume_role_policy': json.dumps(
                    {
                        'Version': '2012-10-17',
                        'Statement': [
                            {
                                'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'vpc-flow-logs.amazonaws.com',
                                }
                            }
                        ]
                    }
                )
            },
            **(
                {
                    'chatbot': {
                        'name': config.qualified_resource_name('chatbot'),
                        'assume_role_policy': json.dumps({
                            'Version': '2012-10-17',
                            'Statement': [
                                {
                                    'Effect': 'Allow',
                                    'Action': 'sts:AssumeRole',
                                    'Principal': {
                                        'Service': 'chatbot.amazonaws.com'
                                    }
                                }
                            ]
                        })
                    }
                }
                if config.slack_integration else
                {}
            ),
        },
        'aws_iam_role_policy': {
            'api_gateway': {
                'name': 'azul-api_gateway',
                'role': '${aws_iam_role.api_gateway.id}',
                'policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': [
                                'logs:CreateLogGroup',
                                'logs:CreateLogStream',
                                'logs:DescribeLogGroups',
                                'logs:DescribeLogStreams',
                                'logs:PutLogEvents',
                                'logs:GetLogEvents',
                                'logs:FilterLogEvents'
                            ],
                            'Resource': '*'
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
            },
            vpc.default_vpc_name: {
                'name': config.qualified_resource_name(f'{vpc.default_vpc_name}_vpc'),
                'role': '${aws_iam_role.%s.id}' % vpc.default_vpc_name,
                'policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': [
                                'logs:CreateLogGroup',
                                'logs:CreateLogStream',
                                'logs:PutLogEvents',
                                'logs:DescribeLogGroups',
                                'logs:DescribeLogStreams'
                            ],
                            'Resource': '*'
                        }
                    ]
                })
            },
            **(
                {
                    'chatbot': {
                        'name': config.qualified_resource_name('chatbot'),
                        'role': '${aws_iam_role.chatbot.id}',
                        'policy': json.dumps({
                            'Version': '2012-10-17',
                            'Statement': [
                                {
                                    'Effect': 'Allow',
                                    'Resource': '*',
                                    'Action': [
                                        'cloudwatch:Describe*',
                                        'cloudwatch:Get*',
                                        'cloudwatch:List*',
                                        'logs:Get*',
                                        'logs:List*',
                                        'logs:Describe*',
                                        'logs:TestMetricFilter',
                                        'logs:FilterLogEvents',
                                        'sns:Get*',
                                        'sns:List*'
                                    ]
                                }
                            ]
                        })
                    }
                }
                if config.slack_integration else
                {}
            ),
        },
        'aws_iam_service_linked_role': {
            'opensearch': {
                'aws_service_name': 'opensearchservice.amazonaws.com'
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
        'aws_config_conformance_pack': {
            'nist_800_53': {
                'name': 'nist-800-53',
                'template_body': conformance_pack('conformance_pack_nist_800_53_rev_4'),
                'depends_on': ['aws_config_configuration_recorder.shared']
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
        'aws_sns_topic_policy': {
            'monitoring': {
                'arn': '${aws_sns_topic.monitoring.arn}',
                'policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': [
                                    'events.amazonaws.com',
                                    'cloudwatch.amazonaws.com'
                                ]
                            },
                            'Action': 'sns:Publish',
                            'Resource': '${aws_sns_topic.monitoring.arn}',
                            'Condition': {
                                'StringEquals': {
                                    'aws:SourceAccount': config.aws_account_id
                                }
                            }
                        }
                    ]
                })
            }
        },
        'aws_wafv2_ip_set': {
            'blocked': {
                'name': 'blocked',
                'scope': 'REGIONAL',
                'ip_address_version': 'IPV4',
                'addresses': [],
                'lifecycle': {
                    'ignore_changes': [
                        'addresses'
                    ]
                }
            }
        },
        'aws_ecr_repository': {
            tf_repository: {
                'name': name,
                'force_delete': True
            }
            for name, tf_repository in docker.images_by_tf_repository.keys()
            if config.docker_registry
        },
        'null_resource': {
            **{
                # Copy image from upstream to ECR
                image.tf_image: {
                    'depends_on': [
                        'aws_ecr_repository.' + image.tf_repository
                    ],
                    'triggers': {
                        'script_hash': '${filesha256("%s/scripts/copy_images_to_ecr.py")}' % config.project_root
                    },
                    'lifecycle': {
                        # While `triggers` above only accepts strings, this
                        # property accepts entire resources. Any change to the
                        # image repository resource should cause the image
                        # copying to be kicked off again. The copying is
                        # idempotent, and efficiently so with respect to
                        # bandwidth consumption, but it still takes a couple
                        # minutes, even if all of the destination images are
                        # already in place, so we'd still like to avoid running
                        # it unnecessarily.
                        'replace_triggered_by': [
                            'aws_ecr_repository.' + image.tf_repository
                        ]
                    },
                    'provisioner': {
                        'local-exec': {
                            'command': ' '.join([
                                'python',
                                f'{config.project_root}/scripts/copy_images_to_ecr.py',
                                '--copy',
                                str(image)
                            ]),
                        }
                    }
                }
                for image in docker.images
                if config.docker_registry
            },
            **(
                {
                    # Clean up leftovers from copying
                    'cleanup': {
                        'depends_on': [
                            'null_resource.' + image.tf_image
                            for image in docker.images
                        ],
                        'lifecycle': {
                            'replace_triggered_by': [
                                'aws_ecr_repository.' + image.tf_repository
                                for image in docker.images
                            ]
                        },
                        'provisioner': {
                            'local-exec': {
                                'command': ' '.join([
                                    'python',
                                    f'{config.project_root}/scripts/copy_images_to_ecr.py',
                                    '--cleanup'
                                ]),
                            }
                        }
                    }
                }
                if config.docker_registry else
                {}
            ),
            **{
                # Delete unused images
                tf_repository: {
                    'depends_on': [
                        'aws_ecr_repository.' + tf_repository,
                        *('null_resource.' + image.tf_image for image in images),
                        'null_resource.cleanup'
                    ],
                    'triggers': {
                        'script_hash': '${filesha256("%s/scripts/copy_images_to_ecr.py")}' % config.project_root,
                        'images': ','.join(sorted(image.tf_image for image in images))
                    },
                    'lifecycle': {
                        'replace_triggered_by': [
                            'aws_ecr_repository.' + tf_repository
                        ]
                    },
                    'provisioner': {
                        'local-exec': {
                            'command': ' '.join([
                                'python',
                                f'{config.project_root}/scripts/copy_images_to_ecr.py',
                                '--delete-unused',
                                str(name)
                            ]),
                        }
                    }
                }
                for (name, tf_repository), images in docker.images_by_tf_repository.items()
                if config.docker_registry
            }
        }
    }
}
tf_config = enable_s3_bucket_inventory(tf_config, 'aws_s3_bucket.logs')
tf_config = block_public_s3_bucket_access(tf_config)
emit_tf(tf_config)
