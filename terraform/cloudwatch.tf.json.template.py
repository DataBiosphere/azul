import json

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.modules import (
    load_app_module,
    load_module,
)
from azul.terraform import (
    emit_tf,
    vpc,
)


def dashboard_body() -> str:
    module = load_module(config.cloudwatch_dashboard_template,
                         'cloudwatch_dashboard_template')
    body = json.dumps(module.dashboard_body)
    return body


emit_tf({
    'data': [
        {
            'external': {
                'elasticsearch_nodes': {
                    'program': [
                        'python',
                        f'{config.project_root}/scripts/elasticsearch_nodes.py'
                    ],
                    'query': {},
                    'depends_on': ([]
                                   if config.share_es_domain else
                                   ['aws_elasticsearch_domain.index'])
                }
            }
        },
        *(
            (
                {
                    'aws_sns_topic': {
                        'monitoring': {
                            'name': aws.monitoring_topic_name
                        }
                    }
                },
                {
                    'aws_nat_gateway': {
                        **{
                            f'gitlab_{zone}': {
                                'filter': {
                                    'name': 'tag:Name',
                                    'values': [f'azul-gitlab_{zone}']
                                },
                            }
                            for zone in range(vpc.num_zones)
                        }
                    },
                    'aws_ec2_client_vpn_endpoint': {
                        'gitlab': {
                            'filter': {
                                'name': 'tag:Name',
                                'values': ['azul-gitlab']
                            }
                        }
                    }
                }
            ) if config.enable_monitoring else ()
        ),
    ],
    'locals': {
        'nodes': '${jsondecode(data.external.elasticsearch_nodes.result.nodes)}'
    },
    'resource': [
        *(
            (
                *(
                    {
                        'aws_cloudwatch_metric_alarm': {
                            f'{lambda_}_5xx': {
                                'alarm_name': config.qualified_resource_name(lambda_ + '_5xx'),
                                'comparison_operator': 'GreaterThanThreshold',
                                # This alarm catches persistent 5XX errors occurring over
                                # one hour, specifically when more than one occurrence is
                                # sampled in a ten-minute period for six consecutive periods.
                                'evaluation_periods': 6,
                                'period': 60 * 10,
                                'metric_name': '5XXError',
                                'namespace': 'AWS/ApiGateway',
                                'statistic': 'Sum',
                                'threshold': 1,
                                'treat_missing_data': 'notBreaching',
                                'dimensions': {
                                    'ApiName': config.qualified_resource_name(lambda_),
                                    'Stage': config.deployment_stage,
                                },
                                'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                            }
                        }
                    }
                    for lambda_ in config.lambda_names()
                ),
                *(
                    {
                        'aws_cloudwatch_log_metric_filter': {
                            f'{lambda_}cachehealth': {
                                'name': config.qualified_resource_name(f'{lambda_}cachehealth', suffix='.filter'),
                                'pattern': '',
                                'log_group_name': (
                                    '/aws/lambda/'
                                    + config.qualified_resource_name(lambda_)
                                    + f'-{lambda_}cachehealth'
                                ),
                                'metric_transformation': {
                                    'name': config.qualified_resource_name(f'{lambda_}cachehealth'),
                                    'namespace': 'LogMetrics',
                                    'value': 1,
                                    'default_value': 0,
                                }
                            }
                        }
                    }
                    for lambda_ in config.lambda_names()
                ),
                *(
                    {
                        'aws_cloudwatch_metric_alarm': {
                            f'{lambda_}cachehealth': {
                                'alarm_name': config.qualified_resource_name(f'{lambda_}cachehealth', suffix='.alarm'),
                                'comparison_operator': 'LessThanThreshold',
                                'threshold': 1,
                                'datapoints_to_alarm': 1,
                                'evaluation_periods': 1,
                                'treat_missing_data': 'breaching',
                                'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
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
                                        'return_data': True,
                                    },
                                    {
                                        'id': 'log_count_raw',
                                        'metric': {
                                            'metric_name': '${aws_cloudwatch_log_metric_filter.'
                                                           '%scachehealth.metric_transformation[0].name}' % lambda_,
                                            'namespace': 'LogMetrics',
                                            'period': 10 * 60,
                                            'stat': 'Sum',
                                        }
                                    }
                                ]
                            }
                        }
                    }
                    for lambda_ in config.lambda_names()
                ),
                {
                    'aws_cloudwatch_metric_alarm': {
                        **{
                            f'internet_{direction}': {
                                'alarm_name': config.qualified_resource_name(f'internet_{direction}'),
                                'comparison_operator': 'GreaterThanThreshold',
                                'threshold': threshold,
                                'evaluation_periods': 1,
                                'datapoints_to_alarm': 1,
                                'treat_missing_data': 'notBreaching',
                                'metric_query': [
                                    {
                                        'id': f'internet_{direction}',
                                        'label': f'Internet {direction} bytes/h',
                                        'expression': ' + '.join(f'm{zone}' for zone in range(vpc.num_zones)),
                                        'return_data': True,
                                    },
                                    *(
                                        {
                                            'id': f'm{zone}',
                                            'metric': {
                                                'dimensions': {
                                                    'NatGatewayId': f'${{data.aws_nat_gateway.gitlab_{zone}.id}}'
                                                },
                                                'namespace': 'AWS/NATGateway',
                                                'metric_name': metric_name,
                                                'period': 1 * 60 * 60,
                                                'stat': 'Sum',
                                            }
                                        }
                                        for zone in range(vpc.num_zones)
                                    )
                                ],
                                'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                            }
                            for direction, metric_name, threshold in [
                                ('ingress', 'BytesInFromDestination', 50 * 1024 * 1024 * 1024),
                                ('egress', 'BytesOutToDestination', 10 * 1024 * 1024 * 1024)
                            ]
                        },
                        **{
                            f'vpn_{direction}': {
                                'alarm_name': config.qualified_resource_name(f'vpn_{direction}'),
                                'comparison_operator': 'GreaterThanThreshold',
                                'threshold': threshold,
                                'evaluation_periods': 1,
                                'datapoints_to_alarm': 1,
                                'treat_missing_data': 'notBreaching',
                                'metric_query': [
                                    {
                                        'id': f'vpn_{direction}',
                                        'label': f'VPN {direction} bytes/h',
                                        'metric': {
                                            'dimensions': {
                                                'Endpoint': '${data.aws_ec2_client_vpn_endpoint.gitlab.id}'
                                            },
                                            'namespace': 'AWS/ClientVPN',
                                            'metric_name': metric_name,
                                            'period': 1 * 60 * 60,
                                            'stat': 'Sum',
                                        },
                                        'return_data': True,
                                    }
                                ],
                                'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                            }
                            for direction, metric_name, threshold in [
                                ('ingress', 'IngressBytes', 100 * 1024 * 1024 * 1024),
                                ('egress', 'EgressBytes', 10 * 1024 * 1024 * 1024)
                            ]
                        },
                        **{
                            metric_alarm.tf_resource_name: {
                                'alarm_name': config.qualified_resource_name(
                                    metric_alarm.tf_resource_name,
                                    suffix='.alarm'
                                ),
                                'namespace': 'AWS/Lambda',
                                'dimensions': {
                                    'FunctionName': '${' + '.'.join((
                                        'aws_lambda_function', metric_alarm.tf_function_resource_name,
                                        'function_name'
                                    )) + '}'
                                },
                                'metric_name': metric_alarm.metric.aws_name,
                                'comparison_operator': 'GreaterThanThreshold',
                                'statistic': 'Sum',
                                'threshold': metric_alarm.threshold,
                                'period': metric_alarm.period,
                                'datapoints_to_alarm': 1,
                                'evaluation_periods': 1,
                                'treat_missing_data': 'notBreaching',
                                'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                            }
                            for lambda_name in config.lambda_names()
                            for metric_alarm in load_app_module(lambda_name).app.metric_alarms
                        },
                        'waf_blocked': {
                            'alarm_name': config.qualified_resource_name('waf_blocked'),
                            'comparison_operator': 'GreaterThanThreshold',
                            'threshold': 25,  # percent blocked of total requests in a period
                            'evaluation_periods': 4,
                            'datapoints_to_alarm': 4,
                            'treat_missing_data': 'notBreaching',
                            'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                            'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                            'metric_query': [
                                {
                                    'id': 'waf',
                                    'label': 'Percentage of blocked requests',
                                    'expression': 'm1/(m0+m1)*100',
                                    'return_data': 'true',
                                },
                                *(
                                    {
                                        'id': f'm{i}',
                                        'metric': {
                                            'namespace': 'AWS/WAFV2',
                                            'metric_name': metric,
                                            'period': 15 * 60,
                                            'stat': 'Sum',
                                            'dimensions': {
                                                'WebACL': '${aws_wafv2_web_acl.api_gateway.name}',
                                                'Region': config.region,
                                                'Rule': 'ALL'
                                            }
                                        }
                                    }
                                    for i, metric in enumerate(['AllowedRequests', 'BlockedRequests'])
                                )
                            ]
                        },
                        'waf_rate_blocked': {
                            'alarm_name': config.qualified_resource_name('waf_rate_blocked'),
                            'comparison_operator': 'GreaterThanThreshold',
                            'threshold': 0,
                            'datapoints_to_alarm': 1,
                            'evaluation_periods': 1,
                            'period': 5 * 60,
                            'metric_name': 'BlockedRequests',
                            'namespace': 'AWS/WAFV2',
                            'statistic': 'Sum',
                            'treat_missing_data': 'notBreaching',
                            'dimensions': {
                                'WebACL': '${aws_wafv2_web_acl.api_gateway.name}',
                                'Region': config.region,
                                'Rule': config.waf_rate_alarm_rule_name
                            },
                            'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                            'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                        }
                    }
                }
            )
            if config.enable_monitoring else
            ()
        ),
        {
            'aws_cloudwatch_dashboard': {
                'dashboard': {
                    'dashboard_name': config.qualified_resource_name('dashboard'),
                    'dashboard_body': dashboard_body()
                }
            }
        }
    ]
})
