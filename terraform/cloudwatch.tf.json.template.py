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


def dashboard_body(name: str) -> str:
    module = load_module(config.cloudwatch_dashboard_template,
                         'cloudwatch_dashboard_template')
    body = json.dumps(module.dashboard_body(name))
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
                    'depends_on': (
                        []
                        if config.share_es_domain else
                        ['aws_opensearch_domain.index']
                    )
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
                            f'{app_name}_5xx': {
                                'alarm_name': config.qualified_resource_name(app_name + '_5xx'),
                                'namespace': 'AWS/ApiGateway',
                                'metric_name': '5XXError',
                                'dimensions': {
                                    'ApiName': config.qualified_resource_name(app_name),
                                    'Stage': config.deployment_stage,
                                },
                                'statistic': 'Sum',
                                'comparison_operator': 'GreaterThanThreshold',
                                'threshold': 0,
                                'evaluation_periods': 1,
                                'period': 60 * 60,  # one hour
                                'datapoints_to_alarm': 1,
                                'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'treat_missing_data': 'notBreaching',
                            }
                        }
                    }
                    for app_name in config.app_names()
                ),
                *(
                    {
                        'aws_cloudwatch_log_metric_filter': {
                            f'{app_name}cachehealth': {
                                'name': config.qualified_resource_name(f'{app_name}cachehealth', suffix='.filter'),
                                'pattern': '',
                                'log_group_name': (
                                    '/aws/lambda/'
                                    + config.qualified_resource_name(app_name)
                                    + f'-{app_name}cachehealth'
                                ),
                                'metric_transformation': {
                                    'name': config.qualified_resource_name(f'{app_name}cachehealth'),
                                    'namespace': 'LogMetrics',
                                    'value': 1,
                                    'default_value': 0,
                                }
                            }
                        }
                    }
                    for app_name in config.app_names()
                ),
                *(
                    {
                        'aws_cloudwatch_metric_alarm': {
                            f'{app_name}cachehealth': {
                                'alarm_name': config.qualified_resource_name(f'{app_name}cachehealth', suffix='.alarm'),
                                # CloudWatch uses an unconfigurable "evaluation range" when missing
                                # data is involved. In practice this means that an alarm on the
                                # absence of logs with an evaluation window of ten minutes would
                                # require thirty minutes of no logs before the alarm is raised.
                                # Using a metric query we can fill in missing datapoints with a
                                # value of zero and avoid the need for the evaluation range.
                                'metric_query': [
                                    {
                                        'id': 'log_count_raw',
                                        'metric': {
                                            'namespace': 'LogMetrics',
                                            'metric_name': '${aws_cloudwatch_log_metric_filter.'
                                                           '%scachehealth.metric_transformation[0].name}' % app_name,
                                            'stat': 'Sum',
                                            'period': 10 * 60,
                                        }
                                    },
                                    {
                                        'id': 'log_count_filled',
                                        'expression': 'FILL(log_count_raw, 0)',
                                        'return_data': True,
                                    }
                                ],
                                'comparison_operator': 'LessThanThreshold',
                                'threshold': 1,
                                'evaluation_periods': 1,
                                'datapoints_to_alarm': 1,
                                'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'treat_missing_data': 'breaching',
                            }
                        }
                    }
                    for app_name in config.app_names()
                ),
                {
                    'aws_cloudwatch_metric_alarm': {
                        **{
                            f'internet_{direction}': {
                                'alarm_name': config.qualified_resource_name(f'internet_{direction}'),
                                'metric_query': [
                                    *(
                                        {
                                            'id': f'm{zone}',
                                            'metric': {
                                                'namespace': 'AWS/NATGateway',
                                                'metric_name': metric_name,
                                                'dimensions': {
                                                    # Data source defined in data_sources.tf.json
                                                    'NatGatewayId': f'${{data.aws_nat_gateway.gitlab_{zone}.id}}'
                                                },
                                                'stat': 'Sum',
                                                'period': 1 * 60 * 60,
                                            }
                                        }
                                        for zone in range(vpc.num_zones)
                                    ),
                                    {
                                        'id': f'internet_{direction}',
                                        'label': f'Internet {direction} bytes/h',
                                        'expression': ' + '.join(f'm{zone}' for zone in range(vpc.num_zones)),
                                        'return_data': True,
                                    }
                                ],
                                'comparison_operator': 'GreaterThanThreshold',
                                'threshold': threshold,
                                'evaluation_periods': 1,
                                'datapoints_to_alarm': 1,
                                'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'treat_missing_data': 'notBreaching',
                            }
                            for direction, metric_name, threshold in [
                                ('ingress', 'BytesInFromDestination', 50 * 1024 * 1024 * 1024),
                                ('egress', 'BytesOutToDestination', 10 * 1024 * 1024 * 1024)
                            ]
                        },
                        **{
                            f'vpn_{direction}': {
                                'alarm_name': config.qualified_resource_name(f'vpn_{direction}'),
                                'metric_query': [
                                    {
                                        'id': f'vpn_{direction}',
                                        'label': f'VPN {direction} bytes/h',
                                        'metric': {
                                            'namespace': 'AWS/ClientVPN',
                                            'metric_name': metric_name,
                                            'dimensions': {
                                                'Endpoint': '${data.aws_ec2_client_vpn_endpoint.gitlab.id}'
                                            },
                                            'stat': 'Sum',
                                            'period': 1 * 60 * 60,
                                        },
                                        'return_data': True,
                                    }
                                ],
                                'comparison_operator': 'GreaterThanThreshold',
                                'threshold': threshold,
                                'evaluation_periods': 1,
                                'datapoints_to_alarm': 1,
                                'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'treat_missing_data': 'notBreaching',
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
                                'metric_name': metric_alarm.metric.aws_name,
                                'dimensions': {
                                    'FunctionName': '${' + '.'.join((
                                        'aws_lambda_function', metric_alarm.tf_function_resource_name,
                                        'function_name'
                                    )) + '}'
                                },
                                'statistic': 'Sum',
                                'comparison_operator': 'GreaterThanThreshold',
                                'threshold': metric_alarm.threshold,
                                'evaluation_periods': 1,
                                'period': metric_alarm.period,
                                'datapoints_to_alarm': 1,
                                'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                                'treat_missing_data': 'notBreaching',
                            }
                            for app_name in config.app_names()
                            for metric_alarm in load_app_module(app_name).app.metric_alarms
                        },
                        'waf_rate_blocked': {
                            'alarm_name': config.qualified_resource_name('waf_rate_blocked'),
                            'namespace': 'AWS/WAFV2',
                            'metric_name': 'BlockedRequests',
                            'dimensions': {
                                'WebACL': '${aws_wafv2_web_acl.api_gateway.name}',
                                'Region': config.region,
                                'Rule': config.waf_rate_limit_alarm.name
                            },
                            'statistic': 'Sum',
                            'comparison_operator': 'GreaterThanThreshold',
                            'threshold': 0,
                            'evaluation_periods': 1,
                            'period': 5 * 60,
                            'datapoints_to_alarm': 1,
                            'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                            'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                            'treat_missing_data': 'notBreaching',
                        }
                    }
                }
            )
            if config.enable_monitoring else
            ()
        ),
        {
            'aws_cloudwatch_dashboard': {
                'indexer': {
                    'dashboard_name': config.qualified_resource_name('indexer'),
                    'dashboard_body': dashboard_body('indexer')
                },
                **(
                    {
                        'mirror': {
                            'dashboard_name': config.qualified_resource_name('mirror'),
                            'dashboard_body': dashboard_body('mirror')
                        }
                    }
                    if config.enable_mirroring else
                    {}
                )
            }
        }
    ]
})
