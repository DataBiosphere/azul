import json
import shlex

from azul import (
    config,
    require,
)
from azul.deployment import (
    aws,
)
from azul.queues import (
    Queues,
)
from azul.terraform import (
    emit_tf,
)


def dashboard_body() -> str:
    # To minify the template and confirm it is valid JSON before deployment we
    # parse the template file as JSON and then convert it back to a string.
    with open(config.cloudwatch_dashboard_template) as f:
        body = json.load(f)
    body = json.dumps(body)

    def prod_qualified_resource_name(name: str) -> str:
        resource, _, suffix = config.unqualified_resource_name_and_suffix(name)
        return config.qualified_resource_name(resource, suffix=suffix, stage='prod')

    queues = Queues()
    qualified_resource_names = [
        *config.all_queue_names,
        *queues.functions_by_queue().values()
    ]
    replacements = {
        '542754589326': config.aws_account_id,
        'us-east-1': config.region,
        'azul-index-prod': config.es_domain,
        **{
            prod_qualified_resource_name(name): name
            for name in qualified_resource_names
        }
    }
    # Reverse sorted so that if any keys are substrings of other keys (e.g.
    # 'foo' and 'foo_bar'), the longer string is processed before the substring.
    replacements = dict(reversed(sorted(replacements.items())))

    for old, new in replacements.items():
        require(old in body,
                'Missing placeholder', old, config.cloudwatch_dashboard_template)
        body = body.replace(old, new)
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
        }
    ],
    'locals': {
        'nodes': '${jsondecode(data.external.elasticsearch_nodes.result.nodes)}'
    },
    'resource': [
        *(
            (
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
                            'alarm_actions': ['${aws_sns_topic.monitoring.arn}']
                        }
                    }
                }
                for lambda_ in config.lambda_names()
            )
            if config.enable_monitoring else
            ()
        ),
        *(
            [
                {
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
            ]
            if config.enable_monitoring else
            []
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
