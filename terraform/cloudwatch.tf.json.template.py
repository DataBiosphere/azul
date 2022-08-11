from azul import (
    config,
)
from azul.terraform import (
    emit_tf,
)

emit_tf(None if config.disable_monitoring else {
    'resource': [
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
                        'alarm_actions': ['${aws_sns_topic.monitoring.arn}']
                    }
                }
            }
            for lambda_ in config.lambda_names()
        ),
        {
            'aws_sns_topic': {
                'monitoring': {
                    'name': config.monitoring_topic_name
                }
            },
            'aws_sns_topic_subscription': {
                'monitoring': {
                    'topic_arn': '${aws_sns_topic.monitoring.arn}',
                    # The `email` protocol is only partially supported. Since
                    # Terraform cannot confirm or delete pending subscriptions
                    # (see link below), a script is run during the final stages
                    # of `make deploy` to facilitate confirmation process.
                    # https://registry.terraform.io/providers/hashicorp/aws/4.3.0/docs/resources/sns_topic_subscription#protocol-support
                    'protocol': 'email',
                    'endpoint': config.azul_notification_email
                }
            }
        }
    ]
})
