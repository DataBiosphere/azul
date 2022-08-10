from azul import (
    config,
)
from azul.terraform import (
    emit_tf,
)

emit_tf(None if config.disable_monitoring else {
    "resource": [
        {
            "aws_cloudwatch_metric_alarm": {
                f"{lambda_}_5xx": {
                    "alarm_name": config.qualified_resource_name(lambda_ + '_5xx'),
                    "comparison_operator": "GreaterThanThreshold",
                    # This alarm will catch persistent 5XX errors occurring over
                    # one hour, specifically when more than one occurrence is
                    # sampled in a ten-minute period for six consecutive periods.
                    "evaluation_periods": 6,
                    "period": 60 * 10,
                    "metric_name": "5XXError",
                    "namespace": "AWS/ApiGateway",
                    "statistic": "Sum",
                    "threshold": 1,
                    "treat_missing_data": "notBreaching",
                    "dimensions": {
                        "ApiName": config.qualified_resource_name(lambda_),
                        "Stage": config.deployment_stage,
                    }
                }
            }
        }
        for lambda_ in config.lambda_names()
    ]
})
