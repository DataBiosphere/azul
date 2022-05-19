from azul import (
    config,
)
from azul.terraform import (
    emit_tf,
)

emit_tf(None if config.disable_monitoring else {
    "resource": [
        {
            "aws_cloudwatch_log_metric_filter": {
                f"{lambda_}_5xx": {
                    "name": config.qualified_resource_name(lambda_ + '_5xx'),
                    "pattern": "{ $.status = 5* }",
                    "log_group_name": "${aws_cloudwatch_log_group.%s.name}" % lambda_,
                    "metric_transformation": {
                        "name": config.qualified_resource_name(lambda_ + '_5xx'),
                        "namespace": config.qualified_resource_name('metrics'),
                        "value": 1,
                        "unit": "Count"
                    }
                }
            },
            "aws_cloudwatch_metric_alarm": {
                f"{lambda_}_5xx": {
                    "alarm_name": config.qualified_resource_name(lambda_ + '_5xx'),
                    "comparison_operator": "GreaterThanThreshold",
                    "evaluation_periods": 6,
                    "period": 6 * 10,
                    "metric_name": "${aws_cloudwatch_log_metric_filter.%s_5xx.metric_transformation[0].name}"
                                   % lambda_,
                    "namespace": "${aws_cloudwatch_log_metric_filter.%s_5xx.metric_transformation[0].namespace}"
                                 % lambda_,
                    "statistic": "Sum",
                    "threshold": 10,
                }
            }
        }
        for lambda_ in config.lambda_names()
    ]
})
