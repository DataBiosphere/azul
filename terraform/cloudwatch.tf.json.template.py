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
            }
        }
        for lambda_ in config.lambda_names()
    ]
})
