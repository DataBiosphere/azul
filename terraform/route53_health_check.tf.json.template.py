from azul.template import emit
from azul import config, aws

emit({
    "resource": [
        {
            "aws_route53_health_check": {
                "indexer": {
                    "fqdn": config.api_lambda_domain('indexer'),
                    "port": 443,
                    "type": "HTTPS",
                    "resource_path": "/health",
                    "failure_threshold": "3",
                    "request_interval": "30",
                    "tags": {
                        "Name": config.indexer_name
                    }
                }
            }
        },
        {
            "aws_route53_health_check": {
                "service": {
                    "fqdn": config.api_lambda_domain('service'),
                    "port": 443,
                    "type": "HTTPS",
                    "resource_path": "/health",
                    "failure_threshold": "3",
                    "request_interval": "30",
                    "tags": {
                        "Name": config.service_name
                    }
                }
            }
        },
        {
            "aws_route53_health_check": {
                "data-browser": {
                    "fqdn": config.data_browser_domain,
                    "port": 443,
                    "type": "HTTPS",
                    "resource_path": "/health",
                    "failure_threshold": "3",
                    "request_interval": "30",
                    "tags": {
                        "Name": config.data_browser_name
                    }
                }
            }
        },
        {
            "aws_route53_health_check": {
                "composite": {
                    "reference_name": f"azul-composite-{config.deployment_stage} ",
                    "type": "CALCULATED",
                    "child_health_threshold": 3,
                    "child_healthchecks": [
                        "${aws_route53_health_check." + "indexer" + ".id}",
                        "${aws_route53_health_check." + "service" + ".id}",
                        "${aws_route53_health_check." + "data-browser" + ".id}"
                    ],
                    "cloudwatch_alarm_region": aws.region_name,
                    "tags": {
                        "Name": f"azul-composite-{config.deployment_stage}"
                    }
                }
            }
        }
    ]
} if config.enable_monitoring else None)
