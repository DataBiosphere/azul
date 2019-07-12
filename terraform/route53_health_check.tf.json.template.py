from azul.deployment import aws
from azul.template import emit
from azul import config

emit(None if not config.enable_monitoring else {
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
                "composite-azul": {
                    # NOTE: There is a 64 character limit on reference name. Terraform adds long string at the end so
                    #  we must be economical about what we add.
                    "reference_name": f"azul-{config.deployment_stage}",
                    "type": "CALCULATED",
                    "child_health_threshold": 2,
                    "child_healthchecks": [
                        "${aws_route53_health_check." + "indexer" + ".id}",
                        "${aws_route53_health_check." + "service" + ".id}"
                    ],
                    "cloudwatch_alarm_region": aws.region_name,
                    "tags": {
                        "Name": f"azul-composite-{config.deployment_stage}"
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
                    "resource_path": "/explore",
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
                "data-portal": {
                    "fqdn": config.data_browser_domain,
                    "port": 443,
                    "type": "HTTPS",
                    "resource_path": "/",
                    "failure_threshold": "3",
                    "request_interval": "30",
                    "tags": {
                        "Name": config.data_portal_name
                    }
                }
            }
        },
        {
            "aws_route53_health_check": {
                "composite-portal": {
                    "reference_name": f"portal-{config.deployment_stage}",
                    "type": "CALCULATED",
                    "child_health_threshold": 2,
                    "child_healthchecks": [
                        "${aws_route53_health_check." + "data-browser" + ".id}",
                        "${aws_route53_health_check." + "data-portal" + ".id}"
                    ],
                    "cloudwatch_alarm_region": aws.region_name,
                    "tags": {
                        "Name": f"azul-portal-composite-{config.deployment_stage}"
                    }
                }
            }
        }
    ]
})
