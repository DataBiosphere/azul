from azul.deployment import aws, emit_tf
from azul import config

emit_tf(None if config.disable_monitoring else {
    "resource": [
        *[
            {
                "aws_route53_health_check": {
                    name: {
                        "fqdn": config.api_lambda_domain(name),
                        "port": 443,
                        "type": "HTTPS",
                        "resource_path": "/health/cached",
                        "failure_threshold": "3",
                        "request_interval": "30",
                        "tags": {
                            "Name": full_name
                        },
                        "regions": ['us-west-2', 'us-east-1', 'eu-west-1'],
                        "measure_latency": True,
                        # This is necessary only because of a Terraform bug:
                        # https://github.com/hashicorp/terraform/issues/22171
                        "lifecycle": {
                            "create_before_destroy": True
                        }
                    },
                }
            } for name, full_name in (('indexer', config.indexer_name),
                                      ('service', config.service_name))
        ],
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
                    "measure_latency": True,
                    "cloudwatch_alarm_region": aws.region_name,
                    "tags": {
                        "Name": f"azul-composite-{config.deployment_stage}"
                    }
                }
            }
        },
        *[
            {
                "aws_route53_health_check": {
                    name: {
                        "fqdn": domain,
                        "port": 443,
                        "type": "HTTPS",
                        "resource_path": path,
                        "failure_threshold": "3",
                        "request_interval": "30",
                        "tags": {
                            "Name": full_name
                        },
                        "measure_latency": True,
                        # This is necessary only because of a Terraform bug:
                        # https://github.com/hashicorp/terraform/issues/22171
                        "lifecycle": {
                            "create_before_destroy": True
                        }
                    }
                }
            } for name, domain, full_name, path in (
                ("data-browser", config.data_browser_domain, config.data_browser_name, '/explore'),
                ("data-portal", config.data_browser_domain, config.data_portal_name, '/')
            )
        ],
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
                    "measure_latency": True,
                    "cloudwatch_alarm_region": aws.region_name,
                    "tags": {
                        "Name": f"azul-portal-composite-{config.deployment_stage}"
                    }
                }
            }
        }
    ]
})
