from dataclasses import (
    dataclass,
)
import importlib
import json
import shlex

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.files import (
    file_sha1,
)
from azul.objects import (
    InternMeta,
)
from azul.terraform import (
    emit_tf,
    vpc,
)


@dataclass(frozen=True)
class Lambda:
    """
    Represents a AWS Lambda function fronted by an AWS API Gateway.
    """
    name: str  # the name of the Lambda, e.g. 'service'
    domains: list[str]  # a list of public domain names that the Lambda should be exposed at
    policy: str  # AWS Policy for the lambda function

    @classmethod
    def for_name(cls, name):
        policy_module = importlib.import_module(f'azul.{name}.lambda_iam_policy')
        return cls(name=name,
                   domains=[
                       config.api_lambda_domain(name),
                       *config.api_lambda_domain_aliases(name)
                   ],
                   policy=json.dumps(getattr(policy_module, 'policy')))


lambdas = [
    Lambda.for_name('indexer'),
    Lambda.for_name('service')
]


@dataclass(frozen=True)
class Zone(metaclass=InternMeta):
    """
    Represents a Route 53 hosted zone
    """
    slug: str  # the string to use to name the Terraform data source for the zone
    name: str  # the name of the zone

    @classmethod
    def for_domain(cls, domain):
        if domain.endswith(config.domain_name):
            # Any subdomain of the main domain for the current deployment is expected to be defined in a single zone
            # For some lesser deployments (like the `sandbox` or personal deployments), the subdomain may have a dot
            # in it and the main domain may be shared with other deployments (like the `dev` deployment).
            name = config.domain_name
        else:
            # Other subdomain are expected to be defined in the zone for their immediate parent domain.
            name = '.'.join(domain.split('.')[1:])
        assert name
        return cls(slug=name.replace('.', '_').replace('-', '_'),
                   name=name)


zones_by_domain = {
    domain: Zone.for_domain(domain)
    for lambda_ in lambdas
    for domain in lambda_.domains
}

emit_tf({
    "data": [
        *(
            [
                {
                    "aws_route53_zone": {
                        zone.slug: {
                            "name": zone.name,
                            "private_zone": False
                        }
                    }
                } for zone in set(zones_by_domain.values())
            ]
        ),
        *(
            [
                {
                    "aws_vpc": {
                        "gitlab": {
                            "filter": {
                                "name": "tag:Name",
                                "values": ["azul-gitlab"]
                            }
                        }
                    },
                },
                {
                    "aws_subnet": {
                        f"gitlab_{vpc.subnet_name(public)}_{zone}": {
                            "filter": {
                                "name": "tag:Name",
                                "values": [f"azul-gitlab-{vpc.subnet_name(public)}-{zone}"]
                            }
                        }
                        for public in (False, True)
                        for zone in range(vpc.num_zones)
                    }
                },
                {
                    # To allow the network interface IDs to be iterated here, the
                    # `apply` target in `$project_root/terraform/Makefile` creates
                    # the VPC endpoints first before all other resources.
                    "aws_network_interface": {
                        lambda_.name: {
                            "for_each": "${aws_vpc_endpoint.%s.network_interface_ids}" % lambda_.name,
                            "id": "${each.key}"
                        } for lambda_ in lambdas
                    }
                }
            ] if config.private_api else [
            ]
        )
    ],
    # Note that ${} references exist to interpolate a value AND express a dependency.
    "resource": [
        {
            "aws_api_gateway_base_path_mapping": {
                f"{lambda_.name}_{i}": {
                    "api_id": "${module.chalice_%s.RestAPIId}" % lambda_.name,
                    "stage_name": "${module.chalice_%s.stage_name}" % lambda_.name,
                    "domain_name": "${aws_api_gateway_domain_name.%s_%i.domain_name}" % (lambda_.name, i)
                }
                for i, domain in enumerate(lambda_.domains)
            },
            "aws_api_gateway_domain_name": {
                f"{lambda_.name}_{i}": {
                    "domain_name": "${aws_acm_certificate.%s_%i.domain_name}" % (lambda_.name, i),
                    "certificate_arn": "${aws_acm_certificate_validation.%s_%i.certificate_arn}" % (lambda_.name, i)
                } for i, domain in enumerate(lambda_.domains)
            },
            "aws_api_gateway_method_settings": {
                f"{lambda_.name}_{i}": {
                    "rest_api_id": "${module.chalice_%s.RestAPIId}" % lambda_.name,
                    "stage_name": "${module.chalice_%s.stage_name}" % lambda_.name,
                    "method_path": "*/*",  # every URL path, every HTTP method
                    "settings": {
                        "metrics_enabled": True,
                        "data_trace_enabled": config.debug == 2,
                        "logging_level": "ERROR" if config.debug == 0 else "INFO"
                    }
                } for i, domain in enumerate(lambda_.domains)
            },
            "aws_acm_certificate": {
                f"{lambda_.name}_{i}": {
                    "domain_name": domain,
                    "validation_method": "DNS",
                    "provider": "aws.us-east-1",
                    # I tried using SANs for the alias domains (like the DRS
                    # domain) but Terraform kept swapping the zones, I think
                    # because the order of elements in
                    # `aws_acm_certificate.domain_validation_options` is not
                    # deterministic. The alternative is to use separate certs,
                    # one for each domain, the main one as well as for each
                    # alias.
                    #
                    # Update 03/07/2022: My guess about the non-determinism was
                    # correct. That bug was "fixed" in Terraform by making the
                    # domain_validation_options a set so that elements can't be
                    # accessed via numeric index. The Terraform documentation
                    # recommends looping over the elements in that set. That's
                    # what we do for GitLab. To do the same here would require
                    # bigger refactoring that I don't think is worth it. The
                    # current solution works, too.
                    "subject_alternative_names": [],
                    "lifecycle": {
                        "create_before_destroy": True
                    }
                } for i, domain in enumerate(lambda_.domains)
            },
            "aws_acm_certificate_validation": {
                f"{lambda_.name}_{i}": {
                    "certificate_arn": "${aws_acm_certificate.%s_%i.arn}" % (lambda_.name, i),
                    "validation_record_fqdns": [
                        "${aws_route53_record.%s_domain_validation_%i.fqdn}" % (lambda_.name, i)],
                    "provider": "aws.us-east-1"
                } for i, domain in enumerate(lambda_.domains)
            },
            "aws_route53_record": {
                **{
                    f"{lambda_.name}_domain_validation_{i}": {
                        **{
                            # We know there is only one. See comment above.
                            key: "${tolist(aws_acm_certificate.%s_%i.domain_validation_options).0.resource_record_%s}"
                                 % (lambda_.name, i, key) for key in ('name', 'type')
                        },
                        "zone_id": "${data.aws_route53_zone.%s.id}" % zones_by_domain[domain].slug,
                        "records": [
                            # We know there is only one. See comment above.
                            "${tolist(aws_acm_certificate.%s_%i.domain_validation_options).0.resource_record_value}" % (
                                lambda_.name, i)
                        ],
                        "ttl": 60
                    } for i, domain in enumerate(lambda_.domains)
                },
                **{
                    f"{lambda_.name}_{i}": {
                        "zone_id": "${data.aws_route53_zone.%s.id}" % zones_by_domain[domain].slug,
                        "name": "${aws_api_gateway_domain_name.%s_%i.domain_name}" % (lambda_.name, i),
                        "type": "A",
                        **({
                               "alias": {
                                   "name": "${aws_lb.%s.dns_name}" % lambda_.name,
                                   "zone_id": "${aws_lb.%s.zone_id}" % lambda_.name,
                                   "evaluate_target_health": False
                               }
                           }
                           if config.private_api else
                           {
                               "alias": {
                                   "name": "${aws_api_gateway_domain_name.%s_%i.cloudfront_domain_name}" % (
                                       lambda_.name, i),
                                   "zone_id": "${aws_api_gateway_domain_name.%s_%i.cloudfront_zone_id}" % (
                                       lambda_.name, i),
                                   "evaluate_target_health": True,
                               }
                           })
                    } for i, domain in enumerate(lambda_.domains)
                }
            },
            "aws_cloudwatch_log_group": {
                lambda_.name: {
                    "name": "/aws/apigateway/" + config.qualified_resource_name(lambda_.name),
                    "retention_in_days": 1827,
                }
            },
            "null_resource": {
                f'{lambda_.name}_log_group_provisioner': {
                    "triggers": {
                        "file_sha1": file_sha1(config.project_root + "/scripts/log_api_gateway.py"),
                        "log_group_id": f"${{aws_cloudwatch_log_group.{lambda_.name}.id}}"
                    },
                    # FIXME: Use Terraform to configure API Gateway access logs
                    #        https://github.com/DataBiosphere/azul/issues/3412
                    "provisioner": {
                        "local-exec": {
                            "command": ' '.join(map(shlex.quote, [
                                "python",
                                config.project_root + "/scripts/log_api_gateway.py",
                                "${module.chalice_%s.RestAPIId}" % lambda_.name,
                                config.deployment_stage,
                                "${aws_cloudwatch_log_group.%s.arn}" % lambda_.name
                            ]))
                        }
                    }
                }
            },
            "aws_iam_role": {
                lambda_.name: {
                    "name": config.qualified_resource_name(lambda_.name),
                    "assume_role_policy": json.dumps({
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": "sts:AssumeRole",
                                "Principal": {
                                    "Service": "lambda.amazonaws.com"
                                }
                            },
                            *(
                                {
                                    "Effect": "Allow",
                                    "Action": "sts:AssumeRole",
                                    "Principal": {
                                        "AWS": f"arn:aws:iam::{account}:root"
                                    },
                                    # Wildcards are not supported in `Principal`, but they are in `Condition`
                                    "Condition": {
                                        "StringLike": {
                                            "aws:PrincipalArn": [f"arn:aws:iam::{account}:role/{role}"
                                                                 for role in roles]
                                        }
                                    }
                                }
                                for account, roles in config.external_lambda_role_assumptors.items()
                            )
                        ]
                    }),
                    **aws.permissions_boundary_tf
                }
            },
            "aws_iam_role_policy": {
                lambda_.name: {
                    "name": lambda_.name,
                    "policy": lambda_.policy,
                    "role": "${aws_iam_role.%s.id}" % lambda_.name
                },
            },
            "aws_wafv2_web_acl_association": {
                lambda_.name: {
                    # Chalice doesn't expose the ARN of the API Gateway stages, so we
                    # construct the ARN manually using this workaround.
                    # https://github.com/aws/chalice/issues/1816#issuecomment-1012231084
                    "resource_arn": f"${{module.chalice_{lambda_.name}.rest_api_arn}}"
                                    f"/stages/${{module.chalice_{lambda_.name}.stage_name}}",
                    "web_acl_arn": f"${{aws_wafv2_web_acl.{lambda_.name}.arn}}"
                }
            },
            "aws_wafv2_web_acl": {
                lambda_.name: {
                    "name": config.qualified_resource_name(lambda_.name),
                    "default_action": {
                        "allow": {}
                    },
                    "rule": [
                    ],
                    "scope": "REGIONAL",
                    "visibility_config": {
                        "cloudwatch_metrics_enabled": True,
                        "metric_name": "WebACL",
                        "sampled_requests_enabled": True,
                    }
                }
            },
            **(
                {
                    "aws_lb": {
                        lambda_.name: {
                            "name": config.qualified_resource_name(lambda_.name),
                            "load_balancer_type": "application",
                            "internal": "true",
                            "subnets": [
                                "${data.aws_subnet.gitlab_%s_%s.id}" % (vpc.subnet_name(public=True), zone)
                                for zone in range(vpc.num_zones)
                            ],
                            "security_groups": [
                                "${aws_security_group.%s_alb.id}" % lambda_.name
                            ]
                        }
                    },
                    "aws_lb_listener": {
                        lambda_.name: {
                            "port": 443,
                            "protocol": "HTTPS",
                            "ssl_policy": "ELBSecurityPolicy-2016-08",
                            "certificate_arn": "${aws_acm_certificate.%s_0.arn}" % lambda_.name,
                            "default_action": [
                                {
                                    "target_group_arn": "${aws_lb_target_group.%s.id}" % lambda_.name,
                                    "type": "forward"
                                }
                            ],
                            "load_balancer_arn": "${aws_lb.%s.id}" % lambda_.name
                        }
                    },
                    "aws_lb_target_group": {
                        lambda_.name: {
                            "name": config.qualified_resource_name(lambda_.name),
                            "port": 443,
                            "protocol": "HTTPS",
                            "target_type": "ip",
                            "vpc_id": "${data.aws_vpc.gitlab.id}",
                            "health_check": {
                                "protocol": "HTTPS",
                                "path": f"/{config.deployment_stage}/version",
                                "port": "traffic-port",
                                "healthy_threshold": 5,
                                "unhealthy_threshold": 2,
                                "timeout": 5,
                                "interval": 30,
                                "matcher": "200,403"
                            }
                        }
                    },
                    "aws_lb_target_group_attachment": {
                        lambda_.name: {
                            "for_each": "${{for i in data.aws_network_interface.%s : i.id => i.private_ip}}" % (
                                lambda_.name),
                            "target_group_arn": "${aws_lb_target_group.%s.arn}" % lambda_.name,
                            "target_id": "${each.value}"
                        }
                    },
                    "aws_vpc_endpoint": {
                        lambda_.name: {
                            "vpc_id": "${data.aws_vpc.gitlab.id}",
                            "service_name": f"com.amazonaws.{config.region}.execute-api",
                            "vpc_endpoint_type": "Interface",
                            "security_group_ids": [
                                "${aws_security_group.%s_vpce.id}" % lambda_.name
                            ],
                            "subnet_ids": [
                                f"${{data.aws_subnet.gitlab_{vpc.subnet_name(public=False)}_{zone}.id}}"
                                for zone in range(vpc.num_zones)
                            ]
                        }
                    },
                    "aws_vpc_endpoint_policy": {
                        lambda_.name: {
                            "vpc_endpoint_id": "${aws_vpc_endpoint.%s.id}" % lambda_.name,
                        }
                    },
                    "aws_security_group": {
                        lambda_.name: {
                            "name": config.qualified_resource_name(lambda_.name),
                            "vpc_id": "${data.aws_vpc.gitlab.id}",
                            "ingress": [
                                vpc.security_rule(description="Any traffic from the VPC",
                                                  cidr_blocks=["${data.aws_vpc.gitlab.cidr_block}"],
                                                  protocol=-1,
                                                  from_port=0,
                                                  to_port=0)
                            ],
                            "egress": [
                                vpc.security_rule(description="Any traffic",
                                                  cidr_blocks=["0.0.0.0/0"],
                                                  protocol=-1,
                                                  from_port=0,
                                                  to_port=0)
                            ],
                        },
                        f"{lambda_.name}_alb": {
                            "name": config.qualified_resource_name(lambda_.name, suffix="_alb"),
                            "vpc_id": "${data.aws_vpc.gitlab.id}",
                            "ingress": [
                                vpc.security_rule(description="Any traffic from the VPC",
                                                  cidr_blocks=["${data.aws_vpc.gitlab.cidr_block}"],
                                                  protocol=-1,
                                                  from_port=0,
                                                  to_port=0)
                            ],
                            "egress": [
                                vpc.security_rule(description="Any traffic to the VPC",
                                                  cidr_blocks=["${data.aws_vpc.gitlab.cidr_block}"],
                                                  protocol=-1,
                                                  from_port=0,
                                                  to_port=0)
                            ],
                        },
                        f"{lambda_.name}_vpce": {
                            "name": config.qualified_resource_name(lambda_.name, suffix="_vpce"),
                            "vpc_id": "${data.aws_vpc.gitlab.id}",
                            "ingress": [
                                vpc.security_rule(description="Any traffic from the VPC",
                                                  cidr_blocks=["${data.aws_vpc.gitlab.cidr_block}"],
                                                  protocol=-1,
                                                  from_port=0,
                                                  to_port=0)
                            ],
                            "egress": [
                                vpc.security_rule(description="Any traffic to the VPC",
                                                  cidr_blocks=["${data.aws_vpc.gitlab.cidr_block}"],
                                                  protocol=-1,
                                                  from_port=0,
                                                  to_port=0)
                            ],
                        }
                    }
                }
                if config.private_api else {
                }
            )
        } for lambda_ in lambdas
    ]
})
