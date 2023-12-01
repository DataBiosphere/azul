from dataclasses import (
    dataclass,
)
import importlib
import json

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.objects import (
    InternMeta,
)
from azul.terraform import (
    chalice,
    emit_tf,
    vpc,
)


@dataclass(frozen=True)
class Application:
    """
    An application is set of AWS Lambda functions that cooperate to serve a
    particular purpose. One of the functions is fronted by AWS API Gateway so
    as to expose the application via HTTP.
    """
    name: str  # the name of the application, e.g. 'service'
    domains: list[str]  # a list of public domain names that the application is exposed at
    policy: str  # the AWS IAM policy defining the permissions of the application

    @classmethod
    def for_name(cls, name):
        policy_module = importlib.import_module(f'azul.{name}.lambda_iam_policy')
        return cls(name=name,
                   domains=[
                       config.api_lambda_domain(name),
                       *config.api_lambda_domain_aliases(name)
                   ],
                   policy=json.dumps(getattr(policy_module, 'policy')))


apps = [
    Application.for_name('indexer'),
    Application.for_name('service')
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
    for app in apps
    for domain in app.domains
}

api_gateway_log_format = {
    'accountId': '$context.accountId',
    'apiId': '$context.apiId',
    'authorizer_claims_property': '$context.authorizer.claims.property',
    'authorizer_error': '$context.authorizer.error',
    'authorizer_principalId': '$context.authorizer.principalId',
    'authorizer_property': '$context.authorizer.property',
    'awsEndpointRequestId': '$context.awsEndpointRequestId',
    'awsEndpointRequestId2': '$context.awsEndpointRequestId2',
    'customDomain_basePathMatched': '$context.customDomain.basePathMatched',
    'dataProcessed': '$context.dataProcessed',
    'domainName': '$context.domainName',
    'domainPrefix': '$context.domainPrefix',
    'error_message': '$context.error.message',
    'error_messageString': '$context.error.messageString',
    'error_responseType': '$context.error.responseType',
    'extendedRequestId': '$context.extendedRequestId',
    'httpMethod': '$context.httpMethod',
    'identity_accountId': '$context.identity.accountId',
    'identity_caller': '$context.identity.caller',
    'identity_cognitoAuthenticationProvider': '$context.identity.cognitoAuthenticationProvider',
    'identity_cognitoAuthenticationType': '$context.identity.cognitoAuthenticationType',
    'identity_cognitoIdentityId': '$context.identity.cognitoIdentityId',
    'identity_cognitoIdentityPoolId': '$context.identity.cognitoIdentityPoolId',
    'identity_principalOrgId': '$context.identity.principalOrgId',
    'identity_clientCert_clientCertPem': '$context.identity.clientCert.clientCertPem',
    'identity_clientCert_subjectDN': '$context.identity.clientCert.subjectDN',
    'identity_clientCert_issuerDN': '$context.identity.clientCert.issuerDN',
    'identity_clientCert_serialNumber': '$context.identity.clientCert.serialNumber',
    'identity_clientCert_validity_notBefore': '$context.identity.clientCert.validity.notBefore',
    'identity_clientCert_validity_notAfter': '$context.identity.clientCert.validity.notAfter',
    'identity_sourceIp': '$context.identity.sourceIp',
    'identity_user': '$context.identity.user',
    'identity_userAgent': '$context.identity.userAgent',
    'identity_userArn': '$context.identity.userArn',
    'integration_error': '$context.integration.error',
    'integration_integrationStatus': '$context.integration.integrationStatus',
    'integration_latency': '$context.integration.latency',
    'integration_requestId': '$context.integration.requestId',
    'integration_status': '$context.integration.status',
    'integrationErrorMessage': '$context.integrationErrorMessage',
    'integrationLatency': '$context.integrationLatency',
    'integrationStatus': '$context.integrationStatus',
    'path': '$context.path',
    'protocol': '$context.protocol',
    'requestId': '$context.requestId',
    'requestTime': '$context.requestTime',
    'requestTimeEpoch': '$context.requestTimeEpoch',
    'responseLatency': '$context.responseLatency',
    'responseLength': '$context.responseLength',
    'routeKey': '$context.routeKey',
    'stage': '$context.stage',
    'status': '$context.status'
}

emit_tf({
    'data': [
        {
            'aws_route53_zone': {
                zone.slug: {
                    'name': zone.name,
                    'private_zone': False
                }
                for zone in set(zones_by_domain.values())
            },
            'aws_vpc': {
                'gitlab': {
                    'filter': {
                        'name': 'tag:Name',
                        'values': ['azul-gitlab']
                    }
                }
            },
            'aws_subnet': {
                f'gitlab_{vpc.subnet_name(public)}_{zone}': {
                    'filter': {
                        'name': 'tag:Name',
                        'values': [f'azul-gitlab_{vpc.subnet_name(public)}_{zone}']
                    }
                }
                for public in (False, True)
                for zone in range(vpc.num_zones)
            },
            'aws_wafv2_ip_set': {
                'blocked': {
                    'name': 'blocked',
                    'scope': 'REGIONAL'
                }
            }
        },
        *(
            {
                **chalice.tf_config(app.name)['data'],
                **(
                    {
                        # To allow the network interface IDs to be iterated here, the
                        # `apply` target in `$project_root/terraform/Makefile` creates
                        # the VPC endpoints first before all other resources.
                        'aws_network_interface': {
                            app.name: {
                                'for_each': '${aws_vpc_endpoint.%s.network_interface_ids}' % app.name,
                                'id': '${each.key}'
                            }
                        }
                    }
                    if config.private_api else
                    {}
                )
            }
            for app in apps
        )
    ],
    'locals': [
        chalice.tf_config(app.name)['locals']
        for app in apps
    ],
    'resource': [
        {
            'aws_wafv2_web_acl': {
                'api_gateway': {
                    'name': config.qualified_resource_name('api_gateway'),
                    'default_action': {
                        'allow': {}
                    },
                    'rule': [
                        {
                            'priority': 0,
                            'name': 'BlockedIPs',
                            'action': {
                                'block': {}
                            },
                            'statement': {
                                'ip_set_reference_statement': {
                                    'arn': '${data.aws_wafv2_ip_set.blocked.arn}'
                                }
                            },
                            'visibility_config': {
                                'metric_name': 'BlockedIPs',
                                'sampled_requests_enabled': True,
                                'cloudwatch_metrics_enabled': True
                            }
                        },
                        {
                            'priority': 1,
                            'name': config.waf_rate_rule_name,
                            'action': {
                                'block': {}
                            },
                            'statement': {
                                'rate_based_statement': {
                                    'limit': 1000,  # limit must be between 100 and 20,000,000
                                    'aggregate_key_type': 'IP'
                                }
                            },
                            'visibility_config': {
                                'metric_name': config.waf_rate_rule_name,
                                'sampled_requests_enabled': True,
                                'cloudwatch_metrics_enabled': True
                            }
                        },
                        {
                            'priority': 2,
                            'name': 'AWS-CommonRuleSet',
                            'override_action': {
                                'none': {}
                            },
                            'statement': {
                                'managed_rule_group_statement': {
                                    'name': 'AWSManagedRulesCommonRuleSet',
                                    'vendor_name': 'AWS',
                                    'rule_action_override': [
                                        {
                                            # This rule would limit the query
                                            # string to 2048 bytes, which would
                                            # block valid requests made during
                                            # the integration tests. We disarm
                                            # it by setting the action to
                                            # `count`. API Gateway protects us
                                            # from over-sized query strings by
                                            # limiting the total combined size
                                            # of the request line and header
                                            # values to 10240 bytes.
                                            'name': 'SizeRestrictions_QUERYSTRING',
                                            'action_to_use': {
                                                'count': {}
                                            }
                                        }
                                    ]
                                }
                            },
                            'visibility_config': {
                                'metric_name': 'AWS-CommonRuleSet',
                                'sampled_requests_enabled': True,
                                'cloudwatch_metrics_enabled': True
                            }
                        },
                        {
                            'priority': 3,
                            'name': 'AWS-AmazonIpReputationList',
                            'override_action': {
                                'none': {}
                            },
                            'statement': {
                                'managed_rule_group_statement': {
                                    'name': 'AWSManagedRulesAmazonIpReputationList',
                                    'vendor_name': 'AWS'
                                }
                            },
                            'visibility_config': {
                                'metric_name': 'AWS-AmazonIpReputationList',
                                'sampled_requests_enabled': True,
                                'cloudwatch_metrics_enabled': True
                            }
                        },
                        {
                            'priority': 4,
                            'name': 'AWS-UnixRuleSet',
                            'override_action': {
                                'none': {}
                            },
                            'statement': {
                                'managed_rule_group_statement': {
                                    'name': 'AWSManagedRulesUnixRuleSet',
                                    'vendor_name': 'AWS'
                                }
                            },
                            'visibility_config': {
                                'metric_name': 'AWS-UnixRuleSet',
                                'sampled_requests_enabled': True,
                                'cloudwatch_metrics_enabled': True
                            }
                        },
                    ],
                    'scope': 'REGIONAL',
                    'visibility_config': {
                        'cloudwatch_metrics_enabled': True,
                        'metric_name': 'WebACL',
                        'sampled_requests_enabled': True,
                    }
                }
            },
            'aws_cloudwatch_log_group': {
                'waf_api_gateway': {
                    # WAF logging requires this specific log group name prefix
                    # https://docs.aws.amazon.com/waf/latest/developerguide/logging-cw-logs.html#logging-cw-logs-naming
                    'name': 'aws-waf-logs-' + config.qualified_resource_name('api_gateway'),
                    'retention_in_days': config.audit_log_retention_days
                }
            },
            'aws_wafv2_web_acl_logging_configuration': {
                'waf_api_gateway': {
                    'log_destination_configs': [
                        '${aws_cloudwatch_log_group.waf_api_gateway.arn}'
                    ],
                    'resource_arn': '${aws_wafv2_web_acl.api_gateway.arn}',
                    'logging_filter': {
                        'default_behavior': 'DROP',
                        'filter': {
                            'behavior': 'KEEP',
                            'requirement': 'MEETS_ALL',
                            'condition': {
                                'action_condition': {
                                    'action': 'BLOCK'
                                }
                            }
                        }
                    }
                }
            },
            'aws_lambda_function_event_invoke_config': {
                function_name: {
                    'function_name': '${aws_lambda_function.indexer_%s.function_name}' % function_name,
                    'maximum_retry_attempts': 0
                }
                for function_name in
                [
                    lm
                    for lm in ['forward_alb_logs', 'forward_s3_logs']
                    if config.enable_log_forwarding
                ]
            }
        },
        *(
            {
                **chalice.tf_config(app.name)['resource'],
                'aws_api_gateway_stage': {
                    app.name: {
                        'rest_api_id': '${aws_api_gateway_rest_api.%s.id}' % app.name,
                        'deployment_id': '${aws_api_gateway_deployment.%s.id}' % app.name,
                        'stage_name': config.deployment_stage,
                        'access_log_settings': {
                            'destination_arn': '${aws_cloudwatch_log_group.%s.arn}' % app.name,
                            'format': json.dumps(api_gateway_log_format)
                        },
                        'lifecycle': {
                            'replace_triggered_by': [
                                'aws_api_gateway_deployment.%s.id' % app.name
                            ]
                        }
                    }
                },
                'aws_api_gateway_base_path_mapping': {
                    f'{app.name}_{i}': {
                        'api_id': '${aws_api_gateway_rest_api.%s.id}' % app.name,
                        'stage_name': '${aws_api_gateway_stage.%s.stage_name}' % app.name,
                        'domain_name': '${aws_api_gateway_domain_name.%s_%i.domain_name}' % (app.name, i),
                        'lifecycle': {
                            'replace_triggered_by': [
                                'aws_api_gateway_stage.%s.id' % app.name
                            ]
                        }
                    }
                    for i, domain in enumerate(app.domains)
                },
                'aws_api_gateway_domain_name': {
                    f'{app.name}_{i}': {
                        'domain_name': '${aws_acm_certificate.%s_%i.domain_name}' % (app.name, i),
                        'certificate_arn': '${aws_acm_certificate_validation.%s_%i.certificate_arn}' % (app.name, i),
                        'security_policy': 'TLS_1_2'
                    } for i, domain in enumerate(app.domains)
                },
                'aws_api_gateway_method_settings': {
                    f'{app.name}_{i}': {
                        'rest_api_id': '${aws_api_gateway_rest_api.%s.id}' % app.name,
                        'stage_name': '${aws_api_gateway_stage.%s.stage_name}' % app.name,
                        'method_path': '*/*',  # every URL path, every HTTP method
                        'settings': {
                            'metrics_enabled': True,
                            'data_trace_enabled': config.debug == 2,
                            'logging_level': 'ERROR' if config.debug == 0 else 'INFO'
                        },
                        'lifecycle': {
                            'replace_triggered_by': [
                                'aws_api_gateway_stage.%s.id' % app.name
                            ]
                        }
                    } for i, domain in enumerate(app.domains)
                },
                'aws_acm_certificate': {
                    f'{app.name}_{i}': {
                        'domain_name': domain,
                        'validation_method': 'DNS',
                        'provider': 'aws.us-east-1',
                        # I tried using SANs for the alias domains (like the DRS
                        # domain) but Terraform kept swapping the zones, I think
                        # because the order of elements in
                        # `aws_acm_certificate.domain_validation_options` is not
                        # deterministic. The alternative is to use separate certs,
                        # one for each domain, the main one as well as for each
                        # alias.
                        #
                        # Update 03/07/2022: My guess about the non-determinism was
                        # correct. That bug was 'fixed' in Terraform by making the
                        # domain_validation_options a set so that elements can't be
                        # accessed via numeric index. The Terraform documentation
                        # recommends looping over the elements in that set. That's
                        # what we do for GitLab. To do the same here would require
                        # bigger refactoring that I don't think is worth it. The
                        # current solution works, too.
                        'subject_alternative_names': [],
                        'lifecycle': {
                            'create_before_destroy': True
                        }
                    } for i, domain in enumerate(app.domains)
                },
                'aws_acm_certificate_validation': {
                    f'{app.name}_{i}': {
                        'certificate_arn': '${aws_acm_certificate.%s_%i.arn}' % (app.name, i),
                        'validation_record_fqdns': [
                            '${aws_route53_record.%s_domain_validation_%i.fqdn}' % (app.name, i)],
                        'provider': 'aws.us-east-1'
                    } for i, domain in enumerate(app.domains)
                },
                'aws_route53_record': {
                    **{
                        f'{app.name}_domain_validation_{i}': {
                            **{
                                # We know there is only one. See comment above.
                                key: '${tolist(aws_acm_certificate.%s_%i.domain_validation_options)'
                                     '.0.resource_record_%s}'
                                     % (app.name, i, key)
                                for key in ('name', 'type')
                            },
                            'zone_id': '${data.aws_route53_zone.%s.id}' % zones_by_domain[domain].slug,
                            'records': [
                                # We know there is only one. See comment above.
                                '${tolist(aws_acm_certificate.%s_%i.domain_validation_options)'
                                '.0.resource_record_value}'
                                % (app.name, i)
                            ],
                            'ttl': 60
                        } for i, domain in enumerate(app.domains)
                    },
                    **{
                        f'{app.name}_{i}': {
                            'zone_id': '${data.aws_route53_zone.%s.id}' % zones_by_domain[domain].slug,
                            'name': '${aws_api_gateway_domain_name.%s_%i.domain_name}' % (app.name, i),
                            'type': 'A',
                            **({
                                   'alias': {
                                       'name': '${aws_lb.%s.dns_name}' % app.name,
                                       'zone_id': '${aws_lb.%s.zone_id}' % app.name,
                                       'evaluate_target_health': False
                                   }
                               }
                               if config.private_api else
                               {
                                   'alias': {
                                       'name': '${aws_api_gateway_domain_name.%s_%i.cloudfront_domain_name}' % (
                                           app.name, i),
                                       'zone_id': '${aws_api_gateway_domain_name.%s_%i.cloudfront_zone_id}' % (
                                           app.name, i),
                                       'evaluate_target_health': True,
                                   }
                               })
                        } for i, domain in enumerate(app.domains)
                    },
                    **(
                        {
                            'notify': {
                                'zone_id': '${data.aws_route53_zone.%s.id}' % zones_by_domain[app.domains[0]].slug,
                                'name': '_amazonses.' + app.domains[0],
                                'type': 'TXT',
                                'ttl': '600',
                                'records': ['${aws_ses_domain_identity.notify.verification_token}']
                            }
                        }
                        if app.name == 'indexer' and config.enable_monitoring else
                        {}
                    )
                },
                'aws_cloudwatch_log_group': {
                    app.name: {
                        'name': '/aws/apigateway/' + config.qualified_resource_name(app.name),
                        'retention_in_days': config.audit_log_retention_days,
                    }
                },
                'aws_iam_role': {
                    app.name: {
                        'name': config.qualified_resource_name(app.name),
                        'assume_role_policy': json.dumps({
                            'Version': '2012-10-17',
                            'Statement': [
                                {
                                    'Effect': 'Allow',
                                    'Action': 'sts:AssumeRole',
                                    'Principal': {
                                        'Service': 'lambda.amazonaws.com'
                                    }
                                },
                                *(
                                    {
                                        'Effect': 'Allow',
                                        'Action': 'sts:AssumeRole',
                                        'Principal': {
                                            'AWS': f'arn:aws:iam::{account}:root'
                                        },
                                        # Wildcards are not supported in `Principal`, but they are in `Condition`
                                        'Condition': {
                                            'StringLike': {
                                                'aws:PrincipalArn': [f'arn:aws:iam::{account}:role/{role}'
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
                'aws_iam_role_policy': {
                    app.name: {
                        'name': app.name,
                        'policy': app.policy,
                        'role': '${aws_iam_role.%s.id}' % app.name
                    },
                },
                'aws_wafv2_web_acl_association': {
                    app.name: {
                        'resource_arn': '${aws_api_gateway_stage.%s.arn}' % app.name,
                        'web_acl_arn': '${aws_wafv2_web_acl.api_gateway.arn}',
                        'lifecycle': {
                            'replace_triggered_by': [
                                'aws_api_gateway_stage.%s.id' % app.name
                            ]
                        }
                    }
                },
                'aws_security_group': {
                    app.name: {
                        'name': config.qualified_resource_name(app.name),
                        'vpc_id': '${data.aws_vpc.gitlab.id}',
                        'ingress': [
                            vpc.security_rule(description='Any traffic from the VPC',
                                              cidr_blocks=['${data.aws_vpc.gitlab.cidr_block}'],
                                              protocol=-1,
                                              from_port=0,
                                              to_port=0)
                        ],
                        'egress': [
                            vpc.security_rule(description='Any traffic',
                                              cidr_blocks=['0.0.0.0/0'],
                                              protocol=-1,
                                              from_port=0,
                                              to_port=0)
                        ],
                    },
                    **(
                        {
                            f'{app.name}_alb': {
                                'name': config.qualified_resource_name(app.name, suffix='_alb'),
                                'vpc_id': '${data.aws_vpc.gitlab.id}',
                                'ingress': [
                                    vpc.security_rule(description='Any traffic from the VPC',
                                                      cidr_blocks=['${data.aws_vpc.gitlab.cidr_block}'],
                                                      protocol=-1,
                                                      from_port=0,
                                                      to_port=0)
                                ],
                                'egress': [
                                    vpc.security_rule(description='Any traffic to the VPC',
                                                      cidr_blocks=['${data.aws_vpc.gitlab.cidr_block}'],
                                                      protocol=-1,
                                                      from_port=0,
                                                      to_port=0)
                                ],
                            },
                            f'{app.name}_vpce': {
                                'name': config.qualified_resource_name(app.name, suffix='_vpce'),
                                'vpc_id': '${data.aws_vpc.gitlab.id}',
                                'ingress': [
                                    vpc.security_rule(description='Any traffic from the VPC',
                                                      cidr_blocks=['${data.aws_vpc.gitlab.cidr_block}'],
                                                      protocol=-1,
                                                      from_port=0,
                                                      to_port=0)
                                ],
                                'egress': [
                                    vpc.security_rule(description='Any traffic to the VPC',
                                                      cidr_blocks=['${data.aws_vpc.gitlab.cidr_block}'],
                                                      protocol=-1,
                                                      from_port=0,
                                                      to_port=0)
                                ],
                            }
                        } if config.private_api else {
                        }
                    )
                },
                **(
                    {
                        'aws_ses_domain_identity': {
                            'notify': {
                                'domain': app.domains[0]
                            }
                        },
                        'aws_ses_identity_policy': {
                            'notify': {
                                'identity': '${aws_ses_domain_identity.notify.arn}',
                                'name': config.qualified_resource_name('notify'),
                                'policy': json.dumps({
                                    'Version': '2012-10-17',
                                    'Statement': [
                                        {
                                            'Effect': 'Allow',
                                            'Principal': {
                                                'AWS': 'arn:aws:sts::'
                                                       + aws.account
                                                       + ':assumed-role/'
                                                       + '${aws_lambda_function.' + app.name + '.function_name}/'
                                                       # The following is the role-session-name of the principal
                                                       # assuming the role via an AWS STS AssumeRole operation.
                                                       + '${aws_lambda_function.' + '_'.join([app.name, 'notify'])
                                                       + '.function_name}'
                                            },
                                            'Action': [
                                                'ses:SendEmail',
                                                'ses:SendRawEmail'
                                            ],
                                            'Resource': '${aws_ses_domain_identity.notify.arn}',
                                        }
                                    ]
                                })
                            }
                        }
                    } if app.name == 'indexer' and config.enable_monitoring else {}
                ),
                **(
                    {
                        'aws_lb': {
                            app.name: {
                                'name': config.qualified_resource_name(app.name),
                                'load_balancer_type': 'application',
                                'internal': 'true',
                                'subnets': [
                                    '${data.aws_subnet.gitlab_%s_%s.id}' % (
                                        vpc.subnet_name(public=True), zone)
                                    for zone in range(vpc.num_zones)
                                ],
                                'security_groups': [
                                    '${aws_security_group.%s_alb.id}' % app.name
                                ],
                                'access_logs': [
                                    {
                                        'bucket': '${data.aws_s3_bucket.logs.id}',
                                        'prefix': config.alb_access_log_path_prefix(app.name),
                                        'enabled': True
                                    }
                                ]
                            }
                        },
                        'aws_lb_listener': {
                            app.name: {
                                'port': 443,
                                'protocol': 'HTTPS',
                                'ssl_policy': 'ELBSecurityPolicy-FS-1-2-Res-2019-08',
                                'certificate_arn': '${aws_acm_certificate.%s_0.arn}' % app.name,
                                'default_action': [
                                    {
                                        'target_group_arn': '${aws_lb_target_group.%s.id}' % app.name,
                                        'type': 'forward'
                                    }
                                ],
                                'load_balancer_arn': '${aws_lb.%s.id}' % app.name
                            }
                        },
                        'aws_lb_target_group': {
                            app.name: {
                                'name': config.qualified_resource_name(app.name),
                                'port': 443,
                                'protocol': 'HTTPS',
                                'target_type': 'ip',
                                'vpc_id': '${data.aws_vpc.gitlab.id}',
                                'health_check': {
                                    'protocol': 'HTTPS',
                                    'path': f'/{config.deployment_stage}/version',
                                    'port': 'traffic-port',
                                    'healthy_threshold': 5,
                                    'unhealthy_threshold': 2,
                                    'timeout': 5,
                                    'interval': 30,
                                    'matcher': '200,403'
                                }
                            }
                        },
                        'aws_lb_target_group_attachment': {
                            app.name: {
                                'for_each': '${{for i in data.aws_network_interface.%s : i.id => i.private_ip}}' % (
                                    app.name),
                                'target_group_arn': '${aws_lb_target_group.%s.arn}' % app.name,
                                'target_id': '${each.value}'
                            }
                        },
                        'aws_vpc_endpoint': {
                            app.name: {
                                'vpc_id': '${data.aws_vpc.gitlab.id}',
                                'service_name': f'com.amazonaws.{config.region}.execute-api',
                                'vpc_endpoint_type': 'Interface',
                                'security_group_ids': [
                                    '${aws_security_group.%s_vpce.id}' % app.name
                                ],
                                'subnet_ids': [
                                    f'${{data.aws_subnet.gitlab_{vpc.subnet_name(public=False)}_{zone}.id}}'
                                    for zone in range(vpc.num_zones)
                                ]
                            }
                        },
                        'aws_vpc_endpoint_policy': {
                            app.name: {
                                'vpc_endpoint_id': '${aws_vpc_endpoint.%s.id}' % app.name,
                            }
                        }
                    }
                    if config.private_api else {
                    }
                )
            } for app in apps
        )
    ]
})
