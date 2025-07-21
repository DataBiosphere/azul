from dataclasses import (
    dataclass,
)
import importlib
import json

from more_itertools import (
    one,
)

from azul import (
    R,
    cached_property,
    config,
    iif,
)
from azul.chalice import (
    AzulChaliceApp,
)
from azul.deployment import (
    aws,
    public_ip,
)
from azul.modules import (
    load_app_module,
)
from azul.objects import (
    InternMeta,
)
from azul.terraform import (
    chalice,
    emit_tf,
    vpc,
)
from azul.types import (
    JSON,
    JSONs,
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

    @cached_property
    def chalice(self) -> AzulChaliceApp:
        return load_app_module(self.name).app


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


def check_waf_rules(rules: JSONs) -> JSONs:
    """
    Verify that all the WAF rule actions we use are from a known set of actions.
    If an unexpected action is identified here, it is likely that the logging
    filters in the WAF logging configuration will also need to be updated to
    handle the identified action.
    """
    for rule in rules:
        if 'action' in rule:
            assert one(rule['action'].keys()) in ['block', 'allow'], R(
                'WAF rule has an unexpected action', rule)
        elif 'override_action' in rule:
            assert one(rule['override_action'].keys()) == 'none', R(
                'WAF rule has an unexpected override action', rule)
        else:
            assert False, rule
    return rules


zones_by_domain = {
    domain: Zone.for_domain(domain)
    for app in apps
    for domain in app.domains
}

api_gateway_log_format = {
    'accountId': '$context.accountId',
    'apiId': '$context.apiId',
    'domainName': '$context.domainName',
    'domainPrefix': '$context.domainPrefix',
    'error_message': '$context.error.message',
    'error_responseType': '$context.error.responseType',
    'extendedRequestId': '$context.extendedRequestId',
    'httpMethod': '$context.httpMethod',
    'identity_sourceIp': '$context.identity.sourceIp',
    'identity_userAgent': '$context.identity.userAgent',
    'integration_error': '$context.integration.error',
    'integration_integrationStatus': '$context.integration.integrationStatus',
    'integration_latency': '$context.integration.latency',
    'integration_requestId': '$context.integration.requestId',
    'integration_status': '$context.integration.status',
    'integrationStatus': '$context.integrationStatus',
    'path': '$context.path',
    'protocol': '$context.protocol',
    'requestId': '$context.requestId',
    'requestTime': '$context.requestTime',
    'requestTimeEpoch': '$context.requestTimeEpoch',
    'responseLatency': '$context.responseLatency',
    'responseLength': '$context.responseLength',
    'stage': '$context.stage',
    'status': '$context.status'
}


def waf_match_method(http_method: str) -> JSON:
    return {
        'byte_match_statement': {
            'field_to_match': {
                'method': {}
            },
            'positional_constraint': 'EXACTLY',
            'search_string': http_method,
            'text_transformation': {
                'priority': 0,
                'type': 'NONE'
            }
        }
    }


def waf_match_path(path_regex: str) -> JSON:
    return {
        'regex_match_statement': {
            'regex_string': path_regex,
            'field_to_match': {
                'uri_path': {}
            },
            'text_transformation': {
                'priority': 0,
                'type': 'NONE'
            }
        }
    }


def add_waf_blocked_alarm(resources: JSON) -> JSON:
    """
    Add a metric alarm that trips if the ratio between blocked and overall
    requests goes above 25%. Note that requests blocked by rules listed in
    :py:attr:`Config.waf_rules_not_logged` are not considered.
    """
    if not config.enable_monitoring:
        return resources
    else:
        rules = [
            rule['name']
            for rule in resources['aws_wafv2_web_acl']['api_gateway']['rule']
            if (
                (
                    'block' in rule.get('action', {})
                    # In the case of AWS-managed rules, each rule's action is
                    # pre-configured, and 'override_action' must be specified.
                    # Note, not all possible managed rules use a block action,
                    # however all the managed rules we use do.
                    or 'none' in rule.get('override_action', {})
                )
                and rule['name'] not in config.waf_rules_not_logged
            )
        ]
        metrics = [
            ('AllowedRequests', 'ALL'),
            *[('BlockedRequests', rule) for rule in rules]
        ]
        m_sum = '+'.join(f'm{i}' for i in range(1, len(metrics)))
        expression = f'{m_sum}/(m0+{m_sum})*100'

        assert 'aws_cloudwatch_metric_alarm' not in resources
        return resources | {
            'aws_cloudwatch_metric_alarm': {
                'waf_blocked': {
                    'alarm_name': config.qualified_resource_name('waf_blocked'),
                    'comparison_operator': 'GreaterThanThreshold',
                    'threshold': 25,  # percent blocked of total requests in a period
                    'evaluation_periods': 4,
                    'datapoints_to_alarm': 4,
                    'treat_missing_data': 'notBreaching',
                    'alarm_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                    'ok_actions': ['${data.aws_sns_topic.monitoring.arn}'],
                    'metric_query': [
                        {
                            'id': 'waf',
                            'label': 'Percentage of blocked requests',
                            'expression': expression,
                            'return_data': 'true',
                        },
                        *(
                            {
                                'id': f'm{i}',
                                'metric': {
                                    'namespace': 'AWS/WAFV2',
                                    'metric_name': metric,
                                    'period': 15 * 60,
                                    'stat': 'Sum',
                                    'dimensions': {
                                        'WebACL': '${aws_wafv2_web_acl.api_gateway.name}',
                                        'Region': config.region,
                                        'Rule': rule
                                    }
                                }
                            }
                            for i, (metric, rule) in enumerate(metrics)
                        )
                    ]
                }
            }
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
                name: {
                    'name': config.qualified_resource_name(resource_name=name,
                                                           stage=config.main_deployment_stage),
                    'scope': 'REGIONAL'
                }
                for name in [
                    config.blocked_v4_ips_term,
                    config.allowed_v4_ips_term
                ]
            },
            'aws_wafv2_regex_pattern_set': {
                name: {
                    'name': config.qualified_resource_name(resource_name=name,
                                                           stage=config.main_deployment_stage),
                    'scope': 'REGIONAL',
                }
                for name in [
                    config.blocked_user_agents_regex_term,
                    config.blocked_user_agents_custom_regex_term
                ]
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
        add_waf_blocked_alarm({
            'aws_wafv2_ip_set': {
                # The IPs in this set are exempt from the rate limit on service
                # API requests so as to prevent integration tests from tripping
                # them. In the set, we include the IP of the GitLab instance and
                # that of the machine deploying the set because those are the
                # machines most likely to run integration tests.
                #
                'it_v4_ips': {
                    'name': config.qualified_resource_name('it_v4_ips'),
                    'scope': 'REGIONAL',
                    'ip_address_version': 'IPV4',
                    'addresses': [
                        f'{public_ip()}/32',
                        *[
                            # Data source defined in data_sources.tf.json
                            f'${{data.aws_nat_gateway.gitlab_{zone}.public_ip}}/32'
                            for zone in range(vpc.num_zones)
                        ]
                    ]
                }
            },
            'aws_wafv2_web_acl': {
                'api_gateway': {
                    'name': config.qualified_resource_name('api_gateway'),
                    'default_action': {
                        'allow': {}
                    },
                    'rule': check_waf_rules([
                        {**rule, 'priority': i}
                        for i, rule in enumerate([
                            *[
                                {
                                    'name': name,
                                    'statement': {
                                        'ip_set_reference_statement': {
                                            'arn': '${data.aws_wafv2_ip_set.%s.arn}' % name
                                        }
                                    },
                                    'action': {
                                        action: {}
                                    },
                                    # We label these requests to give us the
                                    # option to exclude them from being logged
                                    # in the WAF log group. See
                                    # aws_wafv2_web_acl_logging_configuration
                                    'rule_label': {
                                        'name': name
                                    },
                                    'visibility_config': {
                                        'metric_name': name,
                                        'sampled_requests_enabled': True,
                                        'cloudwatch_metrics_enabled': True
                                    }
                                }
                                for name, action in [
                                    (config.blocked_v4_ips_term, 'block'),
                                    (config.allowed_v4_ips_term, 'allow')
                                ]
                            ],
                            {
                                'name': config.blocked_user_agents_regex_term,
                                'statement': {
                                    'or_statement': {
                                        'statement': [
                                            {
                                                'regex_pattern_set_reference_statement': {
                                                    'arn': '${data.aws_wafv2_regex_pattern_set.%s.arn}' % regex_set,
                                                    'field_to_match': {
                                                        'single_header': {
                                                            'name': 'user-agent'
                                                        }
                                                    },
                                                    'text_transformation': {
                                                        'priority': 0,
                                                        'type': 'NONE'
                                                    }
                                                }
                                            }
                                            for regex_set in [
                                                config.blocked_user_agents_regex_term,
                                                config.blocked_user_agents_custom_regex_term
                                            ]
                                        ]
                                    }
                                },
                                'action': {
                                    'block': {}
                                },
                                # We label these requests to give us the option
                                # to exclude them from being logged in the WAF
                                # log group. See
                                # aws_wafv2_web_acl_logging_configuration
                                'rule_label': {
                                    'name': config.blocked_user_agents_regex_term
                                },
                                'visibility_config': {
                                    'metric_name': config.blocked_user_agents_regex_term,
                                    'sampled_requests_enabled': True,
                                    'cloudwatch_metrics_enabled': True
                                }
                            },
                            {
                                'name': 'aws_common_rule_set',
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
                                            },
                                            # FIXME: https://github.com/DataBiosphere/azul-private/issues/128
                                            {
                                                # This rule aims to limit bodies to
                                                # 8192 bytes. We need to be able to
                                                # handle larger bodies with hoisted
                                                # parameters, so we demote the rule
                                                # action to be counting instead of
                                                # blocking.
                                                'name': 'SizeRestrictions_BODY',
                                                'action_to_use': {
                                                    'count': {}
                                                }
                                            }
                                        ]
                                    }
                                },
                                'override_action': {
                                    'none': {}
                                },
                                'visibility_config': {
                                    'metric_name': 'aws_common_rule_set',
                                    'sampled_requests_enabled': True,
                                    'cloudwatch_metrics_enabled': True
                                }
                            },
                            {
                                'name': 'aws_amazon_ip_reputation_list',
                                'statement': {
                                    'managed_rule_group_statement': {
                                        'name': 'AWSManagedRulesAmazonIpReputationList',
                                        'vendor_name': 'AWS'
                                    }
                                },
                                'override_action': {
                                    'none': {}
                                },
                                'visibility_config': {
                                    'metric_name': 'aws_amazon_ip_reputation_list',
                                    'sampled_requests_enabled': True,
                                    'cloudwatch_metrics_enabled': True
                                }
                            },
                            {
                                'name': 'aws_unix_rule_set',
                                'statement': {
                                    'managed_rule_group_statement': {
                                        'name': 'AWSManagedRulesUnixRuleSet',
                                        'vendor_name': 'AWS'
                                    }
                                },
                                'override_action': {
                                    'none': {}
                                },
                                'visibility_config': {
                                    'metric_name': 'aws_unix_rule_set',
                                    'sampled_requests_enabled': True,
                                    'cloudwatch_metrics_enabled': True
                                }
                            },
                            *iif(config.waf_bot_control, [
                                {
                                    'name': 'aws_managed_rules_bot_control_rule_set',
                                    'statement': {
                                        'managed_rule_group_statement': {
                                            'name': 'AWSManagedRulesBotControlRuleSet',
                                            'vendor_name': 'AWS',
                                            'version': 'Version_3.1',
                                            'scope_down_statement': {
                                                'not_statement': {
                                                    # Keep consistent with the rules in the response of the
                                                    # /robots.txt route in src/azul/chalice.py
                                                    'statement': waf_match_path(r'^/($|swagger/|robots.txt$)')
                                                }
                                            },
                                            'managed_rule_group_configs': [
                                                {
                                                    'aws_managed_rules_bot_control_rule_set': {
                                                        'inspection_level': 'COMMON'
                                                    }
                                                }
                                            ],
                                            'rule_action_override': [
                                                {
                                                    'name': name,
                                                    'action_to_use': {
                                                        "count": {}
                                                    }
                                                } for name in [
                                                    'CategoryHttpLibrary',
                                                    'SignalNonBrowserUserAgent',
                                                    'SignalAutomatedBrowser',
                                                    'CategoryMiscellaneous',
                                                ]
                                            ]
                                        }
                                    },
                                    'override_action': {
                                        'none': {}
                                    },
                                    'visibility_config': {
                                        'metric_name': 'aws_managed_rules_bot_control_rule_set',
                                        'sampled_requests_enabled': True,
                                        'cloudwatch_metrics_enabled': True
                                    }
                                },
                                {
                                    # It's undocumented what bots are considered
                                    # "verified". While the above managed rule
                                    # only labels requests from "verified" bots,
                                    # this rule completely blocks those labeled
                                    # requests. The managed rule is scoped down
                                    # to URLs dissallowed in robots.txt, so this
                                    # rule shouldn't affect well-behaved bot.
                                    'name': 'block_verified_bots_rule',
                                    'statement': {
                                        'label_match_statement': {
                                            'scope': 'LABEL',
                                            'key': 'awswaf:managed:aws:bot-control:bot:verified'
                                        }
                                    },
                                    'action': {
                                        'block': {}
                                    },
                                    "visibility_config": {
                                        'metric_name': 'block_verified_bots_rule',
                                        'sampled_requests_enabled': True,
                                        'cloudwatch_metrics_enabled': True
                                    }
                                }
                            ]),
                            *[
                                {
                                    'name': rate_limit.name,
                                    'statement': {
                                        'rate_based_statement': {
                                            'limit': rate_limit.value,
                                            'evaluation_window_sec': rate_limit.period,
                                            'aggregate_key_type': 'IP'
                                        }
                                    },
                                    'action': {
                                        'block': {
                                            'custom_response': {
                                                'response_code': 429,
                                                'response_header': [
                                                    {
                                                        'name': 'Retry-After',
                                                        'value': str(rate_limit.retry_after)
                                                    }
                                                ]
                                            }
                                        }
                                    },
                                    'visibility_config': {
                                        'metric_name': rate_limit.name,
                                        'sampled_requests_enabled': True,
                                        'cloudwatch_metrics_enabled': True
                                    }
                                }
                                # We use two rate rules, one with a lower
                                # threshold that will block requests, and one
                                # with a higher threshold that will block
                                # requests and trigger an alarm. Note, the rules
                                # need to be defined in order of descending
                                # threshold size since once a rate rule is
                                # tripped, it will prevent evaluation of any
                                # following rules.
                                for rate_limit in [
                                    config.waf_rate_limit_alarm,
                                    config.waf_rate_limit,
                                ]
                            ],
                            {
                                # See it_v4_ips above
                                'name': 'allow_it_requests',
                                'statement': {
                                    'and_statement': [
                                        {
                                            'statement': [
                                                {
                                                    'ip_set_reference_statement': {
                                                        'arn': '${aws_wafv2_ip_set.%s.arn}' % 'it_v4_ips'
                                                    }
                                                },
                                                waf_match_method('PUT'),
                                                waf_match_path('^(/fetch)?/manifest/files')
                                            ]
                                        }
                                    ]
                                },
                                'action': {
                                    'allow': {}
                                },
                                'visibility_config': {
                                    'metric_name': 'allow_it_requests',
                                    'sampled_requests_enabled': True,
                                    'cloudwatch_metrics_enabled': True
                                }
                            },
                            *[
                                {
                                    'name': limit.name,
                                    'statement': {
                                        'rate_based_statement': {
                                            'limit': limit.value,
                                            'evaluation_window_sec': limit.period,
                                            'aggregate_key_type': 'IP',
                                            'scope_down_statement': {
                                                'and_statement': [
                                                    {
                                                        'statement': [
                                                            waf_match_method(method),
                                                            waf_match_path(path)
                                                        ]
                                                    }
                                                ]
                                            }
                                        }
                                    },
                                    'action': {
                                        'block': {
                                            'custom_response': {
                                                'response_code': 429,
                                                'response_header': [
                                                    {
                                                        'name': 'Retry-After',
                                                        'value': str(limit.retry_after)
                                                    }
                                                ]
                                            }
                                        }
                                    },
                                    'visibility_config': {
                                        'metric_name': limit.name,
                                        'sampled_requests_enabled': True,
                                        'cloudwatch_metrics_enabled': True
                                    }
                                }
                                for method, path, limit in [
                                    ('GET', '^(/fetch)?/repository/files', config.waf_rate_limit_files),
                                    ('PUT', '^(/fetch)?/manifest/files', config.waf_rate_limit_manifests)
                                ]
                            ]
                        ])
                    ]),
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
                        # We use the default behavior of 'KEEP' and selectively
                        # 'DROP' logs that we don't need. This implementation
                        # gives us filters that only 'DROP', working around
                        # https://www.github.com/hashicorp/terraform-provider-aws/issues/32665
                        # which causes TF to deploy the filters in random order,
                        # potentially breaking the desired effect when some
                        # filters 'DROP' and others 'KEEP'.
                        #
                        'default_behavior': 'KEEP',
                        'filter': [
                            {
                                'behavior': 'DROP',
                                'requirement': 'MEETS_ALL',
                                'condition': condition
                            }
                            for condition in [
                                {
                                    'action_condition': {
                                        'action': 'ALLOW'
                                    }
                                },
                                *[
                                    {
                                        'label_name_condition': {
                                            'label_name': 'awswaf:%s:webacl:'
                                                          '${aws_wafv2_web_acl.api_gateway.name}:%s' % (
                                                              config.aws_account_id,
                                                              term
                                                          )
                                        }
                                    } for term in config.waf_rules_not_logged
                                ]
                            ]
                        ]
                    }
                }
            },
            'aws_lambda_function_event_invoke_config': {
                retry.tf_function_resource_name: {
                    'function_name': '${aws_lambda_function.%s.function_name}'
                                     % retry.tf_function_resource_name,
                    'maximum_retry_attempts': retry.num_retries
                }
                for app in apps
                for retry in app.chalice.retries
            }
        }),
        *(
            chalice.tf_config(app.name)['resource']
            for app in apps
        ),
        *(
            {
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
                    }
                },
                'aws_cloudwatch_log_group': {
                    app.name: {
                        'name': '/aws/apigateway/' + config.qualified_resource_name(app.name),
                        'retention_in_days': config.audit_log_retention_days,
                    },
                    f'{app.name}_api_execution': {
                        'name': 'API-Gateway-Execution-Logs_' +
                                '${aws_api_gateway_rest_api.%s.id}' % app.name +
                                '/%s' % config.deployment_stage,
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
