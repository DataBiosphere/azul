import json

from azul import (
    config,
)
from azul.terraform import (
    emit_tf,
    vpc,
)

logs = {
    'index': ('INDEX_SLOW_LOGS', True),
    'search': ('SEARCH_SLOW_LOGS', True),
    'error': ('ES_APPLICATION_LOGS', True)
}

domain = config.es_domain

emit_tf(None if config.share_es_domain else {
    'resource': [
        *(
            {
                'aws_cloudwatch_log_group': {
                    f'{log}_log': {
                        'name': f'/aws/aes/domains/{domain}/{log}-logs',
                        'retention_in_days': config.audit_log_retention_days
                    }
                }
            }
            for log in logs.keys()
        ),
        {
            'aws_cloudwatch_log_resource_policy': {
                'index': {
                    'policy_name': domain,
                    'policy_document': json.dumps(
                        {
                            'Version': '2012-10-17',
                            'Statement': [
                                {
                                    'Effect': 'Allow',
                                    'Principal': {
                                        'Service': 'es.amazonaws.com'
                                    },
                                    'Action': [
                                        'logs:PutLogEvents',
                                        'logs:CreateLogStream'
                                    ],
                                    'Resource': [
                                        '${aws_cloudwatch_log_group.' + log + '_log.arn}:*' for log in logs.keys()
                                    ]
                                }
                            ]
                        }
                    )
                }
            }
        },
        {
            'aws_elasticsearch_domain': {
                'index': {
                    'access_policies': json.dumps({
                        'Version': '2012-10-17',
                        'Statement': [
                            {
                                'Effect': 'Allow',
                                'Principal': {
                                    'AWS': 'arn:aws:iam::${local.account_id}:root'
                                },
                                'Action': 'es:*',
                                'Resource': 'arn:aws:es:${local.region}:${local.account_id}:domain/' + domain + '/*'
                            }
                        ]
                    }),
                    'advanced_options': {
                        'rest.action.multi.allow_explicit_index': 'true',
                        'override_main_response_version': 'false'
                    },
                    'cluster_config': {
                        'instance_count': config.es_instance_count,
                        'instance_type': config.es_instance_type,
                        # Needed for using multiple subnets (1 per zone) in the VPC
                        'zone_awareness_enabled': True
                    },
                    'domain_name': domain,
                    'ebs_options': {
                        'ebs_enabled': 'true',
                        'volume_size': config.es_volume_size,
                        'volume_type': 'gp2'
                    }
                    if config.es_volume_size else
                    {
                        'ebs_enabled': 'false',
                    },
                    'vpc_options': {
                        'subnet_ids': [
                            f'${{data.aws_subnet.gitlab_private_{zone}.id}}'
                            for zone in range(vpc.num_zones)
                        ],
                        'security_group_ids': ['${aws_security_group.elasticsearch.id}']
                    },
                    'elasticsearch_version': '7.10',
                    'log_publishing_options': [
                        {
                            'cloudwatch_log_group_arn': '${aws_cloudwatch_log_group.' + log + '_log.arn}',
                            'enabled': 'true' if enabled else 'false',
                            'log_type': log_type
                        }
                        for log, (log_type, enabled) in logs.items()
                    ],
                    'lifecycle': {
                        'ignore_changes': [
                            # Quoting AWS support:
                            #
                            # > Please note that with automated snapshots
                            # > disabled, the 'automatedSnapshotStartHour'
                            # > parameter of the domain configuration is set
                            # > to '-1' from the service end (this can only
                            # > be done from the service side). Please ensure
                            # > that this parameter is not overriden to a
                            # > different value from your end, else the
                            # > automated snapshots would be triggered back
                            # > again.
                            #
                            # So we can't explicitly set `ignore_changes`
                            # to -1 here since that would prevent the
                            # creation of the resource by Terraform. It's
                            # possible that just omitting that property is
                            # sufficient but doing so resulted in a plan
                            # that changed the property from -1 to null
                            # while listing it in `ignore_changes` resulted
                            # in a plan without any changes, which seems
                            # safer to me. The property is deprecated
                            # anyways.
                            #
                            'snapshot_options'
                        ]
                    },
                    'encrypt_at_rest': {
                        'enabled': True
                    },
                    'node_to_node_encryption': {
                        'enabled': True
                    }
                }
            },
            'null_resource': {
                'cluster_settings': {
                    'depends_on': [
                        'aws_elasticsearch_domain.index'
                    ],
                    'triggers': {
                        'script_hash': '${filesha256("%s/scripts/manage_cluster_settings.py")}' % config.project_root
                    },
                    'lifecycle': {
                        'replace_triggered_by': [
                            'aws_elasticsearch_domain.index'
                        ]
                    },
                    'provisioner': {
                        'local-exec': {
                            'command': ' '.join([
                                'python',
                                f'{config.project_root}/scripts/manage_cluster_settings.py'
                            ]),
                        }
                    }
                }
            },
            'aws_security_group': {
                'elasticsearch': {
                    'name': config.qualified_resource_name('elasticsearch'),
                    'vpc_id': '${data.aws_vpc.gitlab.id}',
                    'ingress': [
                        vpc.security_rule(cidr_blocks=['${data.aws_vpc.gitlab.cidr_block}'],
                                          protocol='tcp',
                                          from_port=443,
                                          to_port=443)
                    ],
                }
            }
        }
    ]
})
