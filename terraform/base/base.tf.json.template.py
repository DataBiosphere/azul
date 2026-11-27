import shlex

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.infra.terraform import (
    emit_tf,
)
from azul.lib.functions import (
    iif,
)
from azul.service.user_service import (
    UserService,
)


# FIXME: Remove this function along with the `import` section below
#        https://github.com/DataBiosphere/azul/issues/8518
#
def _table_exists(table_name: str) -> bool:
    try:
        aws.dynamodb.describe_table(TableName=table_name)
    except aws.dynamodb.exceptions.ResourceNotFoundException:
        return False
    else:
        return True


# In blue-green mode, the `base` component of the alias owns the user state,
# that of each instance everything else. In stand-alone mode, the `base`
# component owns both.
#
owns_user_state = not config.deployment.is_instance
owns_instance_state = not config.deployment.is_alias

kms_keys = [
    key
    for key in config.kms_keys
    if (owns_user_state if key.user_state else owns_instance_state)
]

emit_tf({
    'resource': [
        {
            'google_service_account': {
                'azul' + service_account.value: {
                    'project': '${local.google_project}',
                    'account_id': service_account.id(config),
                    'display_name': service_account.id(config),
                    'description': f'Azul service account in {config.deployment_stage}',
                    'provisioner': [
                        {
                            'local-exec': {
                                'command': ' '.join(map(shlex.quote, [
                                    'python',
                                    config.project_root + '/scripts/provision_credentials.py',
                                    'service_account',
                                    '--create',
                                    '${self.email}',
                                    service_account.secret_name
                                ]))
                            }
                        }, {
                            'local-exec': {
                                'when': 'destroy',
                                'command': ' '.join(map(shlex.quote, [
                                    'python',
                                    config.project_root + '/scripts/provision_credentials.py',
                                    'service_account',
                                    '--destroy',
                                    '${self.email}',
                                    service_account.secret_name
                                ]))
                            }
                        }
                    ]
                }
                for service_account in config.ServiceAccount
                if owns_instance_state
            },
            **(
                {
                    'google_project_iam_member': {
                        'azul': {
                            'project': '${local.google_project}',
                            'role': '${google_project_iam_custom_role.azul.id}',
                            'member': 'serviceAccount:${google_service_account.azul.email}'
                        },
                    },
                    'google_project_iam_custom_role': {
                        'azul': {
                            'role_id': f'azul_{config.deployment_stage}_{config.deployment_incarnation}',
                            'title': f'azul_{config.deployment_stage}',
                            'permissions': [
                                'bigquery.jobs.create',
                                'bigquery.tables.list',
                                *[
                                    f'bigquery.{resource}.{action}'
                                    for resource in ('capacityCommitments', 'reservations')
                                    for action in ('get', 'update')
                                ],
                                *[
                                    f'bigquery.{resource}.{action}'
                                    for resource in ('capacityCommitments', 'reservations', 'reservationAssignments')
                                    for action in ('create', 'list', 'delete')
                                ]
                            ]
                        },
                    }
                }
                if config.is_tdr_enabled() and owns_instance_state else
                {}
            ),
            'null_resource': iif(owns_instance_state, {
                'hmac_secret': {
                    'provisioner': [
                        {
                            'local-exec': {
                                'command': ' '.join(map(shlex.quote, [
                                    'python',
                                    config.project_root + '/scripts/provision_credentials.py',
                                    'hmac',
                                    '--create',
                                ]))
                            }
                        }, {
                            'local-exec': {
                                'when': 'destroy',
                                'command': ' '.join(map(shlex.quote, [
                                    'python',
                                    config.project_root + '/scripts/provision_credentials.py',
                                    'hmac',
                                    '--destroy',
                                ]))
                            }
                        }
                    ]
                }
            }),
            'aws_kms_key': {
                key.name: {
                    'key_usage': key.usage,
                    'customer_master_key_spec': key.spec,
                }
                for key in kms_keys
            },
            'aws_kms_alias': {
                key.name: {
                    'name': key.alias,
                    'target_key_id': '${aws_kms_key.%s.key_id}' % key.name
                }
                for key in kms_keys
            },
            'aws_dynamodb_table': iif(owns_user_state, {
                'users': {
                    'name': config.dynamo_user_table_name,
                    'billing_mode': 'PAY_PER_REQUEST',
                    'hash_key': UserService.user_key_attribute,
                    'attribute': [
                        {
                            'name': UserService.user_key_attribute,
                            'type': 'S'
                        }
                    ],
                    'ttl': {
                        'attribute_name': UserService.user_ttl_attribute,
                        'enabled': True
                    }
                },
                'access_tokens': {
                    'name': config.dynamo_access_token_table_name,
                    'billing_mode': 'PAY_PER_REQUEST',
                    'hash_key': UserService.access_token_key_attribute,
                    'attribute': [
                        {
                            'name': UserService.access_token_key_attribute,
                            'type': 'S'
                        }
                    ],
                    'ttl': {
                        'attribute_name': UserService.access_token_ttl_attribute,
                        'enabled': True
                    }
                }
            })
        },
    ],
    # The user tables moved here from the main component. Adopt the existing
    # ones during the first apply.
    #
    # FIXME: Remove this section once every deployment has been upgraded
    #        https://github.com/DataBiosphere/azul/issues/8518
    #
    'import': [
        {
            'to': f'aws_dynamodb_table.{name}',
            'id': table_name
        }
        for name, table_name in [
            ('users', config.dynamo_user_table_name),
            ('access_tokens', config.dynamo_access_token_table_name)
        ]
        if owns_user_state and _table_exists(table_name)
    ]
})
