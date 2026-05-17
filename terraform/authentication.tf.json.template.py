import shlex

from azul import (
    config,
)
from azul.infra.terraform import (
    emit_tf,
)

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
                if config.is_tdr_enabled() else
                {}
            ),
            'null_resource': {
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
            },
            'aws_kms_key': {
                key.name: {
                    'key_usage': key.usage,
                    'customer_master_key_spec': key.spec,
                }
                for key in config.kms_keys
            },
            'aws_kms_alias': {
                key.name: {
                    'name': key.alias,
                    'target_key_id': '${aws_kms_key.%s.key_id}' % key.name
                }
                for key in config.kms_keys
            }
        },
    ]
})
