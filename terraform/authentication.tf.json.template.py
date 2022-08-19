import shlex

from azul import (
    config,
)
from azul.terraform import (
    emit_tf,
)

emit_tf({
    "resource": [
        {
            "google_service_account": {
                'azul' + service_account.value: {
                    "project": "${local.google_project}",
                    "account_id": service_account.id,
                    "display_name": service_account.id,
                    "description": f"Azul service account in {config.deployment_stage}",
                    "provisioner": [
                        {
                            "local-exec": {
                                "command": ' '.join(map(shlex.quote, [
                                    "python",
                                    config.project_root + "/scripts/provision_credentials.py",
                                    "google-key",
                                    "--build",
                                    "${self.email}",
                                    service_account.secret_name
                                ]))
                            }
                        }, {
                            "local-exec": {
                                "when": "destroy",
                                "command": ' '.join(map(shlex.quote, [
                                    "python",
                                    config.project_root + "/scripts/provision_credentials.py",
                                    "google-key",
                                    "--destroy",
                                    "${self.email}",
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
                    "google_project_iam_member": {
                        "azul": {
                            "project": "${local.google_project}",
                            "role": "${google_project_iam_custom_role.azul.id}",
                            "member": "serviceAccount:${google_service_account.azul.email}"
                        },
                    },
                    "google_project_iam_custom_role": {
                        "azul": {
                            "role_id": f"azul_{config.deployment_stage}_{config.deployment_incarnation}",
                            "title": f"azul_{config.deployment_stage}",
                            "permissions": [
                                "bigquery.jobs.create",
                                "bigquery.reservations.get",
                                "bigquery.capacityCommitments.get",
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
            "null_resource": {
                "hmac_secret": {
                    "provisioner": [
                        {
                            "local-exec": {
                                "command": ' '.join(map(shlex.quote, [
                                    "python",
                                    config.project_root + "/scripts/provision_credentials.py",
                                    "hmac-key",
                                    "--build",
                                ]))
                            }
                        }, {
                            "local-exec": {
                                "when": "destroy",
                                "command": ' '.join(map(shlex.quote, [
                                    "python",
                                    config.project_root + "/scripts/provision_credentials.py",
                                    "hmac-key",
                                    "--destroy",
                                ]))
                            }
                        }
                    ]
                }
            }
        },
    ]
})
