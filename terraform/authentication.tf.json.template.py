import shlex

from azul import config
from azul.deployment import emit_tf

emit_tf({
    "resource": [
        {
            "google_service_account": {
                "indexer": {
                    # We set the count to 0 to ensure that the destroy provisioner runs.
                    # See https://www.terraform.io/docs/provisioners/index.html#destroy-time-provisioners
                    "count": 1 if config.subscribe_to_dss else 0,
                    "project": "${local.google_project}",
                    "account_id": config.qualified_resource_name('indexer'),
                    "display_name": f"Azul indexer in {config.deployment_stage}",
                    "provisioner": [
                        {
                            "local-exec": {
                                "command": ' '.join(map(shlex.quote, [
                                    "python",
                                    config.project_root + "/scripts/provision_credentials.py",
                                    "google-key",
                                    "--build",
                                    "${google_service_account.indexer[0].email}",
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
                                    "${google_service_account.indexer[0].email}",
                                ]))
                            }
                        }
                    ]
                }
            },
            "null_resource":{
                "hmac-secret": {
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
