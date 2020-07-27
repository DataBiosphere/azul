import shlex

from azul import (
    config,
)
from azul.deployment import (
    emit_tf,
)

emit_tf({
    "resource": [
        {
            "google_service_account": {
                "indexer": {
                    "project": "${local.google_project}",
                    "account_id": config.indexer_google_service_account,
                    "display_name": f"Azul indexer in {config.deployment_stage}",
                    "provisioner": [
                        {
                            "local-exec": {
                                "command": ' '.join(map(shlex.quote, [
                                    "python",
                                    config.project_root + "/scripts/provision_credentials.py",
                                    "google-key",
                                    "--build",
                                    "${self.email}",
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
                                ]))
                            }
                        }
                    ]
                }
            },
            "null_resource": {
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
