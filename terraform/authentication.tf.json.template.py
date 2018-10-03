from azul import config
from azul.template import emit

emit({
    "resource": [
        {
            "google_service_account": {
                "indexer": {
                    "project": "${local.google_project}",
                    "account_id": config.qualified_resource_name('indexer'),
                    "display_name": f"Azul indexer in {config.deployment_stage}"
                }
            }
        },
        {
            "google_service_account_key": {
                "indexer": {
                    "service_account_id": "${google_service_account.indexer.name}"
                }
            }
        },
        {
            "aws_secretsmanager_secret": {
                "indexer_google_service_account": {
                    "name": config.google_service_account('indexer'),
                    "recovery_window_in_days": 0  # force immediate deletion
                }
            },
            "aws_secretsmanager_secret_version": {
                "indexer_google_service_account": {
                    "secret_id": "${aws_secretsmanager_secret.indexer_google_service_account.id}",
                    "secret_string": "${base64decode(google_service_account_key.indexer.private_key)}"
                }
            }
        }
    ]
} if config.subscribe_to_dss else None)
