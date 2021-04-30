from azul import (
    config,
)
from azul.terra import (
    UserAuthTDRClient,
)
from azul.terraform import (
    emit_tf,
)
from azul.version_service import (
    VersionService,
)

emit_tf(
    {
        "resource": [
            {
                "aws_dynamodb_table": {
                    "object_versions": {
                        "name": config.dynamo_object_version_table_name,
                        "billing_mode": "PAY_PER_REQUEST",
                        "hash_key": VersionService.key_name,
                        "attribute": [
                            {
                                "name": VersionService.key_name,
                                "type": "S"
                            }
                        ]
                    },
                    "tdr_user_snapshot_cache": {
                        "name": config.dynamo_tdr_user_snapshots_table_name,
                        "billing_mode": "PAY_PER_REQUEST",
                        "hash_key": UserAuthTDRClient.ddb_key_name,
                        "attribute": [
                            {
                                "name": UserAuthTDRClient.ddb_key_name,
                                "type": "S"
                            }
                        ],
                        "ttl": {
                            "attribute_name": UserAuthTDRClient.ddb_ttl_name,
                            "enabled": True
                        }
                    }
                }
            }
        ]
    }
)
