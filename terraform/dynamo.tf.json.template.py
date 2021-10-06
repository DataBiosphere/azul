from azul import (
    config,
)
from azul.service.source_service import (
    SourceCacheService,
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
                    "sources_cache_by_auth": {
                        "name": config.dynamo_sources_cache_table_name,
                        "billing_mode": "PAY_PER_REQUEST",
                        "hash_key": SourceCacheService.key_attribute,
                        "attribute": [
                            {
                                "name": SourceCacheService.key_attribute,
                                "type": "S"
                            }
                        ],
                        "ttl": {
                            "attribute_name": SourceCacheService.ttl_attribute,
                            "enabled": True
                        }
                    }
                }
            }
        ]
    }
)
