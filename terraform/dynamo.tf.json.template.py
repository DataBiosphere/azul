from azul import (
    config,
)
from azul.indexer.cache_service import (
    CacheService,
)
from azul.infra.terraform import (
    emit_tf,
)
from azul.service.source_service import (
    SourceService,
)

emit_tf(
    {
        "resource": [
            {
                "aws_dynamodb_table": {
                    "sources_cache_by_auth": {
                        "name": config.dynamo_sources_cache_table_name,
                        "billing_mode": "PAY_PER_REQUEST",
                        "hash_key": SourceService.key_attribute,
                        "attribute": [
                            {
                                "name": SourceService.key_attribute,
                                "type": "S"
                            }
                        ],
                        "ttl": {
                            "attribute_name": SourceService.ttl_attribute,
                            "enabled": True
                        }
                    },
                    "object_cache": {
                        "name": config.dynamo_object_cache_table_name,
                        "billing_mode": "PAY_PER_REQUEST",
                        "hash_key": CacheService._key_attribute,
                        "attribute": [
                            {
                                "name": CacheService._key_attribute,
                                "type": "S"
                            }
                        ],
                        "ttl": {
                            "attribute_name": CacheService._ttl_attribute,
                            "enabled": True
                        }
                    }
                }
            }
        ],
        # The user tables moved to the `base` component, which imports them
        #
        # FIXME: Remove this section once every deployment has been upgraded
        #        https://github.com/DataBiosphere/azul/issues/8518
        #
        "removed": [
            {
                "from": f"aws_dynamodb_table.{name}",
                "lifecycle": {
                    "destroy": False
                }
            }
            for name in ("users", "access_tokens")
        ]
    }
)
