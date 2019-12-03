from azul import config
from azul.deployment import emit_tf
from azul.version_service import VersionService

emit_tf(
    {
        "resource": [
            {
                "aws_dynamodb_table": {
                    "users_table": {
                        "name": config.dynamo_user_table_name,
                        "billing_mode": "PAY_PER_REQUEST",
                        "hash_key": "UserId",
                        "attribute": [
                            {
                                "name": "UserId",
                                "type": "S"
                            }
                        ]
                    },
                    "carts_table": {
                        "name": config.dynamo_cart_table_name,
                        "billing_mode": "PAY_PER_REQUEST",
                        "hash_key": "UserId",
                        "range_key": "CartId",
                        "attribute": [
                            {
                                "name": "CartId",
                                "type": "S"
                            },
                            {
                                "name": "UserId",
                                "type": "S"
                            },
                            {
                                "name": "CartName",
                                "type": "S"
                            }
                        ],
                        "global_secondary_index": [
                            {
                                "name": "UserIndex",
                                "hash_key": "UserId",
                                "projection_type": "ALL"
                            },
                            {
                                "name": "UserCartNameIndex",
                                "hash_key": "UserId",
                                "range_key": "CartName",
                                "projection_type": "ALL"
                            }
                        ]
                    },
                    "cart_items_table": {
                        "name": config.dynamo_cart_item_table_name,
                        "billing_mode": "PAY_PER_REQUEST",
                        "hash_key": "CartId",
                        "range_key": "CartItemId",
                        "attribute": [
                            {
                                "name": "CartItemId",
                                "type": "S"
                            },
                            {
                                "name": "CartId",
                                "type": "S"
                            }
                        ]
                    },
                    "versions_table": {
                        "name": config.dynamo_object_version_table_name,
                        "billing_mode": "PAY_PER_REQUEST",
                        "hash_key": VersionService.key_name,
                        "attribute": [
                            {
                                "name": VersionService.key_name,
                                "type": "S"
                            }
                        ]
                    }
                }
            }
        ]
    }
)
