from azul import config
from azul.template import emit

emit(
    {
        "resource": [
            {
                "aws_dynamodb_table": {
                    "carts-table": {
                        "name": config.dynamo_cart_table_name,
                        "read_capacity": config.dynamo_read_capacity,
                        "write_capacity": config.dynamo_write_capacity,
                        "hash_key": "CartId",
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
                        "global_secondary_index": {
                            "name": "UserCartNameIndex",
                            "hash_key": "UserId",
                            "range_key": "CartName",
                            "read_capacity": config.dynamo_read_capacity,
                            "write_capacity": config.dynamo_write_capacity,
                            "projection_type": "ALL"
                        }
                    },
                    "cart-items-table": {
                        "name": config.dynamo_cart_item_table_name,
                        "read_capacity": config.dynamo_read_capacity,
                        "write_capacity": config.dynamo_write_capacity,
                        "hash_key": "CartItemId",
                        "attribute": [
                            {
                                "name": "CartItemId",
                                "type": "S"
                            },
                            {
                                "name": "CartId",
                                "type": "S"
                            }
                        ],
                        "global_secondary_index": {
                            "name": "CartIdIndex",
                            "hash_key": "CartId",
                            "read_capacity": config.dynamo_read_capacity,
                            "write_capacity": config.dynamo_write_capacity,
                            "projection_type": "ALL"
                        }
                    }
                }
            },
        ]
    }
)
