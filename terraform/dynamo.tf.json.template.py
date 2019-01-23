from azul import config
from azul.template import emit

emit(
    {
        "resource": [
            {
                "aws_dynamodb_table": {
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
                    }
                }
            }
        ]
    }
)
