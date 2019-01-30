from typing import Optional

from azul import config
from azul.service.responseobjects.dynamo_data_access import DynamoDataAccessor


class UserService:

    def __init__(self):
        self.dynamo_accessor = DynamoDataAccessor()

    def get(self, user_id:str):
        users = self.dynamo_accessor.query(table_name=config.dynamo_user_table_name,
                                           key_conditions={'UserId': user_id},
                                           consistent_read=True)
        try:
            return next(users)
        except StopIteration:
            self.dynamo_accessor.insert_item(table_name=config.dynamo_user_table_name,
                                             item={'UserId': user_id, 'DefaultCartId': None})
            users = self.dynamo_accessor.query(table_name=config.dynamo_user_table_name,
                                               key_conditions={'UserId': user_id},
                                               consistent_read=True)
            return next(users)

    def update(self, user_id:str, default_cart_id):
        return self.dynamo_accessor.update_item(table_name=config.dynamo_user_table_name,
                                                keys={'UserId': user_id},
                                                update_values={'DefaultCartId': default_cart_id})
