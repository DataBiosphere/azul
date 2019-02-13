from azul import config
from azul.service.responseobjects.dynamo_data_access import DynamoDataAccessor, UpdateItemError


class UserService:

    def __init__(self):
        self.dynamo_accessor = DynamoDataAccessor()

    def get_or_create(self, user_id:str):
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
        update_conditions = {}
        if default_cart_id is not None:
            update_conditions['DefaultCartId'] = None
        try:
            return self.dynamo_accessor.update_item(table_name=config.dynamo_user_table_name,
                                                    keys={'UserId': user_id},
                                                    update_values={'DefaultCartId': default_cart_id},
                                                    conditions=update_conditions)
        except UpdateItemError:
            raise UpdateError(user_id, default_cart_id)


class UpdateError(RuntimeError):
    pass
