import base64
import hashlib
import json
import logging
import uuid

from azul import config
from azul.es import ESClientFactory
from azul.service.dynamo_data_access import DynamoDataAccessor
from azul.service.elasticsearch_service import ElasticTransformDump
from azul.service.step_function_helper import StepFunctionHelper
from azul.service.user_service import (
    UserService,
    UpdateError,
)

logger = logging.getLogger(__name__)


class CartItemManager:
    """
    Helper functions to handle read/write/update of carts and cart items
    """
    step_function_helper = StepFunctionHelper()

    def __init__(self):
        self.dynamo_accessor = DynamoDataAccessor()
        self.user_service = UserService()

    @staticmethod
    def encode_params(params):
        return base64.urlsafe_b64encode(bytes(json.dumps(params), encoding='utf-8')).decode('utf-8')

    @staticmethod
    def decode_token(token):
        return json.loads(base64.urlsafe_b64decode(token).decode('utf-8'))

    @staticmethod
    def convert_resume_token_to_exclusive_start_key(resume_token: str):
        if resume_token is None:
            return None
        return json.loads(base64.b64decode(resume_token).decode('utf-8'))

    @staticmethod
    def convert_last_evaluated_key_to_resume_token(last_evaluated_key):
        if last_evaluated_key is None:
            return None
        return base64.b64encode(json.dumps(last_evaluated_key).encode('utf-8')).decode('utf-8')

    def create_cart(self, user_id: str, cart_name: str, default: bool) -> str:
        """
        Add a cart to the cart table and return the ID of the created cart
        An error will be raised if the user already has a cart of the same name or
        if a default cart is being created while one already exists.
        """
        query_dict = {'UserId': user_id, 'CartName': cart_name}
        if self.dynamo_accessor.count(table_name=config.dynamo_cart_table_name,
                                      key_conditions=query_dict,
                                      index_name='UserCartNameIndex') > 0:
            raise DuplicateItemError(f'Cart `{cart_name}` already exists')
        cart_id = str(uuid.uuid4())
        if default:
            try:
                self.user_service.update(user_id, default_cart_id=cart_id)
            except UpdateError:
                # As DynamoDB client doesn't differentiate errors caused by
                # failing the key condition ("Key") or the condition expression
                # ("ConditionExpression"). The method will attempt to update
                # the user object again by ensuring that the user object exists
                # before the update.
                self.user_service.get_or_create(user_id)
                try:
                    self.user_service.update(user_id, default_cart_id=cart_id)
                except UpdateError:
                    # At this point, the user already has a default cart.
                    return self.get_default_cart(user_id)['CartId']
        self.dynamo_accessor.insert_item(config.dynamo_cart_table_name,
                                         item={'CartId': cart_id, **query_dict})
        return cart_id

    def get_cart(self, user_id, cart_id):
        cart = self.dynamo_accessor.get_item(config.dynamo_cart_table_name,
                                             keys={'UserId': user_id, 'CartId': cart_id})
        if cart is None:
            raise ResourceAccessError('Cart does not exist')
        return cart

    def get_default_cart(self, user_id):
        user = self.user_service.get_or_create(user_id)
        if user['DefaultCartId'] is None:
            raise ResourceAccessError('Cart does not exist')
        cart = self.dynamo_accessor.get_item(config.dynamo_cart_table_name,
                                             keys={'UserId': user_id, 'CartId': user['DefaultCartId']})
        if cart is None:
            raise ResourceAccessError('Cart does not exist')
        return cart

    def get_or_create_default_cart(self, user_id):
        user = self.user_service.get_or_create(user_id)
        cart_id = user['DefaultCartId'] or self.create_cart(user_id, 'Default Cart', default=True)
        return self.dynamo_accessor.get_item(config.dynamo_cart_table_name,
                                             keys={'UserId': user_id, 'CartId': cart_id})

    def get_user_carts(self, user_id):
        return list(self.dynamo_accessor.query(table_name=config.dynamo_cart_table_name,
                                               key_conditions={'UserId': user_id},
                                               index_name='UserIndex'))

    def delete_cart(self, user_id, cart_id):
        default_cart_id = self.user_service.get_or_create(user_id)['DefaultCartId']
        if default_cart_id == cart_id:
            self.user_service.update(user_id, default_cart_id=None)
        self.dynamo_accessor.delete_by_key(config.dynamo_cart_item_table_name,
                                           {'CartId': cart_id})
        return self.dynamo_accessor.delete_item(config.dynamo_cart_table_name,
                                                {'UserId': user_id, 'CartId': cart_id})

    def update_cart(self, user_id, cart_id, update_attributes, validate_attributes=True):
        """
        Update the attributes of a cart and return the updated item
        Only accepted attributes will be updated and any others will be ignored
        """
        if cart_id is None:
            cart = self.get_or_create_default_cart(user_id)
        else:
            cart = self.get_cart(user_id, cart_id)
        real_cart_id = cart['CartId']
        if validate_attributes:
            accepted_attributes = {'CartName', 'Description'}
            for key in list(update_attributes.keys()):
                if key not in accepted_attributes:
                    del update_attributes[key]

        if 'CartName' in update_attributes.keys():
            matching_carts = list(self.dynamo_accessor.query(table_name=config.dynamo_cart_table_name,
                                                             key_conditions={
                                                                 'UserId': user_id,
                                                                 'CartName': update_attributes['CartName']
                                                             },
                                                             index_name='UserCartNameIndex'))
            # There cannot be more than one matching cart because of the index's keys
            if len(matching_carts) > 0 and matching_carts[0]['CartId'] != real_cart_id:
                raise DuplicateItemError(f'Cart `{update_attributes["CartName"]}` already exists')

        return self.dynamo_accessor.update_item(config.dynamo_cart_table_name,
                                                {'UserId': user_id, 'CartId': real_cart_id},
                                                update_values=update_attributes)

    def create_cart_item_id(self, cart_id, entity_id, entity_type, bundle_uuid, bundle_version):
        item_id = [cart_id, entity_id, bundle_uuid, bundle_version, entity_type]
        return hashlib.sha256('/'.join(item_id).encode('utf-8')).hexdigest()

    def add_cart_item(self, user_id, cart_id, entity_id, entity_type, entity_version):
        """
        Add an item to a cart and return the created item ID
        An error will be raised if the cart does not exist or does not belong to the user
        """
        # TODO: Cart item should have some user readable name
        if cart_id is None:
            cart = self.get_or_create_default_cart(user_id)
        else:
            cart = self.get_cart(user_id, cart_id)
        real_cart_id = cart['CartId']
        if not entity_version:
            # When entity_version is not given, this method will check the data integrity and retrieve the version.
            entity = ESClientFactory.get().get(index=config.es_index_name(entity_type, True),
                                               id=entity_id,
                                               _source=True,
                                               _source_include=['contents.files.uuid',  # data file UUID
                                                                'contents.files.version',  # data file version
                                                                'contents.projects.document_id',  # metadata file UUID
                                                                'contents.samples.document_id',  # metadata file UUID
                                                                ]
                                               )['_source']
            normalized_entity = self.extract_entity_info(entity_type, entity)
            entity_version = normalized_entity['version']
        new_item = self.transform_entity_to_cart_item(real_cart_id, entity_type, entity_id, entity_version)
        self.dynamo_accessor.insert_item(config.dynamo_cart_item_table_name, new_item)
        return new_item['CartItemId']

    @staticmethod
    def extract_entity_info(entity_type: str, entity):
        normalized_entity = dict(uuid=None, version=None)
        content = entity['contents'][entity_type][0]
        if entity_type == 'files':
            normalized_entity.update(dict(uuid=content['uuid'],
                                          version=content['version']))
        elif entity_type in ('samples', 'projects'):
            print(content)
            normalized_entity['uuid'] = content['document_id']
        else:
            raise ValueError('entity_type must be one of files, samples, or projects')
        return normalized_entity

    @staticmethod
    def transform_entity_to_cart_item(cart_id: str, entity_type: str, entity_id: str, entity_version: str):
        return {
            'CartItemId': f'{entity_id}:{entity_version or ""}',  # Range Key
            'CartId': cart_id,  # Hash Key
            'EntityId': entity_id,
            'EntityVersion': entity_version,
            'EntityType': entity_type
        }

    def get_cart_items(self, user_id, cart_id):
        """
        Get all items in a cart
        An error will be raised if the cart does not exist or does not belong to the user
        """
        if cart_id is None:
            cart = self.get_or_create_default_cart(user_id)
        else:
            cart = self.get_cart(user_id, cart_id)
        real_cart_id = cart['CartId']
        return list(self.dynamo_accessor.query(table_name=config.dynamo_cart_item_table_name,
                                               key_conditions={'CartId': real_cart_id}))

    def get_cart_item_count(self, user_id, cart_id):
        if cart_id is None:
            cart = self.get_or_create_default_cart(user_id)
        else:
            cart = self.get_cart(user_id, cart_id)
        real_cart_id = cart['CartId']
        return self.dynamo_accessor.count(table_name=config.dynamo_cart_item_table_name,
                                          key_conditions={'CartId': real_cart_id},
                                          select=['EntityType'])

    def get_paginable_cart_items(self, user_id, cart_id,
                                 page_size: int = 20,
                                 exclusive_start_key=None,
                                 resume_token=None):
        """
        Get cart items (with pagination).

        :param user_id: User ID
        :param cart_id: Cart ID (UUID)
        :param page_size: Requested Query Limit
        :param exclusive_start_key: the exclusive start key (like an offset in
                                    MySQL), recommended for in-code operations
        :param resume_token: the base64-encoded string of exclusive_start_key
                             recommended for using with external clients
        :return: Return a dictionary of search result with ``items`` (cart
                 items), ``last_evaluated_key`` (last evaluated key, null if
                 it is the last page), ``resume_token`` (the base64-encoded
                 string of ``last_evaluated_key``) and ``page_length`` (the
                 returning page size)

        The ``page_length`` attribute in the returning dictionary is designed
        to provide the actual number of returned items as DynamoDB may return
        less than what the client asks because of the the maximum size of 1 MB
        for query. See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html.

        ``exclusive_start_key`` and ``resume_token`` must not be defined at
        the same time. Otherwise, the method will throw ``ValueError`.
        """
        if exclusive_start_key and resume_token:
            raise ValueError('exclusive_start_key or resume_token must be defined at the same time.')
        if resume_token is not None:
            exclusive_start_key = self.convert_resume_token_to_exclusive_start_key(resume_token)
        if cart_id is None:
            cart = self.get_or_create_default_cart(user_id)
        else:
            cart = self.get_cart(user_id, cart_id)
        real_cart_id = cart['CartId']
        page_query = dict(
            table_name=config.dynamo_cart_item_table_name,
            key_conditions={'CartId': real_cart_id},
            exclusive_start_key=exclusive_start_key,
            select=['CartItemId',
                    'EntityId',
                    'EntityVersion',
                    'EntityType'],
            limit=page_size
        )
        page = next(self.dynamo_accessor.make_query(**page_query))
        items = [item for item in page.items]
        last_evaluated_key = page.last_evaluated_key
        return dict(items=items,
                    last_evaluated_key=last_evaluated_key,
                    resume_token=self.convert_last_evaluated_key_to_resume_token(last_evaluated_key),
                    page_length=len(items))

    def delete_cart_item(self, user_id, cart_id, item_id):
        """
        Delete an item from a cart and return the deleted item if it exists, None otherwise
        An error will be raised if the cart does not exist or does not belong to the user
        """
        if cart_id is None:
            cart = self.get_or_create_default_cart(user_id)
        else:
            cart = self.get_cart(user_id, cart_id)
        real_cart_id = cart['CartId']
        return self.dynamo_accessor.delete_item(config.dynamo_cart_item_table_name,
                                                keys={'CartId': real_cart_id, 'CartItemId': item_id})

    def transform_hit_to_cart_item(self, hit, entity_type, cart_id):
        """
        Transform a hit from ES to the schema for the cart item table
        """
        entity = self.extract_entity_info(entity_type, hit)
        return self.transform_entity_to_cart_item(cart_id, entity_type, entity['uuid'], entity['version'])

    def start_batch_cart_item_write(self, user_id, cart_id, entity_type, filters, item_count, batch_size):
        """
        Trigger the job that will write the cart items and return a token to be used to check the job status
        """
        if cart_id is None:
            cart = self.get_or_create_default_cart(user_id)
        else:
            cart = self.get_cart(user_id, cart_id)
        real_cart_id = cart['CartId']
        execution_id = str(uuid.uuid4())
        execution_input = {
            'filters': filters,
            'entity_type': entity_type,
            'cart_id': real_cart_id,
            'item_count': item_count,
            'batch_size': batch_size
        }
        self.step_function_helper.start_execution(config.cart_item_state_machine_name,
                                                  execution_name=execution_id,
                                                  execution_input=execution_input)
        return self.encode_params({'execution_id': execution_id})

    def get_batch_cart_item_write_status(self, token):
        params = self.decode_token(token)
        execution_id = params['execution_id']
        return self.step_function_helper.describe_execution(config.cart_item_state_machine_name, execution_id)['status']

    def write_cart_item_batch(self, entity_type, filters, cart_id, batch_size, search_after):
        """
        Query ES for one page of items matching the entity type and filters and return
        the number of items written and the search_after for the next page
        """
        es_td = ElasticTransformDump()
        hits, next_search_after = es_td.transform_cart_item_request(entity_type=entity_type,
                                                                    filters=filters,
                                                                    search_after=search_after,
                                                                    size=batch_size)
        self.dynamo_accessor.batch_write(config.dynamo_cart_item_table_name,
                                         [self.transform_hit_to_cart_item(hit, entity_type, cart_id) for hit in hits])
        return len(hits), next_search_after


class ResourceAccessError(Exception):

    def __init__(self, msg):
        self.msg = msg


class DuplicateItemError(Exception):

    def __init__(self, msg):
        self.msg = msg
