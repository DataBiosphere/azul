import logging

from azul import (
    config,
)
from azul.service.cart_item_manager import (
    CartItemManager,
)
from azul.service.collection_data_access import (
    CollectionDataAccess,
    UnauthorizedClientAccessError,
)

logger = logging.getLogger(__name__)


class CartExportService:

    def __init__(self):
        self.cart_item_manager = CartItemManager()

    def export(self,
               export_id: str,
               user_id: str,
               cart_id: str,
               access_token: str,
               collection_uuid: str,
               collection_version: str,
               resume_token: str = None):
        content = self.get_content(user_id, cart_id, collection_uuid, collection_version, resume_token)
        client = CollectionDataAccess(access_token)
        items = content['items']
        if resume_token is None:
            cart = self.cart_item_manager.get_cart(user_id, cart_id)
            cart_name = cart['CartName']
            description = f"Exported from Cart {cart_id} in the Data Browser"
            collection = client.create(collection_uuid, cart_name, description, collection_version, items)
        else:
            try:
                collection = client.append(collection_uuid, collection_version, items)
                # The returning collection will contain the new version of the collection.
            except UnauthorizedClientAccessError:
                # DSS may deny the access to the collection API when the given
                # access token expires before the cart export finishes. In this
                # case, the export job can be resumed with a new access token.
                logger.error('Export %s: DSS denied access to the collection API.', export_id)
                raise ExpiredAccessTokenError()
        return dict(collection=collection,
                    resume_token=content['resume_token'],
                    exported_item_count=len(items))

    def get_content(self, user_id, cart_id, collection_uuid: str, collection_version: str, resume_token: str = None):
        batch_size = min(config.cart_export_max_batch_size, 1000)

        if (collection_uuid and not collection_version) or (not collection_uuid and collection_version):
            raise ValueError('Both collection UUID and version must be given at the same time.')

        page = self.cart_item_manager.get_paginable_cart_items(user_id=user_id,
                                                               cart_id=cart_id,
                                                               page_size=batch_size,
                                                               resume_token=resume_token)

        # As specimens, projects and files are considered as files on DSS and
        # a cart does not contain bundles and collections, the type of each
        # content item will be hard-coded to "file".
        content_items = [
            dict(type='file', uuid=cart_item['EntityId'], version=cart_item['EntityVersion'])
            for cart_item in page['items']
        ]

        if page['last_evaluated_key'] is None:
            next_resume_token = None
        else:
            next_resume_token = self.cart_item_manager.convert_last_evaluated_key_to_resume_token(
                page['last_evaluated_key']
            )

        return dict(items=content_items, resume_token=next_resume_token)


class ExpiredAccessTokenError(RuntimeError):
    pass
