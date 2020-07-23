#! /usr/bin/env python3

import logging

from furl import furl
from hca.dss import DSSClient

from azul import (
    config,
    deployment,
)
from azul.json_freeze import (
    freeze,
    thaw,
)
from azul.plugins import RepositoryPlugin

logger = logging.getLogger(__name__)


def manage_subscriptions(plugin: RepositoryPlugin, dss_client: DSSClient, subscribe=True):
    response = dss_client.get_subscriptions(replica='aws',
                                            subscription_type='elasticsearch')
    current_subscriptions = freeze(response['subscriptions'])

    key, key_id = deployment.aws.get_hmac_key_and_id()

    if subscribe:
        base_url = config.indexer_endpoint()
        prefix = config.dss_query_prefix
        new_subscriptions = [
            freeze(dict(replica='aws',
                        es_query=query(prefix),
                        callback_url=furl(url=base_url,
                                          path=(config.default_catalog, action)),
                        hmac_key_id=key_id))
            for query, action in [
                (plugin.dss_subscription_query, 'add'),
                (plugin.dss_deletion_subscription_query, 'delete')
            ]
        ]
    else:
        new_subscriptions = []

    for subscription in current_subscriptions:
        # Note the use of <= to allow for the fact that DSS returns subscriptions with additional attributes, more
        # than were originally supplied. If the subscription returned by DSS is a superset of the subscription we want
        # to create, we can skip the update.
        matching_subscription = next((new_subscription for new_subscription in new_subscriptions
                                      if new_subscription.items() <= subscription.items()), None)
        if matching_subscription:
            logger.info('Already subscribed: %r', thaw(subscription))
            new_subscriptions.remove(matching_subscription)
        else:
            subscription = thaw(subscription)
            logger.info('Removing stale subscription: %r', subscription)
            dss_client.delete_subscription(uuid=subscription['uuid'],
                                           replica=subscription['replica'],
                                           subscription_type='elasticsearch')

    for subscription in new_subscriptions:
        subscription = thaw(subscription)
        response = dss_client.put_subscription(**subscription, hmac_secret_key=key)
        subscription['uuid'] = response['uuid']
        logger.info('Registered subscription %r.', subscription)
