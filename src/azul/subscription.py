#! /usr/bin/env python3

import logging

from azul import (
    config,
    deployment,
)
from azul.json_freeze import (
    freeze,
    thaw,
)
from azul.plugin import Plugin

logger = logging.getLogger(__name__)


def manage_subscriptions(dss_client, subscribe=True):
    response = call_client(dss_client.get_subscriptions, replica='aws')
    current_subscriptions = freeze(response['subscriptions'])

    key, key_id = deployment.aws.get_hmac_key_and_id()

    if subscribe:
        plugin = Plugin.load()
        base_url = config.indexer_endpoint()
        prefix = config.dss_query_prefix
        new_subscriptions = [freeze(dict(replica='aws',
                                         es_query=query,
                                         callback_url=base_url + path,
                                         hmac_key_id=key_id))
                             for query, path in [(plugin.dss_subscription_query(prefix), '/'),
                                                 (plugin.dss_deletion_subscription_query(prefix), '/delete')]]
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
            call_client(dss_client.delete_subscription,
                        uuid=subscription['uuid'],
                        replica=subscription['replica'])

    for subscription in new_subscriptions:
        subscription = thaw(subscription)
        response = dss_client.put_subscription(**subscription, hmac_secret_key=key)
        subscription['uuid'] = response['uuid']
        logger.info('Registered subscription %r.', subscription)


def call_client(method, *args, **kwargs):
    # Work around https://github.com/HumanCellAtlas/data-store/issues/1957
    if 'subscription_type' in method.parameters:
        kwargs['subscription_type'] = 'elasticsearch'
    # noinspection PyArgumentList
    return method(*args, **kwargs)
