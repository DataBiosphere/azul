#! /usr/bin/env python3

import logging
import os
import sys
import tempfile
from unittest.mock import patch

import boto3

from azul import config
from azul.json_freeze import freeze, thaw
from azul.plugin import Plugin

logger = logging.getLogger(__name__)


def main(argv):
    logging.basicConfig(level=logging.INFO)
    import argparse
    parser = argparse.ArgumentParser(description='Subscribe indexer lambda to bundle events from DSS')
    parser.add_argument('--unsubscribe', '-U', dest='subscribe', action='store_false', default=True)
    parser.add_argument('--shared', '-s', dest='shared', action='store_true', default=False)
    options = parser.parse_args(argv)
    dss_client = config.dss_client()

    if options.shared:
        sm = boto3.client('secretsmanager')
        creds = sm.get_secret_value(SecretId=config.google_service_account('indexer'))
        with tempfile.NamedTemporaryFile(mode='w+') as f:
            f.write(creds['SecretString'])
            f.flush()
            with patch.dict(os.environ, GOOGLE_APPLICATION_CREDENTIALS=f.name):
                subscribe(options.subscribe, dss_client)
    else:
        raise NotImplementedError("https://github.com/DataBiosphere/azul/issues/110")


def subscribe(dss_client, subscribe=True):
    response = dss_client.get_subscriptions(replica='aws')
    current_subscriptions = freeze(response['subscriptions'])

    if subscribe:
        plugin = Plugin.load()
        base_url = config.indexer_endpoint()
        prefix = config.dss_query_prefix
        new_subscriptions = [freeze(dict(replica='aws', es_query=query, callback_url=base_url + path))
                             for query, path in [(plugin.dss_subscription_query(prefix), '/'),
                                                 (plugin.dss_deletion_subscription_query(prefix), '/delete')]]
    else:
        new_subscriptions = []

    matching_subscriptions = []
    for subscription in current_subscriptions:
        # Note the use of <= to allow for the fact that DSS returns subscriptions with additional attributes, more
        # than were originally supplied. If the subscription returned by DSS is a superset of the subscription we want
        # to create, we can skip the update.
        matching_subscription = next((new_subscription for new_subscription in new_subscriptions
                                      if new_subscription.items() <= subscription.items()), None)
        if matching_subscription:
            logging.info('Already subscribed: %r', thaw(subscription))
            matching_subscriptions.append(matching_subscription)
        else:
            logging.info('Removing subscription: %r', thaw(subscription))
            dss_client.delete_subscription(uuid=subscription['uuid'], replica=subscription['replica'])

    for subscription in new_subscriptions:
        if subscription not in matching_subscriptions:
            subscription = thaw(subscription)
            response = dss_client.put_subscription(**subscription)
            subscription['uuid'] = response['uuid']
            logging.info('Registered subscription %r.', subscription)


if __name__ == '__main__':
    main(sys.argv[1:])
