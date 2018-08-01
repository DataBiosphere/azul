import logging
import os
import sys
import tempfile
from unittest.mock import patch
import boto3

from azul import config
from azul.json_freeze import freeze, thaw

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
                subscribe(options, dss_client)
    else:
        raise NotImplementedError("https://github.com/DataBiosphere/azul/issues/110")


def subscribe(options, dss_client):
    response = dss_client.get_subscriptions(replica='aws')
    dss_subscriptions = freeze(response['subscriptions'])
    new_subscriptions = _make_subscriptions(options.subscribe)

    duplicate_subscription = []
    for subscription in dss_subscriptions:
        matching_subscription = next((new_subscription for new_subscription in new_subscriptions
                                      if new_subscription <= subscription))
        if matching_subscription:
            logging.info('Already subscribed: %r', thaw(subscription))
            duplicate_subscription.append(matching_subscription)
        else:
            logging.info('Removing subscription: %r', thaw(subscription))
            dss_client.delete_subscription(uuid=subscription['uuid'], replica=subscription['replica'])
    new_subscriptions = [subs for subs in new_subscriptions if subs not in duplicate_subscription]

    for subscription in new_subscriptions:
        dss_subscription = thaw(subscription)
        response = dss_client.put_subscription(**dss_subscription)
        dss_subscription['uuid'] = response['uuid']
        logging.info('Registered subscription %r.', dss_subscription)


def _make_subscriptions(subscribe):
    if subscribe:
        plugin = config.plugin()
        base_url = "https://" + config.api_lambda_domain('indexer')
        return [freeze(dict(replica='aws', es_query=query, callback_url=base_url + path))
                for query, path in [(plugin.dss_subscription_query, '/'),
                                    (plugin.dss_deletion_subscription_query, '/delete')]]
    else:
        return []


if __name__ == '__main__':
    main(sys.argv[1:])
