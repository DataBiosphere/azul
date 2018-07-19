import logging
import os
import sys
import tempfile
from unittest.mock import patch

import boto3
from hca.dss import DSSClient
from urllib3 import Timeout

from azul import config
from azul.vendored.frozendict import frozendict

logger = logging.getLogger(__name__)


def main(argv):
    logging.basicConfig(level=logging.INFO)
    import argparse
    parser = argparse.ArgumentParser(description='Subscribe indexer lambda to bundle events from DSS')
    parser.add_argument('--unsubscribe', '-U', dest='subscribe', action='store_false', default=True)
    parser.add_argument('--shared', '-s', dest='shared', action='store_true', default=False)
    options = parser.parse_args(argv)
    dss_client = DSSClient()
    dss_client.host = config.dss_endpoint
    dss_client.timeout_policy = Timeout(connect=10, read=40)
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
    subscriptions = freeze(response['subscriptions'])
    plugin = config.plugin()
    if options.subscribe:
        new_subscription = freeze(dict(replica='aws',
                                       es_query=plugin.dss_subscription_query,
                                       callback_url="https://" + config.api_lambda_domain('indexer') + "/"))
    else:
        new_subscription = None
    for subscription in subscriptions:
        if new_subscription and new_subscription.items() <= subscription.items():
            logging.info('Already subscribed: %r', thaw(subscription))
            new_subscription = None
        else:
            logging.log(logging.WARNING if options.subscribe else logging.INFO,
                        'Removing subscription: %r', thaw(subscription))
            dss_client.delete_subscription(uuid=subscription['uuid'], replica=subscription['replica'])
    if new_subscription:
        subscription = thaw(new_subscription)
        response = dss_client.put_subscription(**subscription)
        subscription['uuid'] = response['uuid']
        logging.info('Registered subscription %r.', subscription)


def freeze(x):
    if isinstance(x, dict):
        return frozendict((k, freeze(v)) for k, v in x.items())
    elif isinstance(x, list):
        return [freeze(v) for v in x]
    elif isinstance(x, (bool, str, int, float)) or x is None:
        return x
    else:
        assert False, f'Cannot handle values of type {type(x)}'


def thaw(x):
    if isinstance(x, frozendict):
        return {k: thaw(v) for k, v in x.items()}
    elif isinstance(x, list):
        return [thaw(v) for v in x]
    elif isinstance(x, (bool, str, int, float)) or x is None:
        return x
    else:
        assert False, f'Cannot handle values of type {type(x)}'


if __name__ == '__main__':
    main(sys.argv[1:])
