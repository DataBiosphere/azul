#! /usr/bin/env python3

import logging
import os
import sys
import tempfile
from unittest.mock import patch

import boto3

from azul import config, subscription

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
        creds = sm.get_secret_value(SecretId=config.secrets_manager_secret_name('indexer', 'google_service_account'))
        with tempfile.NamedTemporaryFile(mode='w+') as f:
            f.write(creds['SecretString'])
            f.flush()
            with patch.dict(os.environ, GOOGLE_APPLICATION_CREDENTIALS=f.name):
                subscription.manage_subscriptions(dss_client, subscribe=options.subscribe)
    else:
        raise NotImplementedError("https://github.com/DataBiosphere/azul/issues/110")


if __name__ == '__main__':
    main(sys.argv[1:])
