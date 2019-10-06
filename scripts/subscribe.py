#! /usr/bin/env python3

import logging
import sys

from azul import (
    config,
    subscription,
)
from azul.dss import shared_dss_credentials
from azul.logging import configure_script_logging

logger = logging.getLogger(__name__)


def main(argv):
    configure_script_logging(logger)
    import argparse
    parser = argparse.ArgumentParser(description='Subscribe indexer lambda to bundle events from DSS')
    parser.add_argument('--unsubscribe', '-U', dest='subscribe', action='store_false', default=True)
    parser.add_argument('--personal', '-p', dest='shared', action='store_false', default=True,
                        help="Do not use the shared credentials of the Google service account that represents the "
                             "current deployment, but instead use personal credentials for authenticating to the DSS. "
                             "When specifying this option you will need to a) run `hca dss login` prior to running "
                             "this script or b) set GOOGLE_APPLICATION_CREDENTIALS to point to another service "
                             "account's credentials. Note that this implies that the resulting DSS subscription will "
                             "be owned by a) you or b) the other service account and that only a) you or b) someone "
                             "in possession of those credentials can modify the subscription in the future. This is "
                             "typically not what you'd want.")
    options = parser.parse_args(argv)
    dss_client = config.dss_client()

    if options.shared:
        with shared_dss_credentials():
            subscription.manage_subscriptions(dss_client, subscribe=options.subscribe)
    else:
        subscription.manage_subscriptions(dss_client, subscribe=options.subscribe)


if __name__ == '__main__':
    main(sys.argv[1:])
