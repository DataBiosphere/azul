"""
https://stackoverflow.com/a/24478809/4171119
"""

import logging
import sys
import unittest

from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def print_suite(suite):
    if hasattr(suite, '__iter__'):
        for x in suite:
            print_suite(x)
    else:
        print(suite)


if __name__ == '__main__':
    configure_script_logging(log)
    loader = unittest.defaultTestLoader
    start_dir = sys.argv[1]
    suite = loader.discover(start_dir)
    print_suite(suite)
    if loader.errors:
        for error in loader.errors:
            log.error(error)
        sys.exit(1)
