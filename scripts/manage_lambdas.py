import argparse
import logging

from azul.lambdas import (
    Lambdas,
)
from azul.logging import (
    configure_script_logging,
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    configure_script_logging(logger)
    parser = argparse.ArgumentParser(description='Enables or disables the lambdas in the current deployment.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--enable', dest='enabled', action='store_true', default=None)
    group.add_argument('--disable', dest='enabled', action='store_false')
    args = parser.parse_args()
    assert args.enabled is not None
    Lambdas().manage_lambdas(args.enabled)
