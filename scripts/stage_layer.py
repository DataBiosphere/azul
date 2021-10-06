"""
Package runtime dependencies and stage the resulting ZIP archive in S3 so that
Terraform can then provision a Lambda layer that's shared by all functions.
"""

from argparse import (
    ArgumentParser,
)
import logging
import sys

from azul.lambda_layer import (
    DependenciesLayer,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def main(argv):
    parser = ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    layer = DependenciesLayer()
    layer.update_layer()


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
