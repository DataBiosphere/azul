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
    parser.add_argument('-f', '--force', action='store_true',
                        help='Force building and uploading of the layer package '
                             'even if the dependencies have not changed. Taint '
                             'the corresponding Terraform resource to ensure '
                             'that Terraform re-provisions the Lambda layer.')
    options = parser.parse_args(argv)
    layer = DependenciesLayer()
    layer.update_layer(force=options.force)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
