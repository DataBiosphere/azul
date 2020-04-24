from argparse import ArgumentParser
import logging
import sys

from azul.lambda_layer import LayerBuilder
from azul.logging import configure_script_logging

log = logging.getLogger(__name__)


def main(argv):
    parser = ArgumentParser(description='Package the requirements layer')
    parser.add_argument('-f', '--force', action='store_true', help='force build and re-upload of layer. Also taint '
                                                                   'Terraform resource to ensure it is re-provisioned')
    options = parser.parse_args(argv)
    packager = LayerBuilder()
    if options.force:
        packager.update_layer(force=True)
    else:
        packager.update_layer_if_necessary()


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
