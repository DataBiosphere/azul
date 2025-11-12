"""
Delete all versions of a Lambda function prior to the specified one.
"""
import argparse
import logging
import sys

from azul import (
    R,
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.lambdas import (
    LambdaFunctions,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def main(argv: list[str]):
    assert config.terraform_component == '', R(
        'This script cannot be run with a Terraform component selected',
        config.terraform_component)
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)
    parser.add_argument('--function-name', '-f',
                        required=True,
                        help='The name of the Lambda function.')
    parser.add_argument('--function-version', '-v',
                        type=int,
                        required=True,
                        help='The Lambda function version to keep. Must be an '
                             'integer.')
    args = parser.parse_args(argv)
    log.info('Deleting function %r versions older than %r',
             args.function_name, args.function_version)
    functions = LambdaFunctions()
    functions.delete_older_versions(args.function_name, args.function_version)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
