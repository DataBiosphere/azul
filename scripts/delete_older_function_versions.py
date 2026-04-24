"""
Delete Lambda function versions older than a specified version
"""
import argparse
import logging
import sys

from azul import (
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.lambdas import (
    LambdaFunctions,
)
from azul.lib import (
    R,
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
                        help='A numeric version of the Lambda function. The '
                             'specified version will be kept, as will '
                             'the preceding one. All versions preceding those '
                             'two versions will be deleted.')
    args = parser.parse_args(argv)
    functions = LambdaFunctions()
    functions.delete_older_versions(args.function_name,
                                    args.function_version,
                                    # Keep a previous version to guard against a
                                    # race condition due to eventual consistency
                                    # of alias updates.
                                    num_older_versions_to_keep=1)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
