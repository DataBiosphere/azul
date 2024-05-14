"""
Delete the published versions of every AWS Lambda function in the current
deployment, leaving only the unpublished version ($LATEST) of each.
"""

import logging

from azul import (
    config,
    require,
)
from azul.lambdas import (
    Lambdas,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def main():
    require(config.terraform_component == '',
            'This script cannot be run with a Terraform component selected',
            config.terraform_component)
    Lambdas().delete_published_function_versions()


if __name__ == '__main__':
    configure_script_logging(log)
    main()
