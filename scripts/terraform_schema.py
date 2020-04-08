"""
Save Terraform version and schema information to the file specified
in AZUL_TRACKED_SCHEMA_PATH or verify that the Terraform information
in that file is up-to-date.
"""
import argparse
import logging

from azul import (
    config,
)
from azul.deployment import (
    terraform,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def check_schema_json() -> None:
    if terraform.tracked_versions == terraform.versions():
        return
    else:
        raise RuntimeError('Tracked Terraform schema is out of date. Run `make -C '
                           f'terraform schema` and commit {config.tracked_terraform_schema}')


if __name__ == '__main__':
    configure_script_logging()
    commands = {
        'generate': terraform.write_tracked_schema,
        'check': check_schema_json
    }
    # https://youtrack.jetbrains.com/issue/PY-41806
    # noinspection PyTypeChecker
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('command', choices=commands)
    arguments = parser.parse_args()
    commands[arguments.command]()
