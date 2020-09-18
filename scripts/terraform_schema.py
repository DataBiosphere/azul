"""
Save Terraform version and schema information to the file specified
in AZUL_TRACKED_SCHEMA_PATH or verify that the Terraform information
in that file is up-to-date.
"""
import argparse
import logging
import sys

from azul.deployment import (
    terraform,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def check_schema() -> None:
    schema = terraform.schema
    if schema.versions != terraform.versions:
        raise RuntimeError(f"Cached Terraform schema is out of date. "
                           f"Run '{sys.executable} {__file__} update' "
                           f"and commit {schema.path}")


if __name__ == '__main__':
    configure_script_logging()
    commands = {
        'update': terraform.update_schema,
        'check': check_schema
    }
    # https://youtrack.jetbrains.com/issue/PY-41806
    # noinspection PyTypeChecker
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('command', choices=commands)
    arguments = parser.parse_args()
    commands[arguments.command]()
