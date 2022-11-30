"""
Save Terraform version and schema information to the file specified
in AZUL_TRACKED_SCHEMA_PATH or verify that the Terraform information
in that file is up-to-date.
"""
import argparse
import logging
import sys

from azul.logging import (
    configure_script_logging,
)
from azul.terraform import (
    terraform,
)

log = logging.getLogger(__name__)


def check_schema() -> bool:
    return terraform.schema.versions == terraform.versions


def update_schema() -> bool:
    terraform.update_schema()
    return True


if __name__ == '__main__':
    configure_script_logging()
    commands = dict(update=update_schema, check=check_schema)
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('command', choices=commands)
    arguments = parser.parse_args()
    sys.exit(0 if commands[arguments.command]() else 1)
