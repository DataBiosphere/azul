import argparse
import logging
import subprocess
import sys
from typing import (
    Optional,
)

from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.logging import (
    configure_script_logging,
)
from azul.terraform import (
    terraform,
)

log = logging.getLogger(__name__)

renamed: dict[str, Optional[str]] = {
    'aws_s3_bucket.storage': None,
}


def terraform_state_list() -> list[str]:
    try:
        output = terraform.run('state', 'list')
    except subprocess.CalledProcessError as e:
        if e.returncode == 1 and 'No state file was found' in e.stderr:
            log.info('No state file was found, assuming empty list of resources.')
            return []
        else:
            raise
    else:
        return output.splitlines()


def main(argv: list[str]):
    configure_script_logging(log)
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter,
                                     add_help=True)
    parser.add_argument('--dry-run',
                        action='store_true',
                        help='Report status without altering resources')
    args = parser.parse_args(argv)

    if renamed:
        current_names = terraform_state_list()
        for current_name in current_names:
            try:
                new_name = renamed[current_name]
            except KeyError:
                if current_name in renamed.values():
                    log.info('Found %r, already renamed', current_name)
            else:
                if new_name is None:
                    if args.dry_run:
                        log.info('Found %r, would be removing it from the Terraform state', current_name)
                    else:
                        log.info('Found %r, removing it from the Terraform state', current_name)
                        terraform.run('state', 'rm', current_name)
                else:
                    if args.dry_run:
                        log.info('Found %r, would be renaming it to %r', current_name, new_name)
                    else:
                        log.info('Found %r, renaming it to %r', current_name, new_name)
                        terraform.run('state', 'mv', current_name, new_name)
    else:
        log.info('No renamings defined')


if __name__ == '__main__':
    main(sys.argv[1:])
