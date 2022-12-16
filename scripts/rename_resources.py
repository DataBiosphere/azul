import argparse
import logging
import subprocess
import sys

from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)

renamed = {
}


def terraform_state(command: str, *args: str) -> bytes:
    proc = subprocess.run(['terraform', 'state', command, *args],
                          check=False,
                          capture_output=True,
                          shell=False)
    sys.stderr.buffer.write(proc.stderr)
    if proc.returncode == 0:
        return proc.stdout
    elif (
        proc.returncode == 1
        and command == 'list'
        and b'No state file was found!' in proc.stderr
    ):
        log.info('No state file was found, assuming empty list of resources.')
        return b''
    else:
        proc.check_returncode()


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
        current_names = terraform_state('list').decode().splitlines()
        for current_name in current_names:
            try:
                new_name = renamed[current_name]
            except KeyError:
                if current_name in renamed.values():
                    log.info('Found %r, already renamed', current_name)
            else:
                if args.dry_run:
                    log.info('Found %r, would be renaming it to %r', current_name, new_name)
                else:
                    log.info('Found %r, renaming it to %r', current_name, new_name)
                    terraform_state('mv', current_name, new_name)
    else:
        log.info('No renamings defined')


if __name__ == '__main__':
    main(sys.argv[1:])
