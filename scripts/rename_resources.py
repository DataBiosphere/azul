import json
import logging
import os
import subprocess
import sys

from azul import (
    config,
)
from azul.logging import (
    configure_script_logging,
)
from azul.terraform import (
    chalice,
)

log = logging.getLogger(__name__)

renamed = {
}


def chalice_renamed():
    for lambda_name in ('indexer', 'service'):
        tf_config_path = chalice.package_dir(lambda_name) / chalice.tf_config_name
        with open(tf_config_path) as f:
            tf_config_path = json.load(f)
            mapping = chalice.resource_name_mapping(tf_config_path)
            for (resource_type, name), new_name in mapping.items():
                prefix = f'module.chalice_{lambda_name}.{resource_type}.'
                yield prefix + name, prefix + new_name


renamed.update(dict(chalice_renamed()))


def terraform_state(command: str, *args: str) -> bytes:
    proc = subprocess.run(['terraform', 'state', command, *args],
                          cwd=os.path.join(config.project_root, 'terraform'),
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


def main():
    current_names = terraform_state('list').decode().splitlines()
    for current_name in current_names:
        try:
            new_name = renamed[current_name]
        except KeyError:
            if current_name in renamed.values():
                log.info('Found %r, already renamed', current_name)
        else:
            log.info('Found %r, renaming to %r', current_name, new_name)
            terraform_state('mv', current_name, new_name)


if __name__ == '__main__':
    configure_script_logging(log)
    main()
