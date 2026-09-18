import logging
import os
import re
import sys

import docker
from more_itertools import (
    only,
)

from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)

"""
Convert the given container path to a path that's valid on the host.

If the current process is running inside a Docker container and if that
container has access to the Docker daemon that instantiated that container,
resolve the argument against any host files or directories that are mounted in
the container and print the host path that maps to the argument, otherwise print
the argument.

If this is run on a host, print the argument. If this is run in a container, but
the argument doesn't map to a host path, print the argument.
"""


def resolve_container_path(container_path):
    container_path = os.path.realpath(container_path)
    mountinfo_path = '/proc/self/mountinfo'
    try:
        with open(mountinfo_path) as f:
            log.info('Trying to extract ID of current container from %s', mountinfo_path)
            # Entries of interest in /proc/self/mountinfo look as follows:
            # 752 744 259:2 /docker/containers/dc61d9…/…
            # dc61d9… is the container ID
            container_ids = set()
            for line in f:
                assert line.endswith('\n'), line
                parts = line[:-1].split(' ')
                assert len(parts) > 9, line
                root = parts[3]
                for prefix in ('/var/lib/docker/containers/', '/docker/containers/'):
                    if root.startswith(prefix):
                        log.info('Extracting container ID from %s', root)
                        id = root[len(prefix):].split('/')[0]
                        assert re.fullmatch(r'[0-9a-f]{64}', id), id
                        container_ids.add(id)
    except FileNotFoundError:
        log.info('Did not find %s', mountinfo_path)
    else:
        container_id = only(container_ids)
        if container_id is None:
            log.info('Container ID not found in %s', mountinfo_path)
        else:
            api = docker.client.from_env().api
            for mount in api.inspect_container(container_id)['Mounts']:
                if container_path.startswith(mount['Destination']):
                    tail = os.path.relpath(container_path, mount['Destination'])
                    host_path = os.path.normpath(os.path.join(mount['Source'], tail))
                    log.info('Resolved %s to %s', container_path, host_path)
                    return host_path
    log.info('Assuming %s is a host path', container_path)
    return None


def main(container_path):
    host_path = resolve_container_path(container_path)
    print(container_path if host_path is None else host_path)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1])
