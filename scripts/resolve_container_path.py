import os
import sys

import docker

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
    try:
        with open('/proc/self/cgroup') as f:
            # Entries in /proc/self/cgroup look like this (note the nesting):
            # 11:name=systemd:/docker/82c1bd2…23b5bcf/docker/6547bce…60ca5a7
            prefix, container_id = next(f).strip().split(':')[2].split('/')[-2:]
    except FileNotFoundError:
        pass
    else:
        if prefix == 'docker':
            api = docker.client.from_env().api
            for mount in api.inspect_container(container_id)['Mounts']:
                if container_path.startswith(mount['Destination']):
                    tail = os.path.relpath(container_path, mount['Destination'])
                    return os.path.normpath(os.path.join(mount['Source'], tail))
    return None


if __name__ == '__main__':
    container_path = sys.argv[1]
    host_path = resolve_container_path(container_path)
    print(container_path if host_path is None else host_path)
