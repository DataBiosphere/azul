#! /usr/bin/env python3

import errno
from importlib.abc import (
    MetaPathFinder,
)
from itertools import chain
import os
from pathlib import Path
import subprocess
import sys
from typing import (
    Mapping,
    MutableMapping,
    Tuple,
    TypeVar,
)

__all__ = ('setenv', 'main')


def main(argv):
    import argparse
    parser = argparse.ArgumentParser(description='Install a hook into Python that automatically sources `environment`')
    parser.add_argument('action', choices=['install', 'remove'])
    options = parser.parse_args(argv)
    if 'VIRTUAL_ENV' in os.environ:  # Confirm virtual environment is active `venv || virtualenv`
        import site
        if hasattr(site, 'getsitepackages'):
            # Both plain Python and `venv` have `getsitepackages()`
            sys_prefix = os.path.realpath(sys.prefix)
            link_dir = next(p for p in site.getsitepackages() if os.path.realpath(p).startswith(sys_prefix))
        else:
            # virtualenv's `site` does not have getsitepackages()
            link_dir = os.path.realpath(os.path.join(os.path.dirname(site.__file__), 'site-packages'))
    else:
        raise RuntimeError('Need to be run from within a virtualenv')
    dst = os.path.realpath(__file__)
    try:
        import sitecustomize
    except ImportError:
        pass
    else:
        if os.path.realpath(sitecustomize.__file__) != dst:
            raise RuntimeError(f'A different sitecustomize module already exists at {sitecustomize.__file__}')
    link = os.path.join(link_dir, 'sitecustomize.py')
    rel_dst = os.path.relpath(dst, link_dir)
    try:
        cur_dst = os.readlink(link)
    except FileNotFoundError:
        cur_dst = None
    except OSError as e:
        if e.errno == errno.EINVAL:
            raise RuntimeError(f"{link} is not a symbolic link. It may be a 3rd party file and we won't touch it")
        else:
            raise
    if options.action == 'install':
        if cur_dst is None:
            os.symlink(rel_dst, link)
        elif rel_dst == cur_dst:
            pass
        else:
            raise RuntimeError(f'{link} points somewhere unexpected ({cur_dst})')
    elif options.action == 'remove':
        if cur_dst is None:
            pass
        elif cur_dst == rel_dst:
            os.unlink(link)
        else:
            raise RuntimeError(f'{link} points somewhere unexpected ({cur_dst})')
    else:
        assert False


def setenv():
    self = Path(__file__).resolve()
    project = self.parent.parent
    environment = project.joinpath('environment')
    old = _parse(_run('env'))
    new = _parse(_run(f'source {environment} && env'))
    pycharm_hosted = bool(int(os.environ.get('PYCHARM_HOSTED', '0')))

    def sanitize(k, v):
        return 'REDACTED' if any(s in k.lower() for s in ('secret', 'password', 'token')) else v

    for k, (o, n) in sorted(zip_dict(old, new).items()):
        if o is None:
            if pycharm_hosted:
                _print(f"{self.name}: Setting {k} to '{sanitize(k, n)}'")
                os.environ[k] = n
            else:
                _print(f"{self.name}: Warning: {k} is not set but should be {sanitize(k, n)}, "
                       f"you must run `source environment`")
        elif n is None:
            if pycharm_hosted:
                _print(f"{self.name}: Removing {k}, was set to '{sanitize(k, o)}'")
                del os.environ[k]
            else:
                _print(f"{self.name}: Warning: {k} is '{sanitize(k, o)}' but should not be set, "
                       f"you must run `source environment`")
        elif n != o:
            if k.startswith('PYTHON'):
                _print(f"{self.name}: Ignoring change in {k} from '{sanitize(k, o)}' to '{sanitize(k, n)}'")
            else:
                if pycharm_hosted:
                    _print(f"{self.name}: Changing {k} from '{sanitize(k, o)}' to '{sanitize(k, n)}'")
                    os.environ[k] = n
                else:
                    _print(f"{self.name}: Warning: {k} is '{sanitize(k, o)}' but should be '{sanitize(k, n)}', "
                           f"you must run `source environment`")


K = TypeVar('K')
OV = TypeVar('OV')
NV = TypeVar('NV')


def zip_dict(old: Mapping[K, OV], new: Mapping[K, NV], missing=None) -> MutableMapping[K, Tuple[OV, NV]]:
    """
    Merge two dictionaries. The resulting dictionary contains an entry for every key in either `old` or `new`. Each
    entry in the result associates a key to two values: the value from `old` for that key followed by the value from
    `new` for that key. If the key is absent from either argument, the respective tuple element will be `missing`,
    which defaults to None. If either `old` or `new` could contain None values, some other value should be passed for
    `missing` in order to distinguish None values from values for absent entries.

    >>> zip_dict({1:2}, {1:2})
    {1: (2, 2)}
    >>> zip_dict({1:2}, {3:4})
    {1: (2, None), 3: (None, 4)}
    >>> zip_dict({1:2}, {1:3})
    {1: (2, 3)}
    >>> zip_dict({1:2}, {})
    {1: (2, None)}
    >>> zip_dict({}, {1:2})
    {1: (None, 2)}
    >>> zip_dict({'deleted': 1, 'same': 2, 'changed': 3}, {'same': 2, 'changed': 4, 'added': 5}, missing=-1)
    {'deleted': (1, -1), 'same': (2, 2), 'changed': (3, 4), 'added': (-1, 5)}
    """
    result = ((k, (old.get(k, missing), n)) for k, n in new.items())
    removed = ((k, o) for k, o in old.items() if k not in new)
    removed = ((k, (o, missing)) for k, o in removed)
    return dict(chain(removed, result))


def _print(msg):
    print(msg, file=sys.stderr)


def _run(command) -> str:
    bash = "/bin/bash"
    try:
        shell = os.environ['SHELL']
    except KeyError:
        shell = bash
    else:
        # allow a custom path to bash, but reject all other shells
        if os.path.basename(shell) != 'bash':
            shell = bash
    args = [shell, '-c', command]
    process = subprocess.run(args, stdout=subprocess.PIPE)
    output = process.stdout.decode()
    if process.returncode != 0:
        raise RuntimeError(f'Running {args} failed with {process.returncode}:\n{output}')
    return output


def _parse(env: str) -> MutableMapping[str, str]:
    return {k: v for k, _, v in (line.partition('=') for line in env.splitlines())}


class SanitizingFinder(MetaPathFinder):

    def __init__(self) -> None:
        super().__init__()
        self.bad_path = str(Path(__file__).resolve().parent.parent / 'src' / 'azul')
        self.name = Path(__file__).resolve().name

    def find_spec(self, *args, **kwargs):
        sys_path = sys.path
        while True:
            try:
                index = sys_path.index(self.bad_path)
            except ValueError:
                return None
            else:
                _print(f"{self.name}: Sanitizing sys.path by removing entry {index} containing '{self.bad_path}'.")
                del sys_path[index]


def sanitize_sys_path():
    """
    Certain PyCharm support scripts like docrunner.py add the directory
    containing a module to `sys.path`, presumably with the intent to emulate
    Python behavior for scripts run from the command line:

    https://docs.python.org/3.6/using/cmdline.html#cmdoption-c

    This has negative consequences when the module resides in the `src/azul`
    directory of this project because that directory also contains modules
    whose name conflicts with that of important built-in or third-party
    packages, `json.py` for example. This project relies on the fully-qualified
    package path of those modules to disambiguate them from the built-in ones
    but placing their containing parent directory on `sys.path` defeats that.

    This method attempts to counteract that by removing the directory again.
    """
    # Can't remove the entry immediately because it might not yet be present.
    # Instead, install a hook into the import machinery so it will be removed
    # soon after is added.
    sys.meta_path.insert(0, SanitizingFinder())


def use_cached_boto_session():
    """
    By default, boto does not use a cache for the assume-role provider. This
    means that if assuming a role requires you to enter a MFA key, you will have
    to enter this key every time you instantiate a boto session, even if your
    assume-role session will last for longer.

    This script connects the assume-role provider with the cache used by the
    CLI, saving tedious key reentry.
    """
    try:
        import boto3
        import botocore.credentials
        import botocore.session
    except ImportError:
        _print('Failed to import boto, so cached session will not be used.')
    else:
        # Get the AssumeRole credential provider and make it the only one
        session = botocore.session.get_session()
        resolver = session.get_component('credential_provider')
        assume_role_provider = resolver.get_provider('assume-role')

        # Make the provider use the same cache as the AWS CLI
        cli_cache = os.path.join(os.path.expanduser('~'), '.aws/cli/cache')
        assume_role_provider.cache = botocore.credentials.JSONFileCache(cli_cache)

        # Every boto call will use this default session and therefore hit the
        # cached credentials
        boto3.setup_default_session(botocore_session=session)


if __name__ == '__main__':
    main(sys.argv[1:])
elif __name__ == 'sitecustomize':
    sanitize_sys_path()
    setenv()
    use_cached_boto_session()
