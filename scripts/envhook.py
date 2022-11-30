"""
Install a hook into Python that modifies os.environ to emulate the effect of
sourcing `environment` in a shell so that Python interpreters started by PyCharm
(or other IDEs that don't their environment from a shell) have an environment
similar to that of interpreters started from that shell. The hook is also active
in Python interpreters launched by the shell but it doesn't modify os.environ in
that case. Instead, it prints a warning to stderr if the environment it
inherited from the shell is stale.

The hook also propagates AWS credentials cached by the AWS CLI to botocore and
boto3.
"""

from collections.abc import (
    Mapping,
)
from importlib.abc import (
    Loader,
)
import importlib.util
from itertools import (
    chain,
)
import os
import pathlib
import sys
from typing import (
    Optional,
    TypeVar,
)


class EnvHook:

    @classmethod
    def main(cls):
        self = cls()
        try:
            self._main(sys.argv[1:])
        except EnvhookError as e:
            self._print(e.args[0])
            sys.exit(1)

    @classmethod
    def sitecustomize(cls):
        self = cls()
        enabled = int(os.environ.get('ENVHOOK', '1'))
        if enabled == 0:
            self._print('Currently disabled because the ENVHOOK environment variable is set to 0.')
        else:
            self.setenv()
            self.share_aws_cli_credential_cache()

    def _main(self, argv):
        import argparse
        parser = argparse.ArgumentParser(description='Install a hook into Python that automatically sources `environment`')
        parser.add_argument('action', choices=['install', 'remove'])
        options = parser.parse_args(argv)

        # Confirm virtual environment is active `venv || virtualenv`
        if 'VIRTUAL_ENV' in os.environ:
            import site
            if hasattr(site, 'getsitepackages'):
                # Both plain Python and `venv` have `getsitepackages()`
                sys_prefix = Path(sys.prefix).resolve()
                link_dir = next(p for p in map(Path, site.getsitepackages())
                                if sys_prefix.is_prefix_of(p))
            else:
                # virtualenv's `site` does not have getsitepackages()
                link_dir = (Path(site.__file__).parent / 'site-packages').resolve()
        else:
            raise NoActiveVirtualenv

        dst = Path(__file__).absolute()

        # This is the least invasive way of looking up `sitecustomize`, AFAIK. The
        # alternative is `import sitecustomize` which would propagate exceptions
        # occurring in that module and trigger the side effects of loading that
        # module. This approach is really only safe when that module was already
        # loaded which is not the case if -S was passed or PYTHONNOUSERSITE is set.
        # We really only want to know if it's us or a different module. Another
        # alternative would be sys.modules.get('sitecustomize') but that would yield
        # None with -S or PYTHONNOUSERSITE even when there is a sitecustomize.py,
        # potentially one different from us.
        sitecustomize = importlib.util.find_spec('sitecustomize')
        if sitecustomize is not None:
            sitecustomize = Path(sitecustomize.origin)
            if sitecustomize.resolve() != dst.resolve():
                raise ThirdPartySiteCustomize(sitecustomize)

        link = link_dir / 'sitecustomize.py'
        if link.exists():
            if link.is_symlink():
                cur_dst = link.follow()
            else:
                raise NotASymbolicLinkError(link)
        else:
            cur_dst = None

        if options.action == 'install':
            if cur_dst is None:
                self._print(f'Installing by creating symbolic link from {link} to {dst}.')
                link.symlink_to(dst)
            elif dst == cur_dst:
                self._print(f'Already installed. Symbolic link from {link} to {dst} exists.')
            else:
                raise BadSymlinkDestination(link, cur_dst, dst)
        elif options.action == 'remove':
            if cur_dst is None:
                self._print(f'Not currently installed. Symbolic link {link} does not exist.')
            elif cur_dst == dst:
                self._print(f'Uninstalling by removing {link}.')
                link.unlink()
            else:
                raise BadSymlinkDestination(link, cur_dst, dst)
        else:
            assert False

    def setenv(self, new: Optional[Mapping[str, str]] = None):
        export_environment = self._import_export_environment()
        redact = getattr(export_environment, 'redact')
        if new is None:
            resolve_env = getattr(export_environment, 'resolve_env')
            load_env = getattr(export_environment, 'load_env')
            new, _ = load_env()
            new = resolve_env(new)
        old = os.environ
        for k, (o, n) in sorted(zip_dict(old, new).items()):
            if o is None:
                if self.pycharm_hosted:
                    self._print(f'Setting {k} to {redact(k, n)!r}')
                    os.environ[k] = n
                else:
                    self._print(f'Warning: {k} is not set but should be {redact(k, n)!r}, '
                                f'you should run `source environment`')
            elif n is None:
                pass
            elif n != o:
                if k.startswith('PYTHON'):
                    self._print(f'Ignoring change in {k} from {redact(k, o)!r} to {redact(k, n)!r}')
                else:
                    if self.pycharm_hosted:
                        self._print(f'Changing {k} from {redact(k, o)!r} to {redact(k, n)!r}')
                        os.environ[k] = n
                    else:
                        self._print(f'Warning: {k} is {redact(k, o)!r} but should be {redact(k, n)!r}, '
                                    f'you must run `source environment`')

    @property
    def pycharm_hosted(self):
        return bool(int(os.environ.get('PYCHARM_HOSTED', '0')))

    def _import_export_environment(self):
        # When this module is loaded from the `sitecustomize.py` symbolic link, the
        # directory containing the physical file may not be on the sys.path so we
        # cannot use a normal import to load the `export_environment` module.
        module_name = 'export_environment'
        file_name = module_name + '.py'
        parent_dir = Path(__file__).follow().parent
        spec = importlib.util.spec_from_file_location(name=module_name,
                                                      location=parent_dir / file_name)
        export_environment = importlib.util.module_from_spec(spec)
        assert isinstance(spec.loader, Loader)
        spec.loader.exec_module(export_environment)
        return export_environment

    @classmethod
    def _print(cls, msg):
        print(Path(__file__).resolve().name + ':', msg, file=sys.stderr)

    def _parse(self, env: str) -> dict[str, str]:
        return {k: v for k, _, v in (line.partition('=') for line in env.splitlines())}

    def share_aws_cli_credential_cache(self):
        """
        By default, boto3 and botocore do not use a cache for the assume-role
        provider even though the credentials cache mechanism exists in botocore.
        This means that if assuming a role requires you to enter a MFA code, you
        will have to enter it every time you instantiate a boto3 or botocore client,
        even if your previous session would have lasted longer.

        This function connects the assume-role provider with the cache used by the
        AWS CLI, saving tedious code reentry. It does so only for boto3.
        """
        try:
            import boto3
            import botocore.credentials
            import botocore.session
        except ImportError:
            self._print('Looks like boto3 is not installed. Skipping credential sharing with AWS CLI.')
        else:
            # Get the AssumeRole credential provider
            session = botocore.session.get_session()
            resolver = session.get_component('credential_provider')
            assume_role_provider = resolver.get_provider('assume-role')

            # Make the provider use the same cache as the AWS CLI
            cli_cache = Path('~', '.aws', 'cli', 'cache').expanduser()
            assume_role_provider.cache = botocore.credentials.JSONFileCache(cli_cache)

            # Calls to boto3.client() and .resource() use the default session and
            # therefore hit the cached credentials
            boto3.setup_default_session(botocore_session=session)

            if self.pycharm_hosted:
                # The equivalent of the _preauth function in `environment`
                credentials = session.get_credentials()
                self.setenv(dict(AWS_ACCESS_KEY_ID=credentials.access_key,
                                 AWS_SECRET_ACCESS_KEY=credentials.secret_key,
                                 AWS_SESSION_TOKEN=credentials.token))


K = TypeVar('K')
OV = TypeVar('OV')
NV = TypeVar('NV')


def zip_dict(old: Mapping[K, OV], new: Mapping[K, NV], missing=None) -> dict[K, tuple[OV, NV]]:
    """
    Merge two dictionaries. The resulting dictionary contains an entry for every
    key in either `old` or `new`. Each entry in the result associates a key to
    two values: the value from `old` for that key followed by the value from
    `new` for that key. If the key is absent from either argument, the
    respective tuple element will be `missing`, which defaults to None. If
    either `old` or `new` could contain None values, some other value should be
    passed for `missing` in order to distinguish None values from values for
    absent entries.

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


class Path(pathlib.PosixPath):

    def follow(self) -> 'Path':
        """
        This methods performs one level of symbolic link resolution. For paths
        representing a symbolic link with an absolute target, this methods is
        equivalent to readlink(). For symbolic links with relative targets, this
        method returns the result of appending the target to the parent of this
        path. The returned path is always absolute.

        Unless you need the target of the symbolic link verbatim, you should
        prefer this method over readlink().
        """
        target = self.readlink()
        if target.is_absolute():
            return target
        else:
            return (self.parent / target).absolute()

    def is_relative(self):
        return not self.is_absolute()

    def is_prefix_of(self, other: 'Path'):
        """
        >>> Path('/').is_prefix_of(Path('/'))
        True

        >>> Path('/').is_prefix_of(Path('/a'))
        True

        >>> Path('/a').is_prefix_of(Path('/'))
        False

        >>> Path('/a').is_prefix_of(Path('/a/b'))
        True

        >>> Path('/a/b').is_prefix_of(Path('/a'))
        False
        """
        if self.is_relative():
            raise ValueError('Need absolute path', self)
        elif other.is_relative():
            raise ValueError('Need absolute path', other)
        else:
            return other.parts[:len(self.parts)] == self.parts


class EnvhookError(RuntimeError):
    pass


class NoActiveVirtualenv(EnvhookError):

    def __init__(self) -> None:
        super().__init__('Need to be run from within a virtualenv')


class NotASymbolicLinkError(EnvhookError):

    def __init__(self, link: Path) -> None:
        super().__init__(
            f'{link} is not a symbolic link. Make a backup of that file, '
            f'remove the original and try again. Note that removing the file '
            f'may break other, third-party site customizations.'
        )


class BadSymlinkDestination(EnvhookError):

    def __init__(self, link: Path, actual: Path, expected: Path) -> None:
        super().__init__(
            f'Symbolic link {link} points to {actual} instead of {expected}. '
            f'Try removing the symbolic link and try again.'
        )


class ThirdPartySiteCustomize(EnvhookError):

    def __init__(self, sitecustomize: Path) -> None:
        super().__init__(
            f'A different `sitecustomize` module already exists at '
            f'{sitecustomize}. Make a backup of that file, remove the original '
            f'and try again. Note that removing the file may break other, '
            f'third-party site customizations.'
        )


if __name__ == '__main__':
    EnvHook.main()
elif __name__ == 'sitecustomize':
    EnvHook.sitecustomize()
