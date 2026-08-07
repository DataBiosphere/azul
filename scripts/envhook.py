"""
Install a hook into Python that modifies os.environ to emulate the effect of
sourcing `environment` in a shell so that Python interpreters started by PyCharm
(or other IDEs that don't their environment from a shell) have an environment
similar to that of interpreters started from that shell. The hook is also active
in Python interpreters launched by the shell but it doesn't modify os.environ in
that case. Instead, it prints a warning to stderr if the environment it
inherited from the shell is stale.
"""

from collections.abc import (
    Mapping,
)
from functools import (
    cache,
)
from importlib.abc import (
    Loader,
)
import importlib.util
import os
import pathlib
import sys


class EnvHook:

    def main(self):
        try:
            self._main(sys.argv[1:])
        finally:
            sys.stderr.flush()

    def sitecustomize(self):
        try:
            enabled = int(os.environ.get('ENVHOOK', '1'))
            if enabled == 0:
                self.print('Currently disabled because the ENVHOOK environment variable is set to 0.')
            else:
                self.handle_env()
        except EnvhookError as e:
            if self.pycharm_hosted:
                # Under PyCharm, something suppresses sys.exit, probably
                # wrongly catching BaseException instead of Exception, so we
                # need to force the exit.
                self.print(e.args)
                os._exit(1)
            else:
                raise
        finally:
            sys.stderr.flush()

    def _main(self, argv):
        import argparse

        from azul.args import (
            AzulArgumentHelpFormatter,
        )
        parser = argparse.ArgumentParser(description=__doc__,
                                         formatter_class=AzulArgumentHelpFormatter)
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
                self.print(f'Installing by creating symbolic link from {link} to {dst}.')
                link.symlink_to(dst)
            elif dst == cur_dst:
                self.print(f'Already installed. Symbolic link from {link} to {dst} exists.')
            else:
                raise BadSymlinkDestination(link, cur_dst, dst)
        elif options.action == 'remove':
            if cur_dst is None:
                self.print(f'Not currently installed. Symbolic link {link} does not exist.')
            elif cur_dst == dst:
                self.print(f'Uninstalling by removing {link}.')
                link.unlink()
            else:
                raise BadSymlinkDestination(link, cur_dst, dst)
        else:
            assert False

    def handle_env(self):
        env = self.prepare_env()
        azul_env_hash = self.export_environment.azul_env_hash
        expected, actual = env[azul_env_hash], os.environ.get(azul_env_hash)
        if self.pycharm_hosted:
            if actual is None:
                self.set_env(env)
            else:
                raise TaintedEnv()
        else:
            if actual is None:
                raise EmptyEnv()
            elif actual != expected:
                raise StaleEnv()

    def prepare_env(self) -> Mapping[str, str]:
        prepare_env = self.export_environment.prepare_env
        new, message = prepare_env()
        return new

    def set_env(self, env: Mapping[str, str]):
        redact = self.export_environment.redact
        for k, v in env.items():
            try:
                v_ = os.environ[k]
            except KeyError:
                self.print(f'Setting {k} to {redact(k, v)!r}')
                os.environ[k] = v
            else:
                self.print(f'Not setting {k} to {redact(k, v)!r} '
                           f'because it is already set to {redact(k, v_)!r}')

    @property
    def pycharm_hosted(self):
        return (
            # Indicates Python Console, Run/Debug
            bool(int(os.environ.get('PYCHARM_HOSTED', '0')))
            # Indicates interpreter is being verified before adding it to PyCharm
            or sys.orig_argv[1:3] == ['-c', 'print(1)']
            or sys.orig_argv[1] == '-c' and '_is_gil_enabled' in sys.orig_argv[2]
            # Indicates sys.path and installed packages are being listed
            or 'plugins/python-ce/helpers' in sys.argv[0]
        )

    @classmethod
    @cache
    def import_sibling_script(cls, module_name: str):
        # When this module is loaded from the `sitecustomize.py` symbolic link, the
        # directory containing the physical file may not be on the sys.path so we
        # cannot use a normal import to load any sibling scripts.
        file_name = module_name + '.py'
        parent_dir = Path(__file__).follow().parent
        path = parent_dir / file_name
        spec = importlib.util.spec_from_file_location(name=module_name, location=path)
        module = importlib.util.module_from_spec(spec)
        assert isinstance(spec.loader, Loader)
        spec.loader.exec_module(module)
        return module

    @property
    def export_environment(self):
        return self.import_sibling_script('export_environment')

    prefix = pathlib.Path(__file__).resolve().name + ':'

    @classmethod
    def print(cls, msg):
        k = 'ENVHOOK_SILENT'
        if int(os.environ.get('%s' % k, '0')):
            cls._print(k + ' is set, expect no further diagnostic output.')
            cls.print = lambda *_, **__: None
        else:
            cls._print(msg)
            cls.print = cls._print

    @classmethod
    def _print(cls, msg):
        print(cls.prefix, msg, file=sys.stderr)


class Path(pathlib.PosixPath):

    def follow(self) -> Path:
        """
        This method performs one level of symbolic link resolution. For paths
        representing a symbolic link with an absolute target, this method is
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

    def is_prefix_of(self, other: Path):
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


class EnvhookError(SystemExit):
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

    def __init__(self, other: Path) -> None:
        super().__init__(
            f'A different `sitecustomize` module already exists at {other}. '
            f'Make a backup of that file, remove the original and try again. '
            f'Note that removing the file may break other, third-party site '
            f'customizations.'
        )


class TaintedEnv(EnvhookError):

    def __init__(self):
        super().__init__(
            'The current process is a child of the PyCharm process but '
            'unexpectedly already has the Azul environment loaded'
        )


class StaleEnv(EnvhookError):

    def __init__(self):
        super().__init__(
            'The environment is stale. You need to run `source environment`.'
        )


class EmptyEnv(EnvhookError):

    def __init__(self):
        super().__init__(
            'The environment is empty. You need to run `source environment`.'
        )


if __name__ == '__main__':
    EnvHook().main()
elif __name__ == 'sitecustomize':
    EnvHook().sitecustomize()
