from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    ChainMap,
)
from importlib.abc import (
    Loader,
)
import importlib.util
from io import (
    StringIO,
)
import os
from pathlib import (
    Path,
)
import shlex
import sys
from typing import (
    Iterator as Iterator,
    Mapping,
    Optional,
    TextIO,
    cast,
)

this_module = Path(__file__)

DraftEnvironment = Mapping[str, Optional[str]]

Environment = Mapping[str, str]


class EnvironmentModule(metaclass=ABCMeta):
    """
    Defines the methods that an environment.py or environment.local.py module
    are expected to expose. In lieu of protocols (PEP-0544) we'll use an
    abstract class instead.

    https://www.python.org/dev/peps/pep-0544/#modules-as-implementations-of-protocols
    """

    @abstractmethod
    def env(self) -> DraftEnvironment:
        """
        Returns a dictionary that maps environment variable names to values.
        The values are either None or strings. String values can contain
        references to other environment variables in the form `{FOO}` where
        FOO is the name of an environment variable. See

        https://docs.python.org/3.8/library/string.html#format-string-syntax

        for the concrete syntax. Any references will be resolved after the
        environment has been compiled by merging all environment.py files.

        Entries with a None value will be excluded from the environment. They
        should be used to document variables without providing a default
        value. Other, usually more specific environment.py files should
        provide the value.
        """
        return {
            'FOO': 'bar',
            'BAR': None
        }


class InvalidActiveDeployment(RuntimeError):

    def __init__(self, dir_: Path) -> None:
        super().__init__(
            f"{dir_} does not exist or is not a symbolic link to a directory. "
            f"Please create a symbolic link to the active deployment, as in "
            f"the following example: 'cd deployments && ln -snf dev .active'"
        )


class BadParentDeployment(RuntimeError):

    def __init__(self, parent: Path, component: Path) -> None:
        super().__init__(
            f"Component {component} refers to non-existent parent deployment {parent}"
        )


def load_env() -> Environment:
    """
    Load environment.py and environment.local.py modules from the project
    root and the current active deployment directory, call their env()
    function to obtain the environment dictionary and merge the dictionaries.
    The entries from an environment.local.py take precedence over those from
    a corresponding environment.py in the same directory. The modules from
    the deployment directory take precedence over ones in the project root.
    """
    root_dir = this_module.parent.parent

    deployments_dir = root_dir / 'deployments'
    active_deployment_dir = deployments_dir / '.active'
    if not active_deployment_dir.is_dir() or not active_deployment_dir.is_symlink():
        raise InvalidActiveDeployment(active_deployment_dir)

    # If active deployment is a component of another one, also load the parent
    # deployments (like dev.gitlab).
    active_deployment_dir = Path(os.readlink(str(active_deployment_dir)))
    if not active_deployment_dir.is_absolute():
        active_deployment_dir = deployments_dir / active_deployment_dir
    if not active_deployment_dir.is_dir():
        raise InvalidActiveDeployment(active_deployment_dir)
    relative_active_deployment_dir = active_deployment_dir.relative_to(deployments_dir)
    prefix, _, suffix = str(relative_active_deployment_dir).partition('.')
    if suffix and suffix != 'local':
        parent_deployment_dir = deployments_dir / prefix
        if not parent_deployment_dir.exists():
            raise BadParentDeployment(parent_deployment_dir, active_deployment_dir)
    else:
        parent_deployment_dir = None

    def _load(dir_path: Path, local: bool = False) -> Optional[EnvironmentModule]:
        """
        Load and return the `environment.py` or `environment.local.py` module
        from the given directory if such a module exists, otherwise return None.
        """
        suffix = '.local.py' if local else '.py'
        file_path = dir_path / ('environment' + suffix)
        if file_path.exists():
            module_file = Path(file_path).relative_to(root_dir)
            if __name__ == '__main__':
                print(f'{this_module.name}: Loading environment from {module_file}', file=sys.stderr)
            spec = importlib.util.spec_from_file_location('environment', file_path)
            module = importlib.util.module_from_spec(spec)
            assert isinstance(spec.loader, Loader)
            spec.loader.exec_module(module)
            env = getattr(module, 'env')
            assert callable(env)
            return cast(EnvironmentModule, module)
        else:
            return None

    modules = [
        _load(active_deployment_dir, local=True),
        _load(active_deployment_dir),
        _load(parent_deployment_dir, local=True) if parent_deployment_dir else None,
        _load(parent_deployment_dir) if parent_deployment_dir else None,
        _load(root_dir, local=True),
        _load(root_dir)
    ]
    # Note that ChainMap looks only considers the second mapping in the chain
    # if a key is absent from the first one. IOW, the earlier mappings in the
    # chain take precedence over later ones.
    env = ChainMap(dict(project_root=str(root_dir)))
    for module in modules:
        if module is not None:
            env.maps.append(filter_env(module.env()))
    return env


def filter_env(env: DraftEnvironment) -> Environment:
    """
    Remove entries whose value is None from the environment. None values are
    permitted in environment.py modules such that those entries can be
    documented with having to define a value.
    """
    return {k: v for k, v in env.items() if v is not None}


class ResolvedEnvironment(Environment):

    def __init__(self, env: Environment) -> None:
        super().__init__()
        self.env = env

    def __getitem__(self, k: str) -> str:
        try:
            v = self.env[k]
        except KeyError:
            return ''
        else:
            return v.format_map(self)

    def __len__(self) -> int:
        return len(self.env)

    def __iter__(self) -> Iterator[str]:
        return iter(self.env)

    def __repr__(self) -> str:
        return repr(dict(self))

    __str__ = __repr__


def resolve_env(env: Environment) -> Environment:
    """
    Resolve references to other variables among all values in the given
    environment.

    Literal variable value:

    >>> resolve_env({'x': '42'})
    {'x': '42'}

    Value referencing another variable:

    >>> resolve_env({'x': '{y}', 'y': '42'})
    {'x': '42', 'y': '42'}

    A reference to a missing value yields an empty string, similar to Unix
    shell variable interpolation.

    >>> resolve_env({'x': '{y}'})
    {'x': ''}

    Transitive reference:

    >>> resolve_env({'x': '{y}', 'y': '{z}', 'z': '42'})
    {'x': '42', 'y': '42', 'z': '42'}

    Circular references, direct or indirect are not supported:


    (IGNORE_EXCEPTION_DETAIL is needed because the recursion errors messages
    under certain, yet to be determined conditions, include the suffix " while
    calling Python object")

    >>> resolve_env({'x': '{x}'})  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    RecursionError: maximum recursion depth exceeded
    >>> resolve_env({'x': '{y}', 'y': '{x}'}) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    RecursionError: maximum recursion depth exceeded
    >>> resolve_env({'x': '{y}', 'y': '{z}', 'z': '{x}'})  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    RecursionError: maximum recursion depth exceeded

    Literal (escaped) curly braces:

    >>> resolve_env({'o': '{{', 'c': '}}', 'oc': '{{}}'})
    {'o': '{', 'c': '}', 'oc': '{}'}

    Generated references are not resolved:

    >>> resolve_env({'x': '{o}y{c}', 'y': '42', 'o': '{{', 'c': '}}'})
    {'x': '{y}', 'y': '42', 'o': '{', 'c': '}'}

    If they were, the result would be:

    {'x': '42', 'y': '42', 'o': '{', 'c': '}'}

    Dangling braces are not supported:

    >>> resolve_env({'x': '{'})
    Traceback (most recent call last):
    ...
    ValueError: Single '{' encountered in format string
    >>> resolve_env({'x': '}'})
    Traceback (most recent call last):
    ...
    ValueError: Single '}' encountered in format string

    And an empty pair of braces isn't either:

    >>> resolve_env({'x': '{}'})
    Traceback (most recent call last):
    ...
    ValueError: Format string contains positional fields
    """
    return dict(ResolvedEnvironment(env))


def export_env(env: Environment, output: Optional[TextIO]) -> None:
    """
    Print the given environment in a form that can be evaluated by a shell.
    """
    for k, v in env.items():
        print(f"{this_module.name}: {'Would set' if output is None else 'Setting'} "
              f"{k} to {shlex.quote(redact(k, v))}",
              file=sys.stderr)
        if output is not None:
            print(f'export {k}={shlex.quote(v)}', file=output)


def redact(k: str, v: str) -> str:
    forbidden = ('secret', 'password', 'token')
    return 'REDACTED' if any(s in k.lower() for s in forbidden) else v


def main():
    # Buffer the output to reduce the chance of partial output in case of error
    # confusing the shell's eval. Note the  `|| echo false` in the recommended
    # usage below.
    output = None if os.isatty(sys.stdout.fileno()) else StringIO()
    env = load_env()
    resolved_env = resolve_env(env)
    export_env(resolved_env, output)
    if output is None:
        print("\nStdout appears to be a terminal. No output was generated "
              "other than the usual redacted diagnostic output to stderr. To "
              "avoid this, pass the program's output to your shell's `eval`:\n"
              f"eval $(python3 {sys.argv[0]}) || echo false", file=sys.stderr)
        sys.exit(1)
    else:
        print(output.getvalue(), file=sys.stdout)


if __name__ == '__main__':
    main()
