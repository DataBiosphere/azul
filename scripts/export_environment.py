from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    ChainMap,
    defaultdict,
)
import importlib.util
from io import StringIO
import os
from pathlib import Path
from re import compile
import shlex
import sys
from typing import (
    Mapping,
    Optional,
    TextIO,
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

        https://docs.python.org/3.6/library/string.html#format-string-syntax

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
            spec.loader.exec_module(module)
            return module
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


def resolve_env(env: Environment) -> Environment:
    """
    Resolve references to other variables among all values in the given
    environment.

    >>> resolve_env({'x': '{y}', 'y': '42'})
    {'x': '42', 'y': '42'}

    Unmatched curly braces and curly braces that do not surround valid syntax
    keywords will be ignored:

    >>> resolve_env({'x': '{{', 'y': '{ z }', 'z': '{42}'})
    {'x': '{{', 'y': '{ z }', 'z': '{42}'}

    Transitive references are supported:

    >>> resolve_env({'x': '{y}', 'y': '{z}', 'z': '42'})
    {'x': '42', 'y': '42', 'z': '42'}

    Generative references formed during resolution are supported:

    >>> resolve_env({'x': '{y}}', 'y': '{z', 'z': '42'})
    {'x': '42', 'y': '{z', 'z': '42'}

    Consistent with a $missing_var in the shell a references to a missing key
    yields an empty string replacement:

    >>> resolve_env({'x': 'y is {y}'})
    {'x': 'y is '}

    A circular reference causes an exception:

    >>> resolve_env({'x': '{y}', 'y': '{x}'})
    Traceback (most recent call last):
    ...
    ValueError: Circular reference not allowed.
    """
    while True:
        resolved_env = {k: _custom_format_map(k, v, env) for k, v in env.items()}
        if env == resolved_env:
            return resolved_env
        else:
            env = resolved_env


def _custom_format_map(key: str, val: str, mapping: Mapping[str, str]) -> str:
    """
    Works like `val.format_map(mapping)` but ignores curly braces that
    do not wrap valid syntax keywords (e.g. contain spaces, unmatched), and
    replaces curly brace wrapped keywords with an empty string when a matching
    key is not found in the mapping.
    """
    pattern = compile(r'{[^{}\s]+}')  # matches '{FOO}' and not '{F OO}' or '{}'
    match = pattern.search(val)
    while match is not None:
        start, end = match.span()
        substring = match.group()
        if f'{{{key}}}' == substring:
            raise ValueError('Circular reference not allowed.')
        try:
            substring = substring.format_map(defaultdict(str, mapping))
        except ValueError:
            # skip over keywords not supported by format_map()
            start_at = end
        else:
            val = val[0:start] + substring + val[end:]
            start_at = start + len(substring)
        match = pattern.search(val, pos=start_at)
    return val


def export_env(env: Environment, output: Optional[TextIO]) -> None:
    """
    Print the given environment in a form that can be evaluated by a shell.
    """
    for k, v in env.items():
        print(f"{this_module.name}: {'Would set' if output is None else 'Setting'} {k} to {shlex.quote(redact(k, v))}",
              file=sys.stderr)
        if output is not None:
            print(f'export {k}={shlex.quote(v)}', file=output)


def redact(k: str, v: str) -> str:
    return 'REDACTED' if any(s in k.lower() for s in ('secret', 'password', 'token')) else v


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
