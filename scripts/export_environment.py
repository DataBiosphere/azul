from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    ChainMap,
)
from collections.abc import (
    Iterator,
    Mapping,
)
from importlib.abc import (
    Loader,
)
import importlib.util
from io import (
    StringIO,
)
import json
import os
from pathlib import (
    Path,
)
import shlex
import sys
from typing import (
    Iterable,
    Optional,
    TextIO,
    Tuple,
    cast,
)

this_module = Path(__file__)

root_dir = this_module.parent.parent

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

        https://docs.python.org/3.11/library/string.html#format-string-syntax

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
            f'Component {component} refers to non-existent parent deployment {parent}'
        )


def load_env() -> Tuple[Environment, Optional[str]]:
    """
    Load environment.py and environment.local.py modules from the project
    root and the current active deployment directory, call their env()
    function to obtain the environment dictionary and merge the dictionaries.
    The entries from an environment.local.py take precedence over those from
    a corresponding environment.py in the same directory. The modules from
    the deployment directory take precedence over ones in the project root.
    """

    deployments_dir = root_dir / 'deployments'
    active_deployment_dir = deployments_dir / '.active'
    if active_deployment_dir.exists():
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
        warning = None
    else:
        warning = (
            f'No active deployment (missing {str(active_deployment_dir)!r}). '
            f'Loaded global defaults only.'
        )
        active_deployment_dir = None
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
        active_deployment_dir and _load(active_deployment_dir, local=True),
        parent_deployment_dir and _load(parent_deployment_dir, local=True),
        _load(root_dir, local=True),
        active_deployment_dir and _load(active_deployment_dir),
        parent_deployment_dir and _load(parent_deployment_dir),
        _load(root_dir)
    ]
    # Note that ChainMap looks only considers the second mapping in the chain
    # if a key is absent from the first one. IOW, the earlier mappings in the
    # chain take precedence over later ones.
    env = ChainMap(dict(project_root=str(root_dir)))
    for module in modules:
        if module is not None:
            # https://github.com/python/typeshed/issues/6042
            # noinspection PyTypeChecker
            env.maps.append(filter_env(module.env()))
    return env, warning


def filter_env(env: DraftEnvironment) -> Environment:
    """
    Remove entries whose value is None from the environment. None values are
    permitted in environment.py modules such that those entries can be
    documented without having to define a value.
    """
    return {k: v for k, v in env.items() if v is not None}


class ResolvedEnvironment(DraftEnvironment):

    def __init__(self, env: Environment) -> None:
        super().__init__()
        self._env = env
        self._keys = set()

    def __getitem__(self, k: str) -> Optional[str]:
        if k.isidentifier():
            if k in self._keys:
                raise RecursionError('Circular reference', k)
            else:
                v = self._env[k]
                if v is None:
                    if self._keys:
                        raise KeyError
                    else:
                        return v
                elif not isinstance(v, str):
                    raise TypeError('Referenced must be a string or None', v)
                else:
                    self._keys.add(k)
                    try:
                        if v and (v[0] == '{' and v[-1] == '}' or v[0] == '[' and v[-1] == ']'):
                            try:
                                v = json.loads(v)
                            except ValueError:
                                pass
                            else:
                                return json.dumps(self._format(v))
                        try:
                            return self._format(v)
                        except KeyError:
                            return None
                        except ValueError:
                            return v
                    finally:
                        self._keys.remove(k)
        else:
            # For some reason, format_map does not enforce the syntax of the
            # format string correctly:
            # https://docs.python.org/3.8/library/string.html#grammar-token-field-name
            raise ValueError('Not a valid variable reference', k)

    def _format(self, v):
        if isinstance(v, dict):
            return {k: self._format(v) for k, v in v.items()}
        elif isinstance(v, list):
            return [self._format(v) for v in v]
        elif isinstance(v, str):
            return v.format_map(self)
        else:
            return v

    def __len__(self) -> int:
        return len(self._env)

    def __iter__(self) -> Iterator[str]:
        return iter(self._env)

    def keys(self) -> Iterable[str]:
        return self._env.keys()

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

    >>> resolve_env({'x': 42})
    Traceback (most recent call last):
    ...
    TypeError: ('Referenced must be a string or None', 42)

    Value referencing another variable:

    >>> resolve_env({'x': '{y}', 'y': '42'})
    {'x': '42', 'y': '42'}

    >>> resolve_env({'x': '{y}', 'y': 42})
    Traceback (most recent call last):
    ...
    TypeError: ('Referenced must be a string or None', 42)

    A reference to a missing variable, or a variable whose value is None, causes
    the entire referencing value to be undefined. This is unlike Unix shell
    substitution, where the reference would be replaced with the empty string.
    It's more akin to `null` propagation in SQL. We do this so that we don't
    emit partially populated values, which allows for composing defaults that
    are dependendent on variables defined in overriding environments.

    >>> resolve_env({'x': 'a{y}b'})
    {'x': None}

    >>> resolve_env({'x': 'a{y}b', 'y': None})
    {'x': None, 'y': None}

    Transitive reference:

    >>> resolve_env({'x': '{y}', 'y': '{z}', 'z': '42'})
    {'x': '42', 'y': '42', 'z': '42'}

    Circular references, direct or indirect are not supported:

    >>> resolve_env({'x': '{x}'})
    Traceback (most recent call last):
    ...
    RecursionError: ('Circular reference', 'x')

    >>> resolve_env({'x': '{y}', 'y': '{x}'})
    Traceback (most recent call last):
    ...
    RecursionError: ('Circular reference', 'x')

    >>> resolve_env({'x': '{y}', 'y': '{z}', 'z': '{x}'})
    Traceback (most recent call last):
    ...
    RecursionError: ('Circular reference', 'x')

    Literal (escaped) curly braces:

    >>> resolve_env({'o': '{{', 'c': '}}', 'oc': '{{}}'})
    {'o': '{', 'c': '}', 'oc': '{}'}

    Generated references are not resolved:

    >>> resolve_env({'x': '{o}y{c}', 'y': '42', 'o': '{{', 'c': '}}'})
    {'x': '{y}', 'y': '42', 'o': '{', 'c': '}'}

    If they were, the result would be:

    {'x': '42', 'y': '42', 'o': '{', 'c': '}'}

    Serialized JSON objects and arrays are supported, as long as they make up
    the entire variable value. Variable references in string values of JSON
    objects are translated, as are those in string elements of JSON arrays.

    >>> resolve_env({'x': '{}'})
    {'x': '{}'}

    >>> resolve_env({'x':'{ }'})
    {'x': '{}'}

    >>> resolve_env({'x':' { }'})
    {'x': ' { }'}

    >>> resolve_env({'x':'[]'})
    {'x': '[]'}

    >>> resolve_env({'x':'[ ]'})
    {'x': '[]'}

    >>> resolve_env({'x':' [ ]'})
    {'x': ' [ ]'}

    >>> resolve_env({'x': '{"foo": "bar"}'})
    {'x': '{"foo": "bar"}'}

    >>> resolve_env({'x': '{"foo": "bar"}', 'y': '{x}'})
    {'x': '{"foo": "bar"}', 'y': '{"foo": "bar"}'}

    >>> resolve_env({'x': '{"foo": ["{y}","{y}"]}', 'y': 'bar'})
    {'x': '{"foo": ["bar", "bar"]}', 'y': 'bar'}

    >>> resolve_env({'x': '[42, null, true, {}, "b{y}"]', 'y': 'ar'})
    {'x': '[42, null, true, {}, "bar"]', 'y': 'ar'}
    """
    return dict(ResolvedEnvironment(env))


azul_env_vars = 'azul_env_vars'


def export_env(env: Environment, output: Optional[TextIO]) -> None:
    """
    Print the given environment in a form that can be evaluated by the Bash
    shell, unsetting variables that are no longer used.
    """
    try:
        old_vars = os.environ[azul_env_vars]
    except KeyError:
        old_vars = set()
    else:
        old_vars = set(old_vars.split(','))
    assert not any(',' in var for var in env), env
    env = {
        azul_env_vars: ','.join(env.keys()),
        **env
    }
    for unset in old_vars - env.keys():
        assert unset != azul_env_vars
        print(f"{this_module.name}: {'Would unset' if output is None else 'Unsetting'} "
              f"{unset}",
              file=sys.stderr)
        if output is not None:
            print(f'unset {unset}', file=output)
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
    env, warning = load_env()
    resolved_env = resolve_env(env)
    export_env(resolved_env, output)
    if warning:
        print(warning, file=sys.stderr)
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
