from collections.abc import (
    Mapping,
)
from importlib.abc import (
    Loader,
)
import importlib.util
import os
from pathlib import (
    Path,
)
from typing import (
    Any,
)

from azul import (
    R,
    config,
)
from azul.types import (
    not_none,
)


def load_module(path: str, module_name: str):
    """
    Load a module from the .py file at the given path without affecting
    `sys.path` or `sys.modules`.

    :param path: the file system path to the module file
                 (typically ending in .py)

    :param module_name: the value to assign to the __name__ attribute of the
                        module.

    :param module_attributes: a dictionary of additional attributes to set on
                              the module before executing it. These attributes
                              will be available at module scope when it is first
                              executed

    :return: the module
    """
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None, R('Unable to load module', module_name, path)
    assert isinstance(spec.loader, Loader)
    module = importlib.util.module_from_spec(spec)
    setattr(module, _loaded_dynamically, True)
    assert Path(path).samefile(not_none(module.__file__))
    assert module.__name__ == module_name
    spec.loader.exec_module(module)
    return module


def load_app_module(lambda_name):
    path = os.path.join(config.project_root, 'lambdas', lambda_name, 'app.py')
    # Changing the module name here will break doctest discoverability
    return load_module(path, f'lambdas.{lambda_name}.app')


def load_script(script_name: str):
    path = os.path.join(config.project_root, 'scripts', f'{script_name}.py')
    return load_module(path, script_name)


_loaded_dynamically = '__azul_loaded_dynamically__'


def module_loaded_dynamically(module_globals: Mapping[str, Any]) -> bool:
    """
    Determine if a module was loaded dynamically

    :param module_globals: The return value of globals() when invoked from
                           within the module in question

    :return: True, if the module with the given globals was loaded dynamically
             via a facility in this module, False otherwise
    """
    return module_globals.get(_loaded_dynamically, False)
