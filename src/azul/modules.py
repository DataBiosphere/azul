import importlib.util
import os
from typing import (
    Any,
    Mapping,
    Optional,
)

from azul import config


def load_module(path: str,
                module_name: str,
                module_attributes: Optional[Mapping[str, Any]] = None):
    """
    Load a module from the .py file at the given path without affecting `sys.path` or `sys.modules`.

    :param path: the file system path to the module file (typically ending in .py)

    :param module_name: the value to assign to the __name__ attribute of the module.

    :param module_attributes: a dictionary of additional attributes to set on
                              the module before executing it. These attributes
                              will be available at module scope when it is first
                              executed

    :return: the module
    """
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    if module_attributes is not None:
        for k, v in module_attributes.items():
            setattr(module, k, v)
    spec.loader.exec_module(module)
    assert path == module.__file__
    assert module.__name__ == module_name
    return module


def load_app_module(lambda_name, **module_attributes):
    path = os.path.join(config.project_root, 'lambdas', lambda_name, 'app.py')
    # Changing the module name here will break doctest discoverability
    return load_module(path, f'lambdas.{lambda_name}.app', module_attributes)
