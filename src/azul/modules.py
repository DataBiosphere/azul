import importlib.util
import os

from azul import config


def load_module(path: str, module_name: str):
    """
    Load a module from the .py file at the given path without affecting `sys.path` or `sys.modules`.

    :param path: the file system path to the module file (typically ending in .py)

    :param module_name: the value to assign to the __name__ attribute of the module.

    :return: the module
    """
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert path == module.__file__
    assert module.__name__ == module_name
    return module


def load_app_module(lambda_name):
    path = os.path.join(config.project_root, 'lambdas', lambda_name, 'app.py')
    return load_module(path, '__main__')
