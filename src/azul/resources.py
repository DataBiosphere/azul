import os
from typing import (
    Any,
    BinaryIO,
    IO,
    Literal,
    TextIO,
    overload,
)

from azul.lib import (
    R,
)


@overload
def open_resource(*path: str,
                  package_root: str | None = None,
                  binary: Literal[False] = False
                  ) -> TextIO: ...


@overload
def open_resource(*path: str,
                  package_root: str | None = None,
                  binary: Literal[True]
                  ) -> BinaryIO: ...


def open_resource(*path: str,
                  package_root: str | None = None,
                  binary: bool = False
                  ) -> IO[Any]:
    """
    Return a file object for the resources at the given path. A resource is
    a source file that can be loaded at runtime. Resources typically aren't
    Python code. We further distinguish between static resources that are
    committed to source control and dynamic ones that are generated at build
    time. Static resources can be accessed by passing 'static' as the first
    positional argument.

    This method must be called from within a real AWS Lambda execution context.
    A fake one created by `chalice local` or LocalAppTestCase will do provided
    that the `package_root` argument is passed and points to the directory
    that contains the `app.py` module and the `vendor` directory.

    :param path: The path to the resource relative to the `vendor/resources`
                 directory. The last positional argument is the file name.

    :param package_root: See description above

    :param binary: True to load a binary resource
    """
    resource_file = _resource_path(*path, package_root=package_root)
    return open(resource_file, mode='rb' if binary else 'r')


def list_resources(*path: str, package_root: str | None = None) -> list[str]:
    """
    Return the names of the entries in the resource directory at the given
    path, in arbitrary order. See :func:`open_resource` for a description of
    the arguments, except that the last positional argument is the name of a
    directory.
    """
    return os.listdir(_resource_path(*path, package_root=package_root))


def _resource_path(*path: str, package_root: str | None) -> str:
    assert len(path) > 0, R('Must pass at least the file name of the resource')
    if package_root is None:
        module_dir = os.path.dirname(os.path.abspath(__file__))
        assert module_dir.endswith('/azul'), module_dir
        package_root = os.path.dirname(module_dir)
    if package_root.endswith('/src'):
        raise NotInLambdaContextException(package_root)
    vendor_dir = os.path.join(package_root, 'vendor')
    # The `chalice package` command dissolves the content of the `vendor`
    # directory into the package root so in a deployed Lambda function, the
    # vendor directory is gone. During `chalice local` or in a running
    # LocalAppTestCase, the vendor directory still exists.
    resource_dir = vendor_dir if os.path.exists(vendor_dir) else package_root
    return os.path.join(resource_dir, 'resources', *path)


class NotInLambdaContextException(RuntimeError):

    def __init__(self, package_root) -> None:
        super().__init__('The package root suggests that no Lambda context is active',
                         package_root)
