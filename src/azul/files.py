from contextlib import contextmanager
import hashlib
import os.path
import tempfile


@contextmanager
def write_file_atomically(path, mode=0o644):
    dir_path, file_name = os.path.split(path)
    fd, temp_path = tempfile.mkstemp(dir=dir_path)
    try:
        with os.fdopen(fd, 'w') as f:
            yield f
        os.chmod(temp_path, mode)
        os.rename(temp_path, path)
    except BaseException:
        os.unlink(temp_path)
        raise


def file_sha1(path):
    """
    >>> file_sha1('/dev/null')
    'da39a3ee5e6b4b0d3255bfef95601890afd80709'

    >>> from tempfile import NamedTemporaryFile
    >>> with NamedTemporaryFile() as f:
    ...     f.write(b'f' * (1024 * 1024 - 1))
    ...     file_sha1(f.name)
    1048575
    'f5e766a4faaac674df1dfb707f6557b67bebe99b'

    >>> with NamedTemporaryFile() as f:
    ...     f.write(b'f' * 1024 * 1024)
    ...     file_sha1(f.name)
    1048576
    'c08874b8aacb429a677f0ad660d64919e7d56734'

    >>> with NamedTemporaryFile() as f:
    ...     f.write(b'f' * (1024 * 1024 + 1))
    ...     file_sha1(f.name)
    1048577
    '6a8e89f614a497f5cf741a50d5c2f3c2e430db4e'
    """
    with open(path, 'rb') as f:
        sha1 = hashlib.sha1()
        while True:
            data = f.read(1024 * 1024)
            if not data:
                break
            sha1.update(data)
        return sha1.hexdigest()
