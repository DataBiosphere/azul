from contextlib import contextmanager
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
