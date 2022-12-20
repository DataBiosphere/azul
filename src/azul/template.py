from collections.abc import (
    Mapping,
)
from contextlib import (
    contextmanager,
)
import json
import os
import sys
import tempfile
from typing import (
    Any,
    Optional,
)


def emit(json_doc: Optional[Mapping[str, Any]]):
    with emit_text(remove=json_doc is None) as f:
        json.dump(json_doc, f, indent=4)


@contextmanager
def emit_text(*, remove: bool = False):
    path = sys.argv[1]
    if remove:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        else:
            print(f'Removed {path}')
        with open('/dev/null', 'a') as f:
            yield f
    else:
        f = tempfile.NamedTemporaryFile(mode='w+', dir=os.path.dirname(path), encoding='utf-8', delete=False)
        try:
            yield f
        except BaseException:
            os.unlink(f.name)
            raise
        else:
            print(f"Creating {path}")
            os.rename(f.name, path)
        finally:
            f.close()
