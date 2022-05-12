from collections.abc import (
    Mapping,
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
    path = sys.argv[1]
    if json_doc is None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        else:
            print(f"Removing {path}")
    else:
        f = tempfile.NamedTemporaryFile(mode='w+', dir=os.path.dirname(path), encoding='utf-8', delete=False)
        try:
            json.dump(json_doc, f, indent=4)
        except BaseException:
            os.unlink(f.name)
            raise
        else:
            print(f"Creating {path}")
            os.rename(f.name, path)
        finally:
            f.close()
