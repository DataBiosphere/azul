import copy
from uuid import UUID

from dataclasses import fields, is_dataclass


def as_json(obj):
    if is_dataclass(obj):
        return {
            'entity_type': type(obj).__name__,  # FIXME: make field
            **{f.name: (as_json(getattr(obj, f.name))) for f in fields(obj) if f.repr}
        }
    elif isinstance(obj, (list, tuple, set)):
        return [as_json(v) for v in obj]
    elif isinstance(obj, dict):
        return {as_json(k): as_json(v) for k, v in obj.items()}
    elif isinstance(obj, UUID):
        return str(obj)
    else:
        return copy.deepcopy(obj)
