import copy
from uuid import UUID

from dataclasses import (
    field,
    fields,
    is_dataclass,
)

from humancellatlas.data.metadata.api import Entity


def as_json(obj, fld: field = None):
    if is_dataclass(obj):
        d = {f.name: as_json(getattr(obj, f.name), f) for f in fields(obj) if f.repr}
        if isinstance(obj, Entity):
            d['schema_name'] = obj.schema_name
        return d
    elif isinstance(obj, (list, tuple, set)):
        return [as_json(v) for v in obj]
    elif isinstance(obj, dict):
        if fld:
            # Convert Mapping[UUID, Entity] to List[Entity]. In a JSON structure we typically don't want dynamic keys.
            # That makes it easier to descend a JSON structure using dotted field paths.
            key_type, value_type = fld.type.__args__
            if _issubclass_(key_type, UUID) and _issubclass_(value_type, Entity):
                return [as_json(v) for v in obj.values()]
            else:
                return {as_json(k): as_json(v) for k, v in obj.items()}
        else:
            return {as_json(k): as_json(v) for k, v in obj.items()}
    elif isinstance(obj, UUID):
        return str(obj)
    else:
        return copy.deepcopy(obj)


def _issubclass_(t, s):
    import humancellatlas.data.metadata
    # FIXME: This is ugly for various reasons: We might get a forward ref from a different module, not
    # humancellatlas.data.metadata. _ForwardRef and _eval_type are internals of `typing`. They are exposed via
    # typing.get_type_hints but I am currently struggling to make that work.
    if t.__class__.__name__ == '_ForwardRef':
        t = t._eval_type(localns={}, globalns=humancellatlas.data.metadata.api.__dict__)
    return isinstance(t, type) and isinstance(s, type) and issubclass(t, s)
