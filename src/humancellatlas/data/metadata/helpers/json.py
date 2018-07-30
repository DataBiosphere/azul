import copy
from uuid import UUID

from dataclasses import field, fields, is_dataclass

from humancellatlas.data.metadata import Entity


def as_json(obj, fld: field = None):
    if is_dataclass(obj):
        return {
            'entity_type': type(obj).__name__,  # FIXME: make field
            **{f.name: as_json(getattr(obj, f.name), f) for f in fields(obj) if f.repr}
        }
    elif isinstance(obj, (list, tuple, set)):
        return [as_json(v) for v in obj]
    elif isinstance(obj, dict):
        if fld:
            # Convert Mapping[UUID, Entity] to List[Entity]. In a JSON structure we typically don't want dynamic keys.
            # That makes it easier to descend a JSON structure using dotted field paths.

            def issubclass_(t, s):
                return isinstance(t, type) and isinstance(s, type) and issubclass(t, s)

            key_type, value_type = fld.type.__args__
            if issubclass_(key_type, UUID) and issubclass_(value_type, Entity):
                return [as_json(v) for v in obj.values()]
            else:
                return {as_json(k): as_json(v) for k, v in obj.items()}
        else:
            return {as_json(k): as_json(v) for k, v in obj.items()}
    elif isinstance(obj, UUID):
        return str(obj)
    else:
        return copy.deepcopy(obj)
