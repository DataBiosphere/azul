from enum import (
    Enum,
    auto,
)
from typing import (
    Set,
)


class ServiceActionType(Enum):
    """
    Identifies the type of action in IAM's service model
    """
    list = auto()
    read = auto()
    write = auto()
    tagging = auto()
    permissions = auto()

    @classmethod
    def for_action_groups(cls, groups: Set[str]) -> 'ServiceActionType':
        if groups == {'ReadWrite', 'ReadOnly'}:
            return cls.read
        elif groups == {'ReadWrite'}:
            return cls.write
        elif groups == {'ReadWrite', 'ReadOnly', 'ListOnly'}:
            return cls.list
        elif groups == {'ReadWrite', 'Tagging'}:
            return cls.tagging
        elif groups == {'Permissions'}:
            return cls.permissions
        else:
            assert False, groups

    def __str__(self):
        return self.name
