from enum import (
    EnumMeta,
)


class CaseInsensitiveEnumMeta(EnumMeta):

    def __getitem__(self, item):
        assert isinstance(item, str)
        return super().__getitem__(item.lower())
