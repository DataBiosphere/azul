import enum
from typing import (
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    def auto():
        """
        https://youtrack.jetbrains.com/issue/PY-53388/PyCharm-thinks-enumauto-needs-an-argument#focus=Comments-27-6302771.0-0
        """
        # noinspection PyArgumentList
        return enum.auto()
else:
    auto = enum.auto
