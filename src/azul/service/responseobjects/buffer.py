from logging import getLogger
from io import BytesIO
from typing import Callable

logger = getLogger(__name__)


class FlushableBuffer(BytesIO):
    """
    Allows the buffer to be passed on to the callback function before being removed from the memory.
    """

    def __init__(self, min_size: int, inbetween_callback: Callable):
        super(FlushableBuffer, self).__init__()
        self.min_size = min_size
        self.inbetween_callback = inbetween_callback
        self._remaining_size = 0
        self._total_size = 0

    def write(self, b: bytes):
        byte_count = len(b)
        super().write(b)
        self._remaining_size += byte_count
        self._total_size += byte_count

    def flush(self, check_limit: bool = True):
        if check_limit and self._remaining_size < self.min_size:
            return

        self.inbetween_callback(self.getvalue())
        self.truncate(0)
        self.seek(0)
        self._remaining_size = 0

    @property
    def remaining_size(self):
        return self._remaining_size

    @property
    def total_size(self):
        return self._total_size
