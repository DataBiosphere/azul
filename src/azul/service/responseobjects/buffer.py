from logging import getLogger
from io import BytesIO
from typing import Callable

logger = getLogger(__name__)


class BytesBuffer(BytesIO):

    def __init__(self, limit_size: int, inbetween_callback: Callable):
        super(BytesBuffer, self).__init__()
        self.limit_size = limit_size
        self.inbetween_callback = inbetween_callback
        self._remaining_size = 0
        self._total_size = 0

    def write(self, b: bytes):
        byte_count = len(b)
        super().write(b)
        self._remaining_size += byte_count
        self._total_size += byte_count

    def flush(self, check_limit: bool = True):
        if check_limit and self._remaining_size < self.limit_size:
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
