from logging import getLogger
from io import BytesIO
from typing import Callable

logger = getLogger(__name__)


class FlushableBuffer(BytesIO):
    """
    A in-memory buffer that is designed to flush the output to the callback
    function (``inbetween_callback``), either if there exists the size of
    the remaining data is large enough (over ``min_size``) on flush or when
    the buffer is getting closed, before being removed from the memory.

    :param min_size: The minimum size of buffer (byte length)
    :param inbetween_callback: The callback function to receive flushed output
    """

    def __init__(self, min_size: int, inbetween_callback: Callable):
        super(FlushableBuffer, self).__init__()
        self.__min_size = min_size
        self.__inbetween_callback = inbetween_callback
        self.__remaining_size = 0
        self.__total_size = 0

    def write(self, b: bytes):
        byte_count = len(b)
        super().write(b)
        self.__remaining_size += byte_count
        self.__total_size += byte_count

    def flush(self):
        if self.__remaining_size < self.__min_size:
            return
        self.__clean_up()

    def close(self):
        if self.__remaining_size > 0:
            logger.warning(f'Clearing the remaining buffer (approx. {self.__remaining_size} B)')
            self.__clean_up()
        super().close()

    def __clean_up(self):
        self.__inbetween_callback(self.getvalue())
        self.truncate(0)
        self.seek(0)
        self.__remaining_size = 0

    @property
    def remaining_size(self):
        return self.__remaining_size

    @property
    def total_size(self):
        return self.__total_size
