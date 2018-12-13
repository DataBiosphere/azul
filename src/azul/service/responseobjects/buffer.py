from logging import getLogger
from io import BytesIO
from typing import Callable

logger = getLogger(__name__)


class FlushableBuffer(BytesIO):
    """
    A buffer that flushes the output to a callback function (``callback``),
    when either if the remaining size is large enough (more than ``chunk_size``)
    or when the buffer is closed.

    Let ``N`` be the remaining size. On each ``write``, the callback will be
    invoked ``floor(N / chunk_size)`` times. The remaining bytes will not be
    flushed unless the buffer is closed.

    :param chunk_size: The exact size of each chunk
    :param callback: The callback function to receive flushed output
    """

    def __init__(self, chunk_size: int, callback: Callable):
        super(FlushableBuffer, self).__init__()
        self.__chunk_size = chunk_size
        self.__callback = callback
        self.__remaining_size = 0

    def write(self, b: bytes):
        super().write(b)
        self.__remaining_size += len(b)
        if self.__remaining_size >= self.__chunk_size:
            value = self.getbuffer()
            first_index = 0
            last_index = self.__chunk_size
            while last_index <= self.remaining_size:
                self.__callback(value[first_index:last_index])
                first_index = last_index
                last_index += self.__chunk_size
            self.truncate(0)
            self.seek(0)
            self.__remaining_size = 0
            self.write(value[first_index:last_index])

    def close(self):
        if self.__remaining_size > 0:
            logger.debug(f'Clearing the remaining buffer (approx. {self.__remaining_size} B)')
            self.__callback(self.getbuffer())
            self.__remaining_size = 0
            # As the buffer is closed, the buffer doesn't need to be reset.
        super().close()

    @property
    def remaining_size(self):
        return self.__remaining_size
