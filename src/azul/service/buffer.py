from io import (
    BytesIO,
)
from logging import (
    getLogger,
)
from typing import (
    Callable,
)

logger = getLogger(__name__)


class FlushableBuffer(BytesIO):
    """
    A buffer that flushes the output to a callback function (``callback``),
    when either if the remaining size is large enough (more than ``chunk_size``)
    or when the buffer is closed.

    The callback is invoked zero or more times with an argument that is N bytes
    long, followed by exactly one invocation with an argument that is between 1
    and N bytes long.

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
            offset = 0
            while self.__remaining_size >= self.__chunk_size:
                self.seek(offset)
                self.__callback(self.read(self.__chunk_size))
                offset += self.__chunk_size
                self.__remaining_size -= self.__chunk_size

            # Get the remainder before resetting the pointer.
            self.seek(offset)
            remainder = self.read()

            # Reset the buffer to the empty state.
            self.seek(0)
            self.truncate(0)
            self.__remaining_size = 0

            # Write the remainder back to the buffer.
            self.write(remainder)

    def close(self):
        if self.__remaining_size > 0:
            self.__callback(self.getvalue())
            self.__remaining_size = 0
            # As the buffer is closed, the buffer doesn't need to be reset.
        super().close()

    @property
    def remaining_size(self):
        return self.__remaining_size
