from logging import getLogger
from io import StringIO
from threading import Lock
from typing import Callable

logger = getLogger(__name__)


class Buffer(StringIO):

    def __init__(self, limit_size: int, inbetween_callback: Callable):
        super(Buffer, self).__init__()
        self.limit_size = limit_size
        self.inbetween_callback = inbetween_callback
        self.__remaining_size = 0
        self.__total_size = 0
        self.__pointer_lock = Lock()

    def write(self, s: str):
        self.__pointer_lock.acquire()
        byte_count = len(s.encode())
        self.__remaining_size += byte_count
        self.__total_size += byte_count

        super().write(s)
        self.__pointer_lock.release()

    def flush(self, check_limit: bool = True):
        self.__pointer_lock.acquire()
        if check_limit and self.__remaining_size < self.limit_size:
            self.__pointer_lock.release()
            return

        self.inbetween_callback(self.getvalue().encode())
        self.truncate(0)
        self.seek(0)
        self.__remaining_size = 0
        self.__pointer_lock.release()

    @property
    def remaining_size(self):
        return self.__remaining_size

    @property
    def total_size(self):
        return self.__total_size
