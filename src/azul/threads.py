import threading

from azul import require


class Latch:
    """
    >>> l = Latch(1)
    >>> l.decrement(1)  # opens the latch
    >>> l.decrement(1)  # latch already open, but decrements value
    >>> l.value
    -1

    >>> l = Latch(0)  # latch is initially open
    >>> l.decrement(1)
    >>> l.value
    -1

    >>> l = Latch(0)
    >>> l.decrement(0)
    >>> l.value
    0


    >>> l = Latch(value=0)
    >>> l.decrement(0, timeout=0.01)
    >>> l.value
    0

    >>> l = Latch(value=2)
    >>> l.decrement(1, timeout=0.01)  # not enough to open latch, so time out
    Traceback (most recent call last):
    ...
    TimeoutError
    >>> l.decrement(1)  # opens latch
    >>> l.value
    0

    >>> from concurrent.futures import ThreadPoolExecutor
    >>> n = 2
    >>> with ThreadPoolExecutor(max_workers=n) as tpe:
    ...     l = Latch(n)
    ...     fs = [tpe.submit(l.decrement, 1) for i in range(n)]
    >>> [f.result() for f in fs]
    [None, None]

    >>> with ThreadPoolExecutor(max_workers=n) as tpe:
    ...     l = Latch(n+1)
    ...     fs = [tpe.submit(l.decrement, 1, timeout=1) for i in range(n)]
    >>> [f.result() for f in fs]
    Traceback (most recent call last):
    ...
    TimeoutError
    """

    def __init__(self, value):
        require(isinstance(value, int))
        self.value = value
        self.condition = threading.Condition()

    def decrement(self, value, timeout=None):
        require(isinstance(value, int))
        self.condition.acquire()
        try:
            self.value -= value
            if self.value > 0:
                while True:
                    if self.condition.wait(timeout=timeout):
                        if self.value <= 0:
                            break
                    else:
                        raise TimeoutError()
            else:
                self.condition.notifyAll()
        finally:
            self.condition.release()
