from abc import (
    ABCMeta,
    abstractmethod,
)
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
    as_completed,
)
import logging
import threading
import time
from typing import (
    Iterable,
    List,
    Optional,
)

from azul import require

logger = logging.getLogger(__name__)


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


class DeferredTaskExecutor(metaclass=ABCMeta):
    """
    A wrapper around ThreadPoolExecutor that allows for conveniently deferring method calls to be performed
    concurrently, optionally after other deferred method calls have completed and/or a given amount of time has
    passed.

    >>> class MyExecutor(DeferredTaskExecutor):
    ...
    ...     def __init__(self) -> None:
    ...         super().__init__(num_workers=2)
    ...         self.a, self.b, self.c, self.d = None, None, None, None
    ...
    ...     def _run(self):
    ...         foo = self._defer(self.set, time.time(), 1, b=2, delay=1.23)
    ...         self._defer(self.sum, run_after=[foo])
    ...
    ...     def set(self, start, a, b=None):
    ...         self.delta = time.time() - start
    ...         self._defer(self.never, run_after=[self._defer(self.err)])
    ...         self.a = a
    ...         self.b = b
    ...
    ...     def sum(self):
    ...         self.c = self.a + self.b
    ...
    ...     def err(self):
    ...         raise ValueError(123)
    ...
    ...     def never(self):
    ...         self.d = 1

    >>> e = MyExecutor()
    >>> e.run()  # err() raises an exception
    [ValueError(123)]

    >>> 1.23 <= e.delta < 2 # set() runs after the given delay, but not much later
    True

    >>> e.a, e.b, e.c, e.d  # sum() runs after set(), and never() does not run at all
    (1, 2, 3, None)
    """

    @abstractmethod
    def _run(self) -> None:
        """
        Subclasses override this method for the top-level task they'd like to be performed.

        This method typically calls _defer() at least once.
        """
        raise NotImplementedError

    def __init__(self, num_workers: Optional[int] = None) -> None:
        super().__init__()
        self.tpe = ThreadPoolExecutor(max_workers=num_workers)
        self.futures = set()

    def run(self) -> List[Exception]:
        """
        Clients call this method to initiate the top-level task.

        :return: A list of the exceptions that occurred in deferred methods.
        """
        with self.tpe:
            self._run()
            return self._collect_futures()

    def _defer(self,
               callable_,
               *args,
               run_after: Optional[Iterable[Future]] = None,
               start_time: Optional[float] = None,
               delay: Optional[float] = None,
               **kwargs) -> Future:
        """
        Invoke the given callable (typically a method of this class or a function nested in a method) with the given
        arguments and after the preconditions are met.

        :param callable_: the callable to invoke

        :param args: the positional arguments to pass to the callable

        :param kwargs: the keyword arguments to pass to the callable

        :param run_after: the futures representing other callables that must complete successfully before
                          this callable is invoked

        :param start_time: an optional absolute point in time (as returned by time.time())
                           before which that task will not be invoked, defaults to now

        :param delay: an optional number of seconds that will be added to start_time

        :return: a Future instance representing the callable
        """
        if start_time is None:
            if delay is not None:
                start_time = time.time() + delay
        else:
            if delay is not None:
                start_time = start_time + delay

        def run_if_possible():
            can_run = self._check_run_after(run_after) if run_after else True
            if can_run is False:
                raise self.UnsatisfiedDependency()
            elif can_run is True and (start_time is None or start_time < time.time()):
                return callable_(*args, **kwargs)
            else:
                return self._defer(callable_, *args, run_after=run_after, start_time=start_time, **kwargs)

        def log_exceptions_early(future):
            e = future.exception()
            if e is not None and not isinstance(e, self.UnsatisfiedDependency):
                logger.warning('Exception in deferred callable', exc_info=True)

        future = self.tpe.submit(run_if_possible)
        future.add_done_callback(log_exceptions_early)
        self.futures.add(future)
        return future

    class UnsatisfiedDependency(RuntimeError):
        pass

    def _check_run_after(self, run_after: Iterable[Future]) -> Optional[bool]:
        for future in run_after:
            while True:
                if future.done():
                    if future.exception():
                        return False  # at least one future failed
                    else:
                        # Tasks that call _defer() will return a future which needs to be examined recursively.
                        # This tail recursion could be of arbitrary depth.
                        result = future.result()
                        if isinstance(result, Future):
                            future = result
                        else:
                            break
                else:
                    return None  # some futures are not yet done
        return True  # all futures succeeded

    def _collect_futures(self):
        errors = []
        num_secondary_errors = 0
        while self.futures:
            for future in as_completed(self.futures):
                e = future.exception()
                if e is not None:
                    if isinstance(e, self.UnsatisfiedDependency):
                        num_secondary_errors += 1
                    else:
                        errors.append(e)
                self.futures.remove(future)
        # We cannot have any secondary errors without primary ones
        assert bool(errors) or not bool(num_secondary_errors)
        return errors
