# Copyright (C) 2015-2018 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# 5.14.2018: copied into Toil from https://github.com/BD2KGenomics/bd2k-python-lib
# 3.30.2020: adapted from https://github.com/DataBiosphere/toil/blob/master/src/toil/lib/retry.py

from contextlib import contextmanager
import time

import logging

from typing import (
    Iterator,
    ContextManager,
    Optional,
    Iterable,
    Callable,
)

log = logging.getLogger(__name__)


def never(_: Exception):
    return False


def retry(delays: Iterable[float] = (0, 1, 1, 4, 16, 64),
          timeout: float = 300,
          max_tries: Optional[int] = None,
          predicate: Callable[[Exception], bool] = never) -> Iterator[ContextManager]:
    """
    Retry an operation while the failure matches a given predicate and until a
    given timeout expires, waiting a given amount of time in between attempts.
    This function is a generator that yields contextmanagers. See doctests below
    for example usage.
    :param delays: an iterable yielding the time in seconds to wait before each
        retried attempt, the last element of the iterable will be repeated.
    :param timeout: an overall timeout that should not be exceeded for all
        attempts together. This is a best-effort mechanism only and it won't
        abort an ongoing attempt, even if the timeout expires during that
        attempt.
    :param max_tries: a maximum number of attempts.
    :param predicate: an unary callable returning True if another attempt should
        be made to recover from the given exception. The default value for this
        parameter will prevent any retries!
    :return: a generator yielding context managers, one per attempt

    Retry for a limited amount of time:
    >>> true = lambda _:True
    >>> false = lambda _:False
    >>> i = 0
    >>> for attempt in retry(delays=[0], timeout=.1, predicate=true):
    ...     with attempt:
    ...         i += 1
    ...         raise RuntimeError('foo')
    Traceback (most recent call last):
    ...
    RuntimeError: foo
    >>> i > 1
    True

    If timeout is 0, do exactly one attempt:
    >>> i = 0
    >>> for attempt in retry(timeout=0):
    ...     with attempt:
    ...         i += 1
    ...         raise RuntimeError('foo')
    Traceback (most recent call last):
    ...
    RuntimeError: foo
    >>> i
    1

    Don't attempt more than specified
    >>> i = 0
    >>> for attempt in retry(delays=[0], max_tries=4, predicate=true):
    ...     with attempt:
    ...         i += 1
    ...         raise RuntimeError('foo')
    Traceback (most recent call last):
    ...
    RuntimeError: foo
    >>> i
    4

    Don't retry on success:
    >>> i = 0
    >>> for attempt in retry(delays=[0], timeout=.1, predicate=true):
    ...     with attempt:
    ...         i += 1
    >>> i
    1

    Don't retry on unless predicate returns True:

    >>> i = 0
    >>> for attempt in retry(delays=[0], timeout=.1, predicate=false):
    ...     with attempt:
    ...         i += 1
    ...         raise RuntimeError('foo')
    Traceback (most recent call last):
    ...
    RuntimeError: foo
    >>> i
    1
    """
    if timeout > 0:
        go = [None]
        tries = 0

        @contextmanager
        def repeated_attempt(delay_):
            # @formatter:off
            nonlocal tries
            # @formatter:on
            tries += 1
            try:
                yield
            except Exception as e:
                max_tries_exceeded = max_tries is not None and tries >= max_tries
                if time.time() + delay_ < expiration and predicate(e) and not max_tries_exceeded:
                    log.warning('Got %s, trying again in %is.', e, delay_)
                    time.sleep(delay_)
                else:
                    raise
            else:
                go.pop()

        delays = iter(delays)
        expiration = time.time() + timeout
        delay = next(delays)
        while go:
            yield repeated_attempt(delay)
            delay = next(delays, delay)
    else:
        @contextmanager
        def single_attempt():
            yield

        yield single_attempt()


default_delays = (0, 1, 1, 4, 16, 64)
default_timeout = 300
