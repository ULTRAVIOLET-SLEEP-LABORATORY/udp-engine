#! /usr/bin/env python3

# /src/blocking_deque.py
# Author: Joshua Darrow     07.20.2026


from collections import deque


class BlockingDeque:
    '''Deque that blocks read on empty'''

    def __init__(self, maxlen=None):
        self._buffer = deque(maxlen=maxlen)
        self._cond = threading.Condition()

    def append(self, item):
        with self._cond:
            self._buffer.append(item)
            self._cond.notify()          # wake one waiting reader

    def popleft(self, timeout=None):
        with self._cond:
            # wait_for re-checks the condition each time notify() fires,
            # and releases the lock while waiting so append() can proceed
            if not self._cond.wait_for(lambda: len(self._buffer) > 0, timeout=timeout):
                raise TimeoutError
            return self._buffer.popleft()