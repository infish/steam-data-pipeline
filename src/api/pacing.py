"""Minimum spacing between the requests to one API.

A Pacer keeps at least `interval` seconds between the starts of consecutive
requests to the same API. It waits only for the rest of the interval: time
already spent on the previous response, on parsing and loading it, or on
requests to other APIs counts towards it. The rate limits stay exactly as
strict as a fixed pause before every request, without the extra waiting
(docs/optimizations.md).
"""

import time


class Pacer:
    def __init__(self, interval, sleep_fn=time.sleep, clock=time.monotonic):
        self.interval = interval
        self.sleep_fn = sleep_fn
        self.clock = clock
        self.last_start = None

    def wait(self):
        """Call right before a request; sleeps only what the limit needs."""
        if self.last_start is not None:
            remaining = self.interval - (self.clock() - self.last_start)

            if remaining > 0:
                self.sleep_fn(remaining)

        self.last_start = self.clock()
