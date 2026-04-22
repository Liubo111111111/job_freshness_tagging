from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass


@dataclass(slots=True)
class RuntimeConfig:
    worker_count: int = 4
    provider_rate_limit_per_minute: int = 120
    max_in_flight: int = 8
    timeout_seconds: int = 30
    retry_limit: int = 1


class MinuteRateLimiter:
    def __init__(self, limit_per_minute: int) -> None:
        self.limit_per_minute = limit_per_minute
        self._lock = threading.Lock()
        self._calls: deque[float] = deque()

    def acquire(self) -> None:
        if self.limit_per_minute <= 0:
            return

        while True:
            sleep_seconds = 0.0
            with self._lock:
                now = time.monotonic()
                while self._calls and now - self._calls[0] >= 60:
                    self._calls.popleft()
                if len(self._calls) < self.limit_per_minute:
                    self._calls.append(now)
                    return
                sleep_seconds = max(0.01, 60 - (now - self._calls[0]))
            time.sleep(sleep_seconds)
