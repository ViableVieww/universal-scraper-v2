from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """Async token bucket rate limiter.

    Enforces a hard ceiling of `capacity` calls per refill period.
    Tokens refill continuously at `refill_rate` tokens/second.
    """

    def __init__(self, capacity: int, refill_rate: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate  # tokens per second
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    async def acquire(self) -> None:
        async with self._lock:
            self._refill()

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return

            wait = (1.0 - self._tokens) / self.refill_rate
            await asyncio.sleep(wait)
            self._refill()
            self._tokens -= 1.0


class CircuitBreaker:
    """Pauses all calls for a cooldown period after being tripped.

    Used for Zuhal 429 responses: trip the breaker, and all concurrent
    consumers block until the cooldown expires.
    """

    def __init__(self, cooldown_seconds: float = 600.0) -> None:
        self.cooldown_seconds = cooldown_seconds
        self._tripped = False
        self._trip_time = 0.0
        self._lock = asyncio.Lock()

    @property
    def is_tripped(self) -> bool:
        if not self._tripped:
            return False
        if time.monotonic() - self._trip_time >= self.cooldown_seconds:
            self._tripped = False
            return False
        return True

    def trip(self) -> None:
        self._tripped = True
        self._trip_time = time.monotonic()

    async def wait_if_tripped(self) -> None:
        if not self._tripped:
            return

        async with self._lock:
            remaining = self.cooldown_seconds - (time.monotonic() - self._trip_time)
            if remaining > 0:
                await asyncio.sleep(remaining)
            self._tripped = False
