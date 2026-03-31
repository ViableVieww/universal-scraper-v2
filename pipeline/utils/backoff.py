from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")

SERVICE_BACKOFF: dict[str, tuple[float, float]] = {
    "dns": (0.5, 8.0),
    "serper": (1.0, 32.0),
    "brave": (1.0, 32.0),
    "zuhal": (1.0, 64.0),
}


async def with_backoff(
    coro_factory: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 32.0,
    jitter: float = 0.2,
    retryable: Callable[[Exception], bool] | None = None,
    on_retry: Callable[[int, Exception, float], None] | None = None,
) -> T:
    """Execute an async operation with exponential backoff and jitter.

    Args:
        coro_factory: Zero-arg callable returning a fresh coroutine each call.
        max_attempts: Total attempts before re-raising the last exception.
        base_delay: Initial delay in seconds.
        max_delay: Maximum delay cap in seconds.
        jitter: +/- percentage applied to computed delay (0.2 = +/-20%).
        retryable: Predicate that returns True if the exception warrants a retry.
                   If None, all exceptions are retried.
        on_retry: Callback receiving (attempt_number, exception, delay_seconds).
    """
    last_exc: Exception | None = None

    for attempt in range(max_attempts):
        try:
            return await coro_factory()
        except Exception as exc:
            last_exc = exc

            if retryable and not retryable(exc):
                raise

            if attempt == max_attempts - 1:
                raise

            delay = min(base_delay * (2 ** attempt), max_delay)
            delay *= random.uniform(1 - jitter, 1 + jitter)

            if on_retry:
                on_retry(attempt + 1, exc, delay)

            await asyncio.sleep(delay)

    raise last_exc  # type: ignore[misc]
