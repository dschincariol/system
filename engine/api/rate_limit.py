"""Small token-bucket rate limiter for API operator endpoints."""

from __future__ import annotations

import math
import os
import threading
import time
from dataclasses import dataclass
from typing import Callable


DEFAULT_TOKEN_LIMIT_PER_MIN = 60
DEFAULT_IP_LIMIT_PER_MIN = 10
DEFAULT_DESTRUCTIVE_LIMIT_PER_MIN = 6


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        value = int(str(os.environ.get(name, "") or "").strip())
    except Exception:
        return int(default)
    return max(int(minimum), int(value))


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after_s: int = 0
    bucket_key: str = ""
    limit_per_min: int = 0


class TokenBucket:
    def __init__(
        self,
        *,
        capacity: int,
        refill_per_s: float,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.capacity = max(1, int(capacity))
        self.refill_per_s = max(0.000001, float(refill_per_s))
        self._clock = clock or time.monotonic
        self._tokens = float(self.capacity)
        self._updated_at = float(self._clock())

    @property
    def tokens(self) -> float:
        self._refill()
        return float(self._tokens)

    def _refill(self) -> None:
        now = float(self._clock())
        elapsed = max(0.0, now - self._updated_at)
        if elapsed:
            self._tokens = min(
                float(self.capacity),
                self._tokens + (elapsed * self.refill_per_s),
            )
            self._updated_at = now

    def consume(self, amount: float = 1.0) -> RateLimitDecision:
        need = max(0.0, float(amount))
        self._refill()
        if self._tokens >= need:
            self._tokens -= need
            return RateLimitDecision(allowed=True)

        deficit = need - self._tokens
        retry_after = max(1, int(math.ceil(deficit / self.refill_per_s)))
        return RateLimitDecision(allowed=False, retry_after_s=retry_after)


class ApiRateLimiter:
    def __init__(
        self,
        *,
        token_limit_per_min: int | None = None,
        ip_limit_per_min: int = DEFAULT_IP_LIMIT_PER_MIN,
        destructive_limit_per_min: int | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.token_limit_per_min = int(
            token_limit_per_min
            if token_limit_per_min is not None
            else _env_int(
                "TS_API_RATE_LIMIT_TOKEN_PER_MIN",
                DEFAULT_TOKEN_LIMIT_PER_MIN,
            )
        )
        self.ip_limit_per_min = int(ip_limit_per_min)
        self.destructive_limit_per_min = int(
            destructive_limit_per_min
            if destructive_limit_per_min is not None
            else _env_int(
                "TS_API_RATE_LIMIT_DESTRUCTIVE_PER_MIN",
                DEFAULT_DESTRUCTIVE_LIMIT_PER_MIN,
            )
        )
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._buckets: dict[str, TokenBucket] = {}

    def _bucket(self, key: str, limit_per_min: int) -> TokenBucket:
        limit = max(1, int(limit_per_min))
        existing = self._buckets.get(key)
        if (
            existing is not None
            and existing.capacity == limit
            and abs(existing.refill_per_s - (limit / 60.0)) < 0.000001
        ):
            return existing
        bucket = TokenBucket(
            capacity=limit,
            refill_per_s=limit / 60.0,
            clock=self._clock,
        )
        self._buckets[key] = bucket
        return bucket

    def check(
        self,
        *,
        token: str | None = None,
        ip: str | None = None,
        destructive: bool = False,
    ) -> RateLimitDecision:
        token_value = str(token or "").strip()
        ip_value = str(ip or "").strip() or "unknown"
        if token_value:
            limit = (
                self.destructive_limit_per_min
                if destructive
                else self.token_limit_per_min
            )
            key = f"token:{token_value}"
        else:
            limit = (
                min(self.ip_limit_per_min, self.destructive_limit_per_min)
                if destructive
                else self.ip_limit_per_min
            )
            key = f"ip:{ip_value}"

        with self._lock:
            decision = self._bucket(key, limit).consume(1.0)

        return RateLimitDecision(
            allowed=bool(decision.allowed),
            retry_after_s=int(decision.retry_after_s),
            bucket_key=key,
            limit_per_min=int(limit),
        )


def build_default_rate_limiter() -> ApiRateLimiter:
    return ApiRateLimiter()


__all__ = [
    "ApiRateLimiter",
    "RateLimitDecision",
    "TokenBucket",
    "build_default_rate_limiter",
]
