"""Rate limiting keyed on sha256(push_token), with pluggable stores.

Every limiter method fails OPEN: a store error logs a warning and returns a
permissive value. Rate-limit state is best-effort protection, not correctness.
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from .config import BURST_PER_MINUTE, IP_PER_MINUTE, TOMBSTONE_TTL_SECONDS

log = logging.getLogger("push_gateway.limits")


def token_hash(push_token: str) -> str:
    return hashlib.sha256(push_token.encode("utf-8")).hexdigest()


def next_utc_midnight(now: datetime | None = None) -> datetime:
    now = now or datetime.now(UTC)
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def seconds_to_utc_midnight() -> int:
    return max(1, int((next_utc_midnight() - datetime.now(UTC)).total_seconds()))


def utc_midnight_iso() -> str:
    return next_utc_midnight().isoformat().replace("+00:00", "Z")


def utc_day() -> str:
    return datetime.now(UTC).strftime("%Y%m%d")


class Store(Protocol):
    async def incr(self, key: str, ttl: int) -> int: ...
    async def get_int(self, key: str) -> int: ...
    async def set_flag(self, key: str, ttl: int) -> None: ...
    async def has_flag(self, key: str) -> bool: ...


class MemoryStore:
    """Single-process store: key -> (value, expires_at). Good enough for BYO."""

    def __init__(self) -> None:
        self._data: dict[str, tuple[int, float]] = {}
        self._ops = 0

    def _sweep(self) -> None:
        self._ops += 1
        if self._ops % 4096:
            return
        now = time.monotonic()
        for key in [k for k, (_, exp) in self._data.items() if exp <= now]:
            del self._data[key]

    def _live(self, key: str) -> int | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        if entry[1] <= time.monotonic():
            del self._data[key]
            return None
        return entry[0]

    async def incr(self, key: str, ttl: int) -> int:
        self._sweep()
        value = self._live(key)
        if value is None:
            self._data[key] = (1, time.monotonic() + ttl)
            return 1
        self._data[key] = (value + 1, self._data[key][1])
        return value + 1

    async def get_int(self, key: str) -> int:
        return self._live(key) or 0

    async def set_flag(self, key: str, ttl: int) -> None:
        self._data[key] = (1, time.monotonic() + ttl)

    async def has_flag(self, key: str) -> bool:
        return self._live(key) is not None


class RedisStore:
    """Shared store for multi-replica deployments. All keys are TTL'd."""

    def __init__(self, url: str) -> None:
        import redis.asyncio as aioredis

        self._redis = aioredis.from_url(url, decode_responses=True)

    async def incr(self, key: str, ttl: int) -> int:
        pipe = self._redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, ttl, nx=True)
        count, _ = await pipe.execute()
        return int(count)

    async def get_int(self, key: str) -> int:
        value = await self._redis.get(key)
        return int(value) if value else 0

    async def set_flag(self, key: str, ttl: int) -> None:
        await self._redis.set(key, "1", ex=ttl)

    async def has_flag(self, key: str) -> bool:
        return bool(await self._redis.exists(key))


@dataclass(slots=True)
class DailyCounts:
    successful: int
    errors: int


class RateLimiter:
    def __init__(self, store: Store, global_per_minute: int) -> None:
        self._store = store
        self._global_per_minute = global_per_minute

    # Fail-open wrappers: on store errors pretend the key was never seen.
    async def _incr(self, key: str, ttl: int) -> int:
        try:
            return await self._store.incr(key, ttl)
        except Exception as exc:
            log.warning("store incr failed, failing open: %s", exc)
            return 1

    async def _get(self, key: str) -> int:
        try:
            return await self._store.get_int(key)
        except Exception as exc:
            log.warning("store get failed, failing open: %s", exc)
            return 0

    async def _set(self, key: str, ttl: int) -> None:
        try:
            await self._store.set_flag(key, ttl)
        except Exception as exc:
            log.warning("store set failed, failing open: %s", exc)

    async def _has(self, key: str) -> bool:
        try:
            return await self._store.has_flag(key)
        except Exception as exc:
            log.warning("store check failed, failing open: %s", exc)
            return False

    async def ip_allowed(self, ip: str) -> bool:
        minute = int(time.time() // 60)
        return await self._incr(f"ip:{ip}:{minute}", 120) <= IP_PER_MINUTE

    async def is_tombstoned(self, th: str) -> bool:
        return await self._has(f"tomb:{th}")

    async def tombstone(self, th: str) -> None:
        await self._set(f"tomb:{th}", TOMBSTONE_TTL_SECONDS)

    async def burst_allowed(self, th: str) -> bool:
        minute = int(time.time() // 60)
        return await self._incr(f"burst:{th}:{minute}", 120) <= BURST_PER_MINUTE

    async def global_allowed(self) -> bool:
        minute = int(time.time() // 60)
        return await self._incr(f"global:{minute}", 120) <= self._global_per_minute

    async def daily_counts(self, th: str) -> DailyCounts:
        day = utc_day()
        return DailyCounts(
            successful=await self._get(f"day_ok:{th}:{day}"),
            errors=await self._get(f"day_err:{th}:{day}"),
        )

    async def record_result(self, th: str, *, success: bool) -> None:
        kind = "day_ok" if success else "day_err"
        # Extra hour of TTL so counts stay readable right after the window flips.
        await self._incr(f"{kind}:{th}:{utc_day()}", seconds_to_utc_midnight() + 3600)
