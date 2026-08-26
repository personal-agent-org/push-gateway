"""Rate limiting keyed on sha256(push_token), with pluggable stores.

A store error fails CLOSED: the decision reads raise :class:`StoreUnavailable` and the
gateway refuses the push. Failing open was the older behaviour and it was the wrong trade --
the limiter is the only thing standing between one looping device and the shared FCM quota
every self-hosted instance draws from, so losing Redis quietly turned the relay into an open
one, and the only sign was a log line.

Two deliberate exceptions to "closed":

* **Writes stay best-effort.** ``record_result`` runs AFTER a push was delivered and
  ``tombstone`` after Firebase said the token is dead. Refusing at that point would turn a
  delivered push into an error the caller retries -- and a retry sends the notification twice.
* **A failed tombstone READ refuses with 503, never 404.** 404 means "this token is dead" and
  makes the instance throw it away permanently. A Redis blip must not destroy valid tokens, so
  the unavailable case is a retryable refusal instead.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Callable
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


class StoreUnavailable(Exception):
    """A limit could not be evaluated. The caller refuses the request, retryably."""


class RateLimiter:
    def __init__(
        self,
        store: Store,
        global_per_minute: int,
        on_store_failure: Callable[[str], None] | None = None,
    ) -> None:
        self._store = store
        # Called with the operation name whenever a store call fails, whether it went on to
        # refuse the request or was one of the best-effort writes. A limiter that cannot reach
        # its store is the one failure here that otherwise looks exactly like a healthy one.
        self._on_store_failure = on_store_failure or (lambda _op: None)
        self._global_per_minute = global_per_minute

    # --- decision reads: fail CLOSED ---------------------------------------------------
    async def _incr(self, key: str, ttl: int) -> int:
        try:
            return await self._store.incr(key, ttl)
        except Exception as exc:
            log.warning("store incr failed, refusing: %s", exc)
            self._on_store_failure("incr")
            raise StoreUnavailable("incr") from exc

    async def _get(self, key: str) -> int:
        try:
            return await self._store.get_int(key)
        except Exception as exc:
            log.warning("store get failed, refusing: %s", exc)
            self._on_store_failure("get")
            raise StoreUnavailable("get") from exc

    async def _has(self, key: str) -> bool:
        try:
            return await self._store.has_flag(key)
        except Exception as exc:
            log.warning("store check failed, refusing: %s", exc)
            self._on_store_failure("has")
            raise StoreUnavailable("has") from exc

    # --- writes: best-effort ------------------------------------------------------------
    # These run after the outcome is already decided. Refusing here would turn a delivered
    # push into an error the caller retries, and the retry sends it a second time.
    async def _set(self, key: str, ttl: int) -> None:
        try:
            await self._store.set_flag(key, ttl)
        except Exception as exc:
            log.warning("store set failed, ignoring: %s", exc)
            self._on_store_failure("set")

    async def _incr_best_effort(self, key: str, ttl: int) -> None:
        try:
            await self._store.incr(key, ttl)
        except Exception as exc:
            log.warning("store incr failed, ignoring: %s", exc)
            self._on_store_failure("incr")

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
        await self._incr_best_effort(f"{kind}:{th}:{utc_day()}", seconds_to_utc_midnight() + 3600)
