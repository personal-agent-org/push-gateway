"""App factory for the Personal Agent push gateway."""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST
from pydantic import ValidationError

from . import __version__
from .config import DAILY_QUOTA, MAX_BODY_BYTES, Settings
from .fcm import FcmClient, SendOutcome, load_credentials
from .limits import (
    DailyCounts,
    MemoryStore,
    RateLimiter,
    RedisStore,
    Store,
    StoreUnavailable,
    seconds_to_utc_midnight,
    token_hash,
    utc_midnight_iso,
)
from .metrics import Metrics
from .schemas import PushRequest

log = logging.getLogger("push_gateway")


def _rate_limits_body(counts: DailyCounts) -> dict[str, object]:
    return {
        "successful": counts.successful,
        "errors": counts.errors,
        "maximum": DAILY_QUOTA,
        "remaining": max(0, DAILY_QUOTA - counts.successful),
        "resetsAt": utc_midnight_iso(),
    }


def _client_ip(request: Request, trust_proxy: bool) -> str:
    # Clients can pre-append X-Forwarded-For values (proxies APPEND), so only
    # proxy-set headers or the last hop, the value our own proxy added, are safe.
    if trust_proxy:
        for header in ("cf-connecting-ip", "x-real-ip"):
            value = request.headers.get(header, "").strip()
            if value:
                return value
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            return forwarded.rsplit(",", 1)[-1].strip()
    return request.client.host if request.client else "unknown"


async def _read_body_capped(request: Request, cap: int) -> bytes | None:
    """Read the body without ever buffering more than cap plus one chunk; None = too large."""
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > cap:
        return None
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > cap:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def create_app(settings: Settings | None = None, store: Store | None = None) -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    settings = settings or Settings.from_env()
    credentials = (
        load_credentials(settings.fcm_credentials_file) if settings.fcm_credentials_file else None
    )
    fcm = FcmClient(credentials) if credentials else None
    if store is None:
        store = RedisStore(settings.redis_url) if settings.redis_url else MemoryStore()
    metrics = Metrics(
        version=__version__,
        upstream_configured=fcm is not None,
        store_backend="redis" if settings.redis_url else "memory",
    )
    # The limiter reports its own fail-open moments: without this, Redis going away turns
    # every limit off and nothing but a log line says so.
    limiter = RateLimiter(store, settings.global_per_minute, on_store_failure=metrics.store_failed)

    log.info(
        "push gateway %s starting: relay=%s store=%s trust_proxy=%s global_per_minute=%d",
        __version__,
        "configured" if fcm else "NOT CONFIGURED (set FCM_CREDENTIALS_FILE)",
        "redis" if settings.redis_url else "memory",
        settings.trust_proxy,
        settings.global_per_minute,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        if fcm is not None:
            await fcm.aclose()

    app = FastAPI(
        title="personal-agent-push-gateway",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )

    async def _limits(th: str) -> dict[str, object]:
        return _rate_limits_body(await limiter.daily_counts(th))

    @app.post("/api/v1/push")
    async def push(request: Request) -> JSONResponse:
        try:
            return await _push(request)
        except StoreUnavailable:
            # The limiter could not decide, so nothing is relayed (fail closed). 503 with
            # Retry-After and NOT 404: the caller must retry later, never conclude that the
            # device token is dead and throw it away.
            metrics.reject("store_unavailable")
            log.warning("refusing push: rate-limit store unavailable")
            return JSONResponse(
                {"error": "unavailable"}, status_code=503, headers={"Retry-After": "30"}
            )

    async def _push(request: Request) -> JSONResponse:
        started = time.monotonic()

        if not await limiter.ip_allowed(_client_ip(request, settings.trust_proxy)):
            metrics.reject("ip_rate_limit")
            return JSONResponse(
                {"error": "rate_limited"}, status_code=429, headers={"Retry-After": "60"}
            )

        body = await _read_body_capped(request, MAX_BODY_BYTES)
        if body is None:
            metrics.reject("too_large")
            return JSONResponse({"error": "too_large"}, status_code=413)
        try:
            req = PushRequest.model_validate_json(body)
        except ValidationError:
            metrics.reject("invalid")
            return JSONResponse({"error": "invalid"}, status_code=400)

        if fcm is None:
            # Counted, because a gateway with no credentials refuses every push and used to
            # look completely idle while doing it.
            metrics.reject("not_configured")
            return JSONResponse({"error": "not_configured"}, status_code=503)

        th = token_hash(req.push_token)

        if await limiter.is_tombstoned(th):
            await limiter.record_result(th, success=False)
            metrics.reject("tombstoned")
            _log_send("tombstoned", th, req, started)
            return JSONResponse(
                {"error": "unregistered", "rateLimits": await _limits(th)},
                status_code=404,
            )

        if not await limiter.burst_allowed(th):
            metrics.reject("burst_rate_limit")
            return JSONResponse(
                {"error": "rate_limited", "rateLimits": await _limits(th)},
                status_code=429,
                headers={"Retry-After": "60"},
            )

        counts = await limiter.daily_counts(th)
        if counts.successful >= DAILY_QUOTA:
            metrics.reject("daily_quota")
            return JSONResponse(
                {"error": "rate_limited", "rateLimits": _rate_limits_body(counts)},
                status_code=429,
                headers={"Retry-After": str(seconds_to_utc_midnight())},
            )

        if not await limiter.global_allowed():
            metrics.reject("global_rate_limit")
            return JSONResponse(
                {"error": "rate_limited"}, status_code=503, headers={"Retry-After": "60"}
            )

        upstream_started = time.monotonic()
        outcome = await fcm.send(
            req.push_token, req.data.frame_json(), req.priority, req.ttl, req.collapse_id
        )
        # Timed around the Firebase call ALONE: the request also spends time in the limiter,
        # and folding that in would hide a slow upstream behind a slow store.
        upstream_seconds = time.monotonic() - upstream_started

        if outcome is SendOutcome.DELIVERED:
            await limiter.record_result(th, success=True)
            metrics.sent("delivered", req.priority, upstream_seconds)
            _log_send("delivered", th, req, started)
            return JSONResponse({"rateLimits": await _limits(th)})

        await limiter.record_result(th, success=False)
        if outcome is SendOutcome.UNREGISTERED:
            await limiter.tombstone(th)
            metrics.sent("unregistered", req.priority, upstream_seconds)
            _log_send("unregistered", th, req, started)
            return JSONResponse(
                {"error": "unregistered", "rateLimits": await _limits(th)},
                status_code=404,
            )
        metrics.sent("upstream_error", req.priority, upstream_seconds)
        _log_send("upstream_error", th, req, started)
        return JSONResponse({"error": "upstream"}, status_code=502)

    @app.get("/api/v1/info")
    async def info() -> dict[str, object]:
        return {
            "name": "personal-agent-push-gateway",
            "version": __version__,
            "platforms": ["android"],
            "max_body_bytes": MAX_BODY_BYTES,
            "daily_quota": DAILY_QUOTA,
        }

    @app.get("/healthz")
    async def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/metrics")
    async def metrics_endpoint() -> Response:
        return Response(metrics.render(), media_type=CONTENT_TYPE_LATEST)

    return app


def _log_send(outcome: str, th: str, req: PushRequest, started: float) -> None:
    # Privacy: only the hash prefix and the frame type, never tokens/ids/bodies/IPs.
    log.info(
        "send outcome=%s token=%s type=%s priority=%s latency_ms=%d",
        outcome,
        th[:8],
        req.data.type,
        req.priority,
        int((time.monotonic() - started) * 1000),
    )
