"""App factory for the Personal Agent push gateway."""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
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
    seconds_to_utc_midnight,
    token_hash,
    utc_midnight_iso,
)
from .schemas import PushRequest

log = logging.getLogger("push_gateway")

_OUTCOMES = ("delivered", "unregistered", "rate_limited", "invalid", "upstream_error", "tombstoned")


class Metrics:
    def __init__(self) -> None:
        self.sends: dict[str, int] = dict.fromkeys(_OUTCOMES, 0)

    def count(self, outcome: str) -> None:
        self.sends[outcome] = self.sends.get(outcome, 0) + 1

    def render(self) -> str:
        lines = [
            "# HELP sends_total Push relay attempts by outcome.",
            "# TYPE sends_total counter",
        ]
        lines += [f'sends_total{{outcome="{o}"}} {n}' for o, n in self.sends.items()]
        return "\n".join(lines) + "\n"


def _rate_limits_body(counts: DailyCounts) -> dict[str, object]:
    return {
        "successful": counts.successful,
        "errors": counts.errors,
        "maximum": DAILY_QUOTA,
        "remaining": max(0, DAILY_QUOTA - counts.successful),
        "resetsAt": utc_midnight_iso(),
    }


def _client_ip(request: Request, trust_proxy: bool) -> str:
    if trust_proxy:
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def create_app(settings: Settings | None = None, store: Store | None = None) -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    settings = settings or Settings.from_env()
    credentials = (
        load_credentials(settings.fcm_credentials_file) if settings.fcm_credentials_file else None
    )
    fcm = FcmClient(credentials) if credentials else None
    if store is None:
        store = RedisStore(settings.redis_url) if settings.redis_url else MemoryStore()
    limiter = RateLimiter(store, settings.global_per_minute)
    metrics = Metrics()

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
        started = time.monotonic()

        if not await limiter.ip_allowed(_client_ip(request, settings.trust_proxy)):
            metrics.count("rate_limited")
            return JSONResponse(
                {"error": "rate_limited"}, status_code=429, headers={"Retry-After": "60"}
            )

        body = await request.body()
        if len(body) > MAX_BODY_BYTES:
            metrics.count("invalid")
            return JSONResponse({"error": "too_large"}, status_code=413)
        try:
            req = PushRequest.model_validate_json(body)
        except ValidationError:
            metrics.count("invalid")
            return JSONResponse({"error": "invalid"}, status_code=400)

        if fcm is None:
            return JSONResponse({"error": "not_configured"}, status_code=503)

        th = token_hash(req.push_token)

        if await limiter.is_tombstoned(th):
            await limiter.record_result(th, success=False)
            metrics.count("tombstoned")
            _log_send("tombstoned", th, req, started)
            return JSONResponse(
                {"error": "unregistered", "rateLimits": await _limits(th)},
                status_code=404,
            )

        if not await limiter.burst_allowed(th):
            metrics.count("rate_limited")
            return JSONResponse(
                {"error": "rate_limited", "rateLimits": await _limits(th)},
                status_code=429,
                headers={"Retry-After": "60"},
            )

        counts = await limiter.daily_counts(th)
        if counts.successful >= DAILY_QUOTA:
            metrics.count("rate_limited")
            return JSONResponse(
                {"error": "rate_limited", "rateLimits": _rate_limits_body(counts)},
                status_code=429,
                headers={"Retry-After": str(seconds_to_utc_midnight())},
            )

        if not await limiter.global_allowed():
            metrics.count("rate_limited")
            return JSONResponse(
                {"error": "rate_limited"}, status_code=503, headers={"Retry-After": "60"}
            )

        outcome = await fcm.send(
            req.push_token, req.data.frame_json(), req.priority, req.ttl, req.collapse_id
        )

        if outcome is SendOutcome.DELIVERED:
            await limiter.record_result(th, success=True)
            metrics.count("delivered")
            _log_send("delivered", th, req, started)
            return JSONResponse({"rateLimits": await _limits(th)})

        await limiter.record_result(th, success=False)
        if outcome is SendOutcome.UNREGISTERED:
            await limiter.tombstone(th)
            metrics.count("unregistered")
            _log_send("unregistered", th, req, started)
            return JSONResponse(
                {"error": "unregistered", "rateLimits": await _limits(th)},
                status_code=404,
            )
        metrics.count("upstream_error")
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
    async def metrics_endpoint() -> PlainTextResponse:
        return PlainTextResponse(metrics.render(), media_type="text/plain; version=0.0.4")

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
