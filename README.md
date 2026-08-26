# push-gateway

Central FCM push relay for [Personal Agent](https://github.com/personal-agent-org). Self-hosted
instances POST tiny wake frames here; the gateway relays them to Firebase Cloud Messaging using
one shared service account, so the official Android app gets push without every instance owning
Firebase credentials. Modeled on Home Assistant's mobile_app push gateway.

## How instances use it

```
POST /api/v1/push
{
  "v": 1,
  "push_token": "<FCM registration token from the app>",
  "platform": "android",
  "priority": "high",
  "ttl": 60,
  "collapse_id": "chat_reply",
  "data": {"type": "chat_reply", "payload_id": "run:0197..."}
}
```

The `data` object is the wake frame (a type plus an opaque id, values strings/int only, at most
1024 bytes serialized); the app fetches the real content from its own instance. Responses:

- `200` with a `rateLimits` object (500 successful sends per token per UTC day)
- `404 {"error": "unregistered"}`: the token is dead, delete it and re-register
- `429` with `Retry-After`: burst (30/min) or daily quota exhausted
- `502 {"error": "upstream"}`: FCM hiccup, transient, retry later
- `400`/`413`: invalid or oversized request, do not retry

## Running your own gateway

The public gateway only works with the official app build (push tokens are bound to the app's
Firebase project). To run your own you need your own Firebase project AND an app build using it:

1. Create a Firebase project, add the Android app, download a service account JSON.
2. `docker compose up` with `FCM_CREDENTIALS_FILE` pointing at that JSON (see `compose.yml`).
3. Point your instances at your gateway URL.

Configuration (env):

| Variable | Default | Purpose |
| --- | --- | --- |
| `FCM_CREDENTIALS_FILE` | unset | Path to the Firebase service account JSON. Without it the gateway starts but answers 503. |
| `REDIS_URL` | unset | Shared rate-limit store for multi-replica setups. Unset = in-memory (single process). |
| `TRUST_PROXY` | `false` | Resolve the client IP from proxy headers: `CF-Connecting-IP`, then `X-Real-IP`, then the last `X-Forwarded-For` hop (set behind a reverse proxy). |
| `GLOBAL_PER_MINUTE` | `50000` | Global send ceiling per minute; 503 when tripped. |
| `PORT` | `8080` | Listen port. |

## Operating it

`GET /metrics` is a Prometheus scrape endpoint. `GET /healthz` is liveness; `GET /api/v1/info`
reports version and limits.

| Metric | Type | Labels | Answers |
| --- | --- | --- | --- |
| `push_gateway_sends_total` | counter | `outcome`, `priority` | Did Firebase take it? `delivered`, `unregistered`, `upstream_error`. |
| `push_gateway_rejected_total` | counter | `reason` | Why a push never reached Firebase — each rate limit separately, plus `invalid`, `too_large`, `tombstoned`, `not_configured`, `store_unavailable`. |
| `push_gateway_relay_duration_seconds` | histogram | `outcome` | How slow Firebase is. Timed around that call alone, so a slow store cannot masquerade as a slow upstream. |
| `push_gateway_store_failures_total` | counter | `op` | The rate-limit store is unreachable. |
| `push_gateway_upstream_configured` | gauge | — | `0` means no credentials: every push is refused. |
| `push_gateway_store_backend` | gauge | `backend` | `memory` is per-process and does not survive a restart. |
| `push_gateway_build_info` | gauge | `version` | Which build is running. |

Plus the usual `process_*` and `python_*` series.

Every series exists from startup at zero, so `rate()` alerts work on a gateway that has not
sent anything yet — an absent series and a healthy one look identical otherwise.

The two worth alerting on:

```
# The limiter cannot reach its store, so pushes are being refused.
rate(push_gateway_store_failures_total[5m]) > 0

# Firebase is rejecting or failing.
rate(push_gateway_sends_total{outcome="upstream_error"}[5m]) > 0
```

The frame type (`data.type`) is deliberately **not** a label: it comes from the calling
instance and matches `^[a-z_]{1,64}$`, so it is unbounded cardinality.

Single process (see the `Dockerfile`). Running uvicorn with `--workers` would make each worker
export only its own numbers; that needs `prometheus_client`'s multiprocess mode.

## Rate limits fail closed

If the rate-limit store cannot be reached, the gateway refuses with `503` and `Retry-After`
rather than relaying. The limiter is the only thing between one looping device and the shared
FCM quota every self-hosted instance draws from.

Two deliberate exceptions: writes that happen *after* an outcome is decided (the daily counter,
the tombstone) are best-effort, because refusing there would turn a delivered push into an
error the caller retries — and the retry sends the notification twice. And a failed tombstone
lookup answers `503`, never `404`: `404` means "this token is dead" and would make the instance
throw away a perfectly good push token over a momentary blip.

## Privacy

The gateway sees the sender IP, the push token, the frame type, and an opaque payload id. It
stores nothing beyond TTL'd rate-limit counters (token hashes, never raw tokens; IPs live only
inside counters that expire within minutes). Message content never passes through it: frames are
wake-up signals, the app fetches content directly from its own instance.

## Development

```
uv sync
uv run ruff check .
uv run pytest -q
uv run uvicorn --factory push_gateway.main:create_app --reload
```
