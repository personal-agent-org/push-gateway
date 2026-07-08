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
| `TRUST_PROXY` | `false` | Use the first `X-Forwarded-For` value as the client IP (set behind a reverse proxy). |
| `GLOBAL_PER_MINUTE` | `50000` | Global send ceiling per minute; 503 when tripped. |
| `PORT` | `8080` | Listen port. |

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
