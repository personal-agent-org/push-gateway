import json
import time
from datetime import UTC, datetime

import httpx
import respx
from conftest import FCM_URL, PUSH_TOKEN, TOKEN_URL, push_body

from push_gateway import __version__
from push_gateway.config import DAILY_QUOTA
from push_gateway.limits import token_hash, utc_day

TH = token_hash(PUSH_TOKEN)

UNREGISTERED_400 = {
    "error": {
        "code": 400,
        "status": "INVALID_ARGUMENT",
        "details": [
            {
                "@type": "type.googleapis.com/google.firebase.fcm.v1.FcmError",
                "errorCode": "UNREGISTERED",
            }
        ],
    }
}


def seed(store, key: str, value: int, ttl: float = 3600) -> None:
    store._data[key] = (value, time.monotonic() + ttl)


async def test_happy_path_wire_shape(client, fcm_router):
    route = fcm_router.post(FCM_URL).respond(200, json={"name": "projects/test-project/messages/1"})

    resp = await client.post(
        "/api/v1/push",
        json=push_body(
            ttl=60,
            collapse_id="chat_reply",
            data={
                "type": "chat_reply",
                "payload_id": "run:abc",
                "v": 2,
            },
        ),
    )

    assert resp.status_code == 200
    limits = resp.json()["rateLimits"]
    assert limits["successful"] == 1
    assert limits["errors"] == 0
    assert limits["maximum"] == DAILY_QUOTA
    assert limits["remaining"] == DAILY_QUOTA - 1
    resets = datetime.fromisoformat(limits["resetsAt"].replace("Z", "+00:00"))
    assert resets > datetime.now(UTC)
    assert (resets.hour, resets.minute, resets.second) == (0, 0, 0)

    assert route.call_count == 1
    request = route.calls.last.request
    assert request.headers["authorization"] == "Bearer at"
    sent = json.loads(request.content)
    frame_json = sent["message"]["data"]["frame"]
    assert isinstance(frame_json, str)
    assert json.loads(frame_json) == {"type": "chat_reply", "payload_id": "run:abc", "v": 2}
    assert sent == {
        "message": {
            "token": PUSH_TOKEN,
            "data": {"frame": frame_json},
            "android": {"priority": "high", "ttl": "60s", "collapse_key": "chat_reply"},
        }
    }


async def test_optional_android_fields_omitted(client, fcm_router):
    route = fcm_router.post(FCM_URL).respond(200, json={"name": "x"})
    resp = await client.post("/api/v1/push", json=push_body(priority="normal"))
    assert resp.status_code == 200
    sent = json.loads(route.calls.last.request.content)
    assert sent["message"]["android"] == {"priority": "normal"}


async def test_unregistered_400_detail_tombstones(client, fcm_router):
    route = fcm_router.post(FCM_URL).respond(400, json=UNREGISTERED_400)

    resp = await client.post("/api/v1/push", json=push_body())
    assert resp.status_code == 404
    assert resp.json()["error"] == "unregistered"
    assert resp.json()["rateLimits"]["errors"] == 1
    assert route.call_count == 1

    # Tombstoned: the second send 404s without touching FCM.
    resp2 = await client.post("/api/v1/push", json=push_body())
    assert resp2.status_code == 404
    assert resp2.json()["error"] == "unregistered"
    assert route.call_count == 1


async def test_unregistered_plain_404(client, fcm_router):
    fcm_router.post(FCM_URL).respond(404, json={"error": {"status": "NOT_FOUND"}})
    resp = await client.post("/api/v1/push", json=push_body())
    assert resp.status_code == 404
    assert resp.json()["error"] == "unregistered"


async def test_daily_quota_exhausted(client, store, fcm_router):
    route = fcm_router.post(FCM_URL).respond(200, json={"name": "x"})
    seed(store, f"day_ok:{TH}:{utc_day()}", DAILY_QUOTA)

    resp = await client.post("/api/v1/push", json=push_body())
    assert resp.status_code == 429
    retry_after = int(resp.headers["retry-after"])
    assert 0 < retry_after <= 86400
    limits = resp.json()["rateLimits"]
    assert limits["successful"] == DAILY_QUOTA
    assert limits["remaining"] == 0
    assert route.call_count == 0


async def test_burst_limit(client, store, fcm_router):
    route = fcm_router.post(FCM_URL).respond(200, json={"name": "x"})
    # Seed this minute and the next so a window flip mid-test cannot flake.
    minute = int(time.time() // 60)
    seed(store, f"burst:{TH}:{minute}", 30)
    seed(store, f"burst:{TH}:{minute + 1}", 30)

    resp = await client.post("/api/v1/push", json=push_body())
    assert resp.status_code == 429
    assert resp.headers["retry-after"] == "60"
    assert route.call_count == 0


async def test_oversized_body(client, fcm_router):
    body = push_body(app_id="x" * 200)
    raw = json.dumps(body).encode() + b" " * 5000
    resp = await client.post(
        "/api/v1/push", content=raw, headers={"content-type": "application/json"}
    )
    assert resp.status_code == 413


async def test_oversized_streamed_body_aborts_early(client, fcm_router):
    sent_chunks = 0

    async def gen():
        nonlocal sent_chunks
        for _ in range(64):
            sent_chunks += 1
            yield b"x" * 1024

    # No Content-Length (chunked): the cap must trip while streaming.
    resp = await client.post(
        "/api/v1/push", content=gen(), headers={"content-type": "application/json"}
    )
    assert resp.status_code == 413
    # Aborted right past the 4 KiB cap, not after buffering all 64 KiB.
    assert sent_chunks <= 6


def make_request(headers: dict[str, str]):
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        "client": ("9.9.9.9", 1234),
    }
    return Request(scope)


def test_client_ip_resolution_precedence():
    from push_gateway.main import _client_ip

    xff = {"x-forwarded-for": "1.1.1.1, 2.2.2.2, 3.3.3.3"}
    # Spoofable pre-appended values ignored: only the last (proxy-appended) hop counts.
    assert _client_ip(make_request(xff), True) == "3.3.3.3"
    assert _client_ip(make_request(xff), False) == "9.9.9.9"
    assert _client_ip(make_request({**xff, "x-real-ip": "4.4.4.4"}), True) == "4.4.4.4"
    cf = {**xff, "x-real-ip": "4.4.4.4", "cf-connecting-ip": "5.5.5.5"}
    assert _client_ip(make_request(cf), True) == "5.5.5.5"
    assert _client_ip(make_request({}), True) == "9.9.9.9"


async def test_trust_proxy_spoofed_xff(creds_file, fcm_router):
    import httpx

    from push_gateway.config import Settings
    from push_gateway.limits import MemoryStore
    from push_gateway.main import create_app

    store = MemoryStore()
    app = create_app(Settings(fcm_credentials_file=creds_file, trust_proxy=True), store=store)
    minute = int(time.time() // 60)
    for m in (minute, minute + 1):
        seed(store, f"ip:203.0.113.9:{m}", 600)

    fcm_router.post(FCM_URL).respond(200, json={"name": "x"})
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
        # Exhausted IP in the trusted LAST position: limited.
        resp = await client.post(
            "/api/v1/push",
            json=push_body(),
            headers={"x-forwarded-for": "6.6.6.6, 203.0.113.9"},
        )
        assert resp.status_code == 429

        # Attacker pre-appends the exhausted IP; the real last hop is clean: allowed.
        resp2 = await client.post(
            "/api/v1/push",
            json=push_body(),
            headers={"x-forwarded-for": "203.0.113.9, 198.51.100.7"},
        )
        assert resp2.status_code == 200


async def test_bad_schema(client, fcm_router):
    for bad in [
        push_body(platform="ios"),
        push_body(unexpected="nope"),
        push_body(data={"type": "Bad-Type!", "payload_id": "x"}),
        push_body(data={"type": "ok_type", "payload_id": "bad id with spaces"}),
        push_body(data={"type": "ok_type", "payload_id": "x", "nested": {"a": 1}}),
        push_body(push_token="short"),
        push_body(ttl=90000),
        {"v": 1},
    ]:
        resp = await client.post("/api/v1/push", json=bad)
        assert resp.status_code == 400, bad
        assert resp.json() == {"error": "invalid"}


async def test_data_too_large(client, fcm_router):
    data = {"type": "chat_reply", "payload_id": "run:abc", "blob": "y" * 1100}
    resp = await client.post("/api/v1/push", json=push_body(data=data))
    assert resp.status_code == 400


async def test_fcm_500_is_upstream_and_no_tombstone(client, fcm_router):
    route = fcm_router.post(FCM_URL).respond(500, text="boom")
    resp = await client.post("/api/v1/push", json=push_body())
    assert resp.status_code == 502
    assert resp.json() == {"error": "upstream"}

    # Token not tombstoned: the next send reaches FCM again and succeeds.
    route.respond(200, json={"name": "x"})
    resp2 = await client.post("/api/v1/push", json=push_body())
    assert resp2.status_code == 200
    assert route.call_count == 2
    limits = resp2.json()["rateLimits"]
    assert limits["successful"] == 1
    assert limits["errors"] == 1


async def test_unconfigured_returns_503(fcm_router):
    import httpx

    from push_gateway.config import Settings
    from push_gateway.main import create_app

    app = create_app(Settings())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
        resp = await client.post("/api/v1/push", json=push_body())
    assert resp.status_code == 503
    assert resp.json() == {"error": "not_configured"}


async def test_a_broken_store_refuses_the_push_and_does_not_relay(creds_file, fcm_router):
    """Fail CLOSED. The limiter is the only thing between one looping device and the shared
    FCM quota every self-hosted instance draws from, so a gateway that cannot evaluate its
    limits must not relay -- it used to let everything through with a log line."""
    from push_gateway.config import Settings
    from push_gateway.main import create_app

    class ExplodingStore:
        async def incr(self, key, ttl):
            raise RuntimeError("store down")

        async def get_int(self, key):
            raise RuntimeError("store down")

        async def set_flag(self, key, ttl):
            raise RuntimeError("store down")

        async def has_flag(self, key):
            raise RuntimeError("store down")

    route = fcm_router.post(FCM_URL).respond(200, json={"name": "x"})
    app = create_app(Settings(fcm_credentials_file=creds_file), store=ExplodingStore())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
        resp = await client.post("/api/v1/push", json=push_body())
    # 503 + Retry-After, never 404: 404 means "this token is dead" and makes the instance
    # throw a perfectly good push token away over what may be a momentary blip.
    assert resp.status_code == 503
    assert resp.json() == {"error": "unavailable"}
    assert resp.headers["Retry-After"] == "30"
    assert route.call_count == 0  # nothing reached Firebase


async def test_a_delivered_push_survives_a_store_that_dies_afterwards(creds_file, fcm_router):
    """The other half of the trade: the daily counter is written AFTER delivery, so a failure
    there must not turn a delivered push into an error the caller retries -- the retry would
    send the notification a second time."""
    from push_gateway.config import Settings
    from push_gateway.limits import MemoryStore
    from push_gateway.main import create_app

    class FailsOnlyOnWriteBack(MemoryStore):
        def __init__(self) -> None:
            super().__init__()
            self.relayed = False

        async def incr(self, key, ttl):
            if self.relayed and key.startswith(("day_ok", "day_err")):
                raise RuntimeError("store died mid-flight")
            return await super().incr(key, ttl)

    store = FailsOnlyOnWriteBack()
    route = fcm_router.post(FCM_URL).respond(200, json={"name": "x"})
    app = create_app(Settings(fcm_credentials_file=creds_file), store=store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
        store.relayed = True
        resp = await client.post("/api/v1/push", json=push_body())
    assert resp.status_code == 200
    assert route.call_count == 1


async def test_info_healthz_metrics(client, fcm_router):
    fcm_router.post(FCM_URL).respond(200, json={"name": "x"})
    await client.post("/api/v1/push", json=push_body())

    info = (await client.get("/api/v1/info")).json()
    assert info["name"] == "personal-agent-push-gateway"
    assert info["platforms"] == ["android"]
    assert info["max_body_bytes"] == 4096
    assert info["daily_quota"] == DAILY_QUOTA

    assert (await client.get("/healthz")).json() == {"ok": True}

    res = await client.get("/metrics")
    assert res.headers["content-type"].startswith("text/plain")
    metrics = res.text
    assert 'push_gateway_sends_total{outcome="delivered",priority="high"} 1.0' in metrics
    # Present at zero rather than absent: an alert on rate() cannot fire on a series that
    # does not exist yet, and "no data" reads the same as "no problem".
    assert 'push_gateway_sends_total{outcome="unregistered",priority="high"} 0.0' in metrics


# --- Prometheus instruments -------------------------------------------------------------
#
# The relay used to export one counter, sends_total{outcome}. It could not answer the two
# questions an operator has when something is wrong: why a request was refused (four causes
# all counted as "rate_limited"), and whether the limiter is still working at all (it fails
# OPEN on store errors and only logged a warning).


def _series(text: str, name: str) -> dict[str, float]:
    """The {labels: value} of one metric family, from the exposition text."""
    out: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.startswith(name):
            continue
        key, _, value = line.rpartition(" ")
        out[key[len(name) :].strip("{}")] = float(value)
    return out


async def test_every_refusal_reason_is_its_own_series(client, fcm_router):
    """The four causes behind the old "rate_limited" mean different things: a noisy instance,
    a looping device, a device out of daily budget, and the gateway itself saturated."""
    reasons = _series((await client.get("/metrics")).text, "push_gateway_rejected_total")
    assert {r.split('"')[1] for r in reasons} == {
        "ip_rate_limit",
        "burst_rate_limit",
        "daily_quota",
        "global_rate_limit",
        "tombstoned",
        "invalid",
        "too_large",
        "not_configured",
        "store_unavailable",
    }
    assert set(reasons.values()) == {0.0}


async def test_a_malformed_frame_counts_as_invalid_not_as_a_send(client, fcm_router):
    await client.post("/api/v1/push", json={"v": 1, "nope": True})
    text = (await client.get("/metrics")).text
    assert _series(text, "push_gateway_rejected_total")['reason="invalid"'] == 1.0
    assert sum(_series(text, "push_gateway_sends_total").values()) == 0.0


async def test_an_oversized_body_is_told_apart_from_a_malformed_one(client, fcm_router):
    await client.post(
        "/api/v1/push", json=push_body(data={"type": "x", "payload_id": "y", "pad": "p" * 5000})
    )
    reasons = _series((await client.get("/metrics")).text, "push_gateway_rejected_total")
    assert reasons['reason="too_large"'] + reasons['reason="invalid"'] == 1.0


async def test_a_gateway_without_credentials_is_visible(creds_file, store):
    """It refuses every push. The old metrics showed it as perfectly idle."""
    from push_gateway.config import Settings
    from push_gateway.main import create_app

    app = create_app(Settings(fcm_credentials_file=None), store=store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as c:
        assert (await c.post("/api/v1/push", json=push_body())).status_code == 503
        text = (await c.get("/metrics")).text
    assert _series(text, "push_gateway_rejected_total")['reason="not_configured"'] == 1.0
    assert _series(text, "push_gateway_upstream_configured")[""] == 0.0


async def test_a_configured_gateway_says_so(client):
    text = (await client.get("/metrics")).text
    assert _series(text, "push_gateway_upstream_configured")[""] == 1.0
    assert _series(text, "push_gateway_store_backend")['backend="memory"'] == 1.0
    assert _series(text, "push_gateway_build_info")[f'version="{__version__}"'] == 1.0


async def test_a_store_failure_is_countable(creds_file):
    """It refuses now, but the refusal still has to be visible as a STORE problem: a spike of
    503s with no store_failures series would send someone hunting Firebase instead."""
    from push_gateway.config import Settings
    from push_gateway.main import create_app

    class _BrokenStore:
        async def incr(self, key, ttl):
            raise RuntimeError("redis is gone")

        async def get_int(self, key):
            raise RuntimeError("redis is gone")

        async def set_flag(self, key, ttl):
            raise RuntimeError("redis is gone")

        async def has_flag(self, key):
            raise RuntimeError("redis is gone")

    app = create_app(Settings(fcm_credentials_file=creds_file), store=_BrokenStore())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as c:
        with respx.mock(assert_all_called=False) as router:
            router.route(host="gw").pass_through()
            router.post(TOKEN_URL).respond(200, json={"access_token": "at", "expires_in": 3600})
            router.post(FCM_URL).respond(200, json={"name": "x"})
            await c.post("/api/v1/push", json=push_body())
        text = (await c.get("/metrics")).text
    assert sum(_series(text, "push_gateway_store_failures_total").values()) > 0
    assert _series(text, "push_gateway_rejected_total")['reason="store_unavailable"'] == 1.0


async def test_the_upstream_call_is_timed(client, fcm_router):
    fcm_router.post(FCM_URL).respond(200, json={"name": "x"})
    await client.post("/api/v1/push", json=push_body())
    text = (await client.get("/metrics")).text
    counts = _series(text, "push_gateway_relay_duration_seconds_count")
    assert counts['outcome="delivered"'] == 1.0
    # Timed around the Firebase call alone, so a slow store cannot masquerade as a slow
    # upstream -- and vice versa.
    assert counts['outcome="upstream_error"'] == 0.0


async def test_a_dead_token_is_a_send_outcome_not_a_refusal(client, fcm_router):
    """UNREGISTERED means Firebase answered; it belongs with the sends, and it is tombstoned
    afterwards -- the SECOND attempt is the refusal."""
    fcm_router.post(FCM_URL).respond(404, json={"error": {"status": "NOT_FOUND"}})
    await client.post("/api/v1/push", json=push_body())
    await client.post("/api/v1/push", json=push_body())
    text = (await client.get("/metrics")).text
    assert (
        _series(text, "push_gateway_sends_total")['outcome="unregistered",priority="high"'] == 1.0
    )
    assert _series(text, "push_gateway_rejected_total")['reason="tombstoned"'] == 1.0


async def test_the_frame_type_is_never_a_label(client, fcm_router):
    """data.type matches ^[a-z_]{1,64}$ and comes from the calling instance: as a label it
    would be unbounded cardinality, which is how a Prometheus is killed."""
    fcm_router.post(FCM_URL).respond(200, json={"name": "x"})
    for t in ("chat_reply", "meeting_started", "some_other_thing"):
        await client.post("/api/v1/push", json=push_body(data={"type": t, "payload_id": "run:abc"}))
    text = (await client.get("/metrics")).text
    assert "some_other_thing" not in text
    assert "chat_reply" not in text


async def test_two_apps_do_not_share_counters(creds_file, store):
    """Instruments live on a registry per app, not the process-global default -- otherwise
    every test in this file would accumulate into the same numbers."""
    from push_gateway.config import Settings
    from push_gateway.main import create_app

    a = create_app(Settings(fcm_credentials_file=creds_file), store=store)
    b = create_app(Settings(fcm_credentials_file=creds_file), store=store)
    assert a is not b  # and neither raised "Duplicated timeseries in CollectorRegistry"
