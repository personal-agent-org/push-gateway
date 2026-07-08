import json
import time
from datetime import UTC, datetime

from conftest import FCM_URL, PUSH_TOKEN, push_body

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


async def test_fail_open_on_store_errors(creds_file, fcm_router):
    import httpx

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

    metrics = (await client.get("/metrics")).text
    assert 'sends_total{outcome="delivered"} 1' in metrics
    assert 'sends_total{outcome="unregistered"} 0' in metrics
