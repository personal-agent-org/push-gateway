import json

import httpx
import pytest
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from push_gateway.config import Settings
from push_gateway.limits import MemoryStore
from push_gateway.main import create_app

TOKEN_URL = "https://oauth2.googleapis.com/token"
FCM_URL = "https://fcm.googleapis.com/v1/projects/test-project/messages:send"
PUSH_TOKEN = "device-push-token-0123456789abcdef"


@pytest.fixture(scope="session")
def creds_file(tmp_path_factory):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    creds = {
        "project_id": "test-project",
        "client_email": "svc@test-project.iam.gserviceaccount.com",
        "private_key": pem,
        "token_uri": TOKEN_URL,
    }
    path = tmp_path_factory.mktemp("creds") / "service-account.json"
    path.write_text(json.dumps(creds))
    return str(path)


@pytest.fixture
def store():
    return MemoryStore()


@pytest.fixture
def app(creds_file, store):
    return create_app(Settings(fcm_credentials_file=creds_file), store=store)


@pytest.fixture
async def client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as c:
        yield c


@pytest.fixture
def fcm_router():
    with respx.mock(assert_all_called=False) as router:
        # Let requests to the ASGI test app through untouched.
        router.route(host="gw").pass_through()
        router.post(TOKEN_URL).respond(200, json={"access_token": "at", "expires_in": 3600})
        yield router


def push_body(**overrides):
    body = {
        "v": 1,
        "push_token": PUSH_TOKEN,
        "platform": "android",
        "priority": "high",
        "data": {"type": "chat_reply", "payload_id": "run:abc"},
    }
    body.update(overrides)
    return body
