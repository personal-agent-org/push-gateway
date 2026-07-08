"""FCM HTTP v1 client: service-account OAuth minted with PyJWT, data-only wake frames.

No Google SDK: an RS256 assertion is posted to the service account's token_uri and
the access token is cached until shortly before expiry.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from enum import StrEnum
from pathlib import Path
from typing import Any

import httpx
import jwt

log = logging.getLogger("push_gateway.fcm")

_OAUTH_SCOPE = "https://www.googleapis.com/auth/firebase.messaging"


class SendOutcome(StrEnum):
    DELIVERED = "delivered"
    UNREGISTERED = "unregistered"
    UPSTREAM_ERROR = "upstream_error"


def load_credentials(path: str) -> dict[str, Any] | None:
    try:
        return json.loads(Path(path).read_text())
    except Exception as exc:
        log.warning("FCM credentials unreadable: %s", exc)
        return None


class FcmClient:
    def __init__(self, credentials: dict[str, Any]) -> None:
        self._creds = credentials
        self._project_id: str = credentials["project_id"]
        self._http = httpx.AsyncClient(timeout=10)
        self._token: str | None = None
        self._token_exp: float = 0.0
        self._token_lock = asyncio.Lock()

    async def send(
        self,
        push_token: str,
        frame_json: str,
        priority: str,
        ttl: int | None = None,
        collapse_key: str | None = None,
    ) -> SendOutcome:
        android: dict[str, Any] = {"priority": priority}
        if ttl is not None:
            android["ttl"] = f"{ttl}s"
        if collapse_key is not None:
            android["collapse_key"] = collapse_key
        message = {
            "message": {
                "token": push_token,
                # Data-only: the app renders the frame itself; a "notification"
                # payload would bypass its actions and deep links.
                "data": {"frame": frame_json},
                "android": android,
            }
        }
        url = f"https://fcm.googleapis.com/v1/projects/{self._project_id}/messages:send"
        try:
            resp = await self._http.post(
                url,
                json=message,
                headers={"Authorization": f"Bearer {await self._access_token()}"},
            )
        except Exception as exc:
            log.warning("FCM request failed: %s", type(exc).__name__)
            return SendOutcome.UPSTREAM_ERROR
        if resp.status_code < 300:
            return SendOutcome.DELIVERED
        if resp.status_code in (404, 410) or _is_unregistered(resp):
            return SendOutcome.UNREGISTERED
        log.warning("FCM send failed: status=%s body=%s", resp.status_code, resp.text[:200])
        return SendOutcome.UPSTREAM_ERROR

    async def _access_token(self) -> str:
        async with self._token_lock:
            if self._token and time.time() < self._token_exp - 300:
                return self._token
            now = int(time.time())
            assertion = jwt.encode(
                {
                    "iss": self._creds["client_email"],
                    "scope": _OAUTH_SCOPE,
                    "aud": self._creds["token_uri"],
                    "iat": now,
                    "exp": now + 3600,
                },
                self._creds["private_key"],
                algorithm="RS256",
            )
            resp = await self._http.post(
                self._creds["token_uri"],
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": assertion,
                },
            )
            resp.raise_for_status()
            payload = resp.json()
            self._token = payload["access_token"]
            self._token_exp = time.time() + int(payload.get("expires_in", 3600))
            return self._token

    async def aclose(self) -> None:
        await self._http.aclose()


def _is_unregistered(resp: httpx.Response) -> bool:
    if resp.status_code != 400:
        return False
    try:
        details = resp.json()["error"].get("details", [])
    except Exception:
        return False
    return any(d.get("errorCode") == "UNREGISTERED" for d in details)
