"""Environment configuration and protocol constants."""

from __future__ import annotations

import os
from dataclasses import dataclass

MAX_BODY_BYTES = 4096
MAX_DATA_BYTES = 1024
DAILY_QUOTA = 500
BURST_PER_MINUTE = 30
IP_PER_MINUTE = 600
TOMBSTONE_TTL_SECONDS = 7 * 24 * 3600


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(slots=True)
class Settings:
    fcm_credentials_file: str = ""
    redis_url: str = ""
    trust_proxy: bool = False
    global_per_minute: int = 50000
    port: int = 8080

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            fcm_credentials_file=os.environ.get("FCM_CREDENTIALS_FILE", ""),
            redis_url=os.environ.get("REDIS_URL", ""),
            trust_proxy=_env_bool("TRUST_PROXY"),
            global_per_minute=int(os.environ.get("GLOBAL_PER_MINUTE", "50000")),
            port=int(os.environ.get("PORT", "8080")),
        )
