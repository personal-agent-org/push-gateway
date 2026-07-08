"""Strict request schemas for the v1 push API."""

from __future__ import annotations

import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .config import MAX_DATA_BYTES

_TYPE_RE = re.compile(r"^[a-z_]{1,64}$")
_PAYLOAD_ID_RE = re.compile(r"^[A-Za-z0-9_:-]{1,128}$")


class PushData(BaseModel):
    """Wake frame: a type plus an opaque payload id. The gateway is type-agnostic,
    so extra keys are allowed, but values must be strings or integers."""

    model_config = ConfigDict(extra="allow")

    type: str
    payload_id: str
    v: int | None = None

    @field_validator("type")
    @classmethod
    def _validate_type(cls, value: str) -> str:
        if not _TYPE_RE.fullmatch(value):
            raise ValueError("type must match ^[a-z_]{1,64}$")
        return value

    @field_validator("payload_id")
    @classmethod
    def _validate_payload_id(cls, value: str) -> str:
        if not _PAYLOAD_ID_RE.fullmatch(value):
            raise ValueError("payload_id must match ^[A-Za-z0-9_:-]{1,128}$")
        return value

    @model_validator(mode="after")
    def _validate_extras_and_size(self) -> PushData:
        for key, value in (self.model_extra or {}).items():
            if type(value) not in (str, int):
                raise ValueError(f"data.{key} must be a string or an integer")
        if len(self.frame_json().encode("utf-8")) > MAX_DATA_BYTES:
            raise ValueError(f"data must serialize to at most {MAX_DATA_BYTES} bytes")
        return self

    def frame_json(self) -> str:
        return json.dumps(self.model_dump(exclude_none=True), separators=(",", ":"))


class PushRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    v: Literal[1]
    push_token: str = Field(min_length=8, max_length=4096)
    platform: Literal["android"]
    app_id: str | None = Field(default=None, max_length=255)
    priority: Literal["high", "normal"]
    ttl: int | None = Field(default=None, ge=0, le=86400)
    collapse_id: str | None = Field(default=None, max_length=64)
    data: PushData
