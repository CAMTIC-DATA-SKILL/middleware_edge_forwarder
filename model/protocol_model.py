"""collector → 미들웨어 프로토콜 DTO (collector-plc 와이어와 동일, 수신 전용 부분만).

cmd_r / cmd_w / ack 는 이 프로세스가 다루지 않으므로 두지 않는다.
샘플 type/value 는 collector 종류(plc, robot …)마다 달라 문자열·넓은 유니온으로 받는다.
"""

from __future__ import annotations

from enum import StrEnum
from typing import List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, model_validator


class MsgTypeEnum(StrEnum):
    CMD_R = "cmd_r"
    CMD_W = "cmd_w"
    ACK = "ack"
    HEALTH = "health"
    DATA = "data"


class ProtocolHeaderDTO(BaseModel):
    msg_id: UUID
    gateway_address: str
    collector_address: str
    msg_type: MsgTypeEnum
    msg_body: dict
    timestamp_ms: int


def _alias_device_address(data: object) -> object:
    # 구버전 wire: device_address → device_key
    if isinstance(data, dict) and "device_key" not in data and "device_address" in data:
        data = dict(data)
        data["device_key"] = data.pop("device_address")
    return data


class MsgHealthDTO(BaseModel):
    device_key: str
    ipc_ok: bool
    device_ok: bool
    reason: Optional[str] = None
    observed_ms: int

    @model_validator(mode="before")
    @classmethod
    def _alias(cls, data: object) -> object:
        return _alias_device_address(data)


class MsgSampleDTO(BaseModel):
    # plc: D100 / D800.0 / D100~101, robot: 0.8~F 등 collector 가 파싱한 주소
    addr: str
    # plc: word|bit|int|long word|ascii, robot: u16|i16|f32 …
    type: Optional[str] = None
    value: Optional[Union[bool, int, float, str, list]] = None
    error: Optional[str] = None
    # robot collector 처럼 포인트 이름을 싣는 경우
    name: Optional[str] = None


class MsgDataDTO(BaseModel):
    device_key: str
    sample_ms: int
    samples: List[MsgSampleDTO]

    @model_validator(mode="before")
    @classmethod
    def _alias(cls, data: object) -> object:
        return _alias_device_address(data)
