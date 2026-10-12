"""
ZeroMQ 토픽 조립·해석.

수신(SUB): collector.{kind}.{collector_address}.{msg_type}
  예) collector.plc.plc-mitsubishi-1.data
  kind 는 현재 plc, 이후 robot / cam 등으로 확장 예정.
  forwarder 는 data / health 만 쓴다 (ack 는 embedded_ingest 몫).
"""

from __future__ import annotations

from dataclasses import dataclass

from model.protocol_model import MsgTypeEnum

FORWARD_MSG_TYPES: tuple[MsgTypeEnum, ...] = (MsgTypeEnum.DATA, MsgTypeEnum.HEALTH)


@dataclass(frozen=True)
class CollectorTopic:
    kind: str
    collector_address: str
    msg_type: str


def join_topic(*parts: str) -> str:
    """빈 칸 세그먼트는 건너뛰고 '.' 로 잇는다."""
    return ".".join(p for p in parts if p)


def _leaf(msg_type: MsgTypeEnum | str) -> str:
    return msg_type.value if isinstance(msg_type, MsgTypeEnum) else str(msg_type)


def collector_topic(
    collector_address: str,
    msg_type: MsgTypeEnum | str,
    *,
    root: str = "collector",
    kind: str = "plc",
) -> str:
    """collector 측 토픽. collector.{kind}.{address}.{msg_type}"""
    return join_topic(root, kind, collector_address, _leaf(msg_type))


def sub_topics_for(
    *,
    root: str = "collector",
    kinds: list[str] | None = None,
    device_keys: list[str] | None = None,
) -> list[str]:
    """
    SUB 구독 prefix.
    - kinds 없음 → collector. (전체)
    - device_keys 없음 → collector.{kind}. (ack 는 수신 후 버림)
    - 둘 다 있음 → collector.{kind}.{device_key}.{data|health}
    """
    if not kinds:
        return [root + "."]
    if not device_keys:
        return [join_topic(root, k) + "." for k in kinds]
    return [
        collector_topic(d, mt, root=root, kind=k)
        for k in kinds
        for d in device_keys
        for mt in FORWARD_MSG_TYPES
    ]


def parse_collector_topic(topic: str, *, root: str = "collector") -> CollectorTopic | None:
    """collector.{kind}.{address}.{msg_type} 이 아니면 None."""
    parts = topic.split(".")
    if len(parts) != 4 or parts[0] != root or not all(parts):
        return None
    return CollectorTopic(kind=parts[1], collector_address=parts[2], msg_type=parts[3])
