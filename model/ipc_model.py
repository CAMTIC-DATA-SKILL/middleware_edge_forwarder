"""IPC 수신 메시지. 전송 방식(ZeroMQ 등)과 무관하게 forwarder 가 쓰는 단위."""

from __future__ import annotations

from dataclasses import dataclass

from model.protocol_model import ProtocolHeaderDTO


@dataclass(frozen=True)
class IpcMessage:
    # collector 종류 (plc, robot …). ZeroMQ 는 토픽 collector.{kind}.… 에서 꺼낸다
    kind: str
    header: ProtocolHeaderDTO
    topic: str = ""
    # 토픽에서 온 collector_address (= collector 의 device_key). 없으면 body/header 값 사용
    address: str = ""

    @property
    def device_key(self) -> str:
        body = self.header.msg_body if isinstance(self.header.msg_body, dict) else {}
        raw = body.get("device_key") or body.get("device_address")
        if isinstance(raw, str) and raw:
            return raw
        return self.header.collector_address

    @property
    def source_id(self) -> str:
        return source_id(self.kind, self.device_key)

    @property
    def collector_ref(self) -> str:
        """로그용 토픽 접두사. 예: collector.robot.robot-1"""
        if self.topic:
            parts = [p for p in self.topic.split(".") if p]
            if len(parts) >= 3:
                return ".".join(parts[:3])
        return collector_ref(self.kind, self.device_key)


def source_id(kind: str, device_key: str) -> str:
    """수집원 식별자. 북향 상태 노드 경로(_Forwarder/Sources/{kind}/{device_key})로도 쓴다."""
    return f"{kind}/{device_key}"


def collector_ref(kind: str, device_key: str, *, root: str = "collector") -> str:
    """ZeroMQ 토픽과 같은 collector.{kind}.{device_key}."""
    return f"{root}.{kind}.{device_key}"
