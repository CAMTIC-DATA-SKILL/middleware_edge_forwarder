"""앱 설정 스키마 (ZeroMQ 수신, 북향 OPC UA 서버)."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field

from model.tag_model import CollectorHeader, TagSettings


class ZmqSettings(BaseModel):
    """collector PUB 을 구독하는 ZeroMQ SUB 설정."""

    # collector 는 PUB 을 bind 하므로 기본은 connect. collector 가 여럿이면 엔드포인트도 여럿
    sub_endpoints: list[str] = Field(default_factory=lambda: ["tcp://127.0.0.1:5556"])
    sub_bind: bool = False
    linger_ms: int = 0
    topic_root: str = "collector"
    # 비우면 collector.* 전체 구독
    collector_kinds: list[str] = Field(default_factory=lambda: ["plc"])
    # 비우면 kind 의 모든 device
    device_keys: list[str] = Field(default_factory=list)


class OpcUaServerSettings(BaseModel):
    """상위 시스템에 데이터를 제공하는 북향 OPC UA 서버 설정."""

    endpoint: str = "opc.tcp://0.0.0.0:4840/edge/"
    server_name: str = "middleware_edge_forwarder"
    application_uri: str = "urn:middleware:edge:forwarder"
    namespace_uri: str = "urn:middleware:edge:forwarder:tags"
    # tag map 에 없는 샘플도 {auto_root}/{kind}/{device_key}/{addr} 로 노출
    auto_create_nodes: bool = True
    auto_root: str = "Collectors"


class IpcLogSettings(BaseModel):
    """IPC 수신 메시지 로그: 콘솔 요약 + device_key 별 일별 파일 전문 ({프로젝트 루트}/data/logs/{device_key})."""

    summary: bool = True
    file_enable: bool = True
    # 파일명 날짜 기준. 오늘 포함 최근 N일만 남긴다 (0 = 삭제 안 함)
    retention_days: int = 7
    # 매일 이 시각(로컬, 0~23시)에 정리
    clean_hour: int = Field(default=3, ge=0, le=23)
    # 기동 후 device 폴더를 처음 기록할 때도 정리
    clean_on_start: bool = True


class AppSettings(BaseModel):
    log_level: str = "INFO"
    reconnect_period_ms: int = 5000
    # 이 시간 동안 DATA 가 없으면 수집원 태그를 UncertainLastUsableValue 로 표시 (0 = 끔)
    source_stale_ms: int = 10000
    zmq: ZmqSettings = Field(default_factory=ZmqSettings)
    opcua: OpcUaServerSettings = Field(default_factory=OpcUaServerSettings)
    ipc_log: IpcLogSettings = Field(default_factory=IpcLogSettings)


class Settings(BaseModel):
    app: AppSettings
    # 파일이 없으면 None (auto 노드만 사용)
    tag_map_file: Path | None = None
    tags: list[TagSettings] = Field(default_factory=list)
    collectors: list[CollectorHeader] = Field(default_factory=list)
