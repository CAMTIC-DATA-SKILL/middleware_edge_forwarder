"""
앱 설정은 .env, 북향 태그 매핑은 tag map JSON 에서 읽어 Settings 로 노출한다.

사용 예:
    from config import settings
    settings.app.zmq.sub_endpoints
    for tag in settings.tags:
        print(tag.tag_id, tag.source.key)
    for header in settings.collectors:
        print(header.kind, header.device_key, header.aas_id)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic import ValidationError

from model.app_model import (
    AppSettings,
    IpcLogSettings,
    OpcUaServerSettings,
    Settings,
    ZmqSettings,
)
from model.tag_model import TagMapFile

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def _env_str(key: str, default: str) -> str:
    value = os.getenv(key, default)
    return value.strip() if value is not None else default


def _env_int(key: str, default: int) -> int:
    return int(_env_str(key, str(default)))


def _env_bool(key: str, default: bool) -> bool:
    raw = _env_str(key, "true" if default else "false").lower()
    return raw in ("1", "true", "yes", "on")


def _env_list(key: str, default: str = "") -> list[str]:
    return [p.strip() for p in _env_str(key, default).split(",") if p.strip()]


def _env_path(key: str, default: str) -> Path:
    path = Path(_env_str(key, default))
    if not path.is_absolute():
        path = BASE_DIR / path
    return path


def _tag_map_path() -> Path:
    return _env_path("TAG_MAP_FILE", "tag_map.json")


def _load_tag_map(path: Path) -> TagMapFile:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"tag map JSON 파싱 실패: {path}: {e}") from e
    try:
        return TagMapFile.model_validate(data)
    except ValidationError as e:
        raise ValueError(f"tag map 검증 실패: {path}: {e}") from e


def _load_app_from_env() -> AppSettings:
    return AppSettings(
        log_level=_env_str("LOG_LEVEL", "INFO").upper(),
        reconnect_period_ms=_env_int("RECONNECT_PERIOD_MS", 5000),
        source_stale_ms=_env_int("SOURCE_STALE_MS", 10000),
        zmq=ZmqSettings(
            sub_endpoints=_env_list("ZMQ_SUB_ENDPOINTS", "tcp://127.0.0.1:5556"),
            sub_bind=_env_bool("ZMQ_SUB_BIND", False),
            linger_ms=_env_int("ZMQ_LINGER_MS", 0),
            topic_root=_env_str("ZMQ_TOPIC_ROOT", "collector"),
            collector_kinds=_env_list("ZMQ_COLLECTOR_KINDS", "plc"),
            device_keys=_env_list("ZMQ_DEVICE_KEYS"),
        ),
        opcua=OpcUaServerSettings(
            endpoint=_env_str("OPCUA_ENDPOINT", "opc.tcp://0.0.0.0:4840/edge/"),
            server_name=_env_str("OPCUA_SERVER_NAME", "middleware_edge_forwarder"),
            application_uri=_env_str(
                "OPCUA_APPLICATION_URI", "urn:middleware:edge:forwarder"
            ),
            namespace_uri=_env_str(
                "OPCUA_NAMESPACE_URI", "urn:middleware:edge:forwarder:tags"
            ),
            auto_create_nodes=_env_bool("OPCUA_AUTO_CREATE_NODES", True),
            auto_root=_env_str("OPCUA_AUTO_ROOT", "Collectors"),
            update_period_ms=max(0, _env_int("OPCUA_UPDATE_PERIOD_MS", 0)),
        ),
        ipc_log=IpcLogSettings(
            summary=_env_bool("IPC_LOG_SUMMARY", True),
            file_enable=_env_bool("IPC_LOG_FILE", True),
            retention_days=_env_int("IPC_LOG_RETENTION_DAYS", 7),
            clean_hour=_env_int("IPC_LOG_CLEAN_HOUR", 3),
            clean_on_start=_env_bool("IPC_LOG_CLEAN_ON_START", True),
        ),
    )


def load_settings() -> Settings:
    path = _tag_map_path()
    if not path.is_file():
        return Settings(app=_load_app_from_env())
    tag_map = _load_tag_map(path)
    return Settings(
        app=_load_app_from_env(),
        tag_map_file=path,
        tags=tag_map.tags,
        collectors=tag_map.collectors,
    )


settings = load_settings()
