from __future__ import annotations

import unittest
from uuid import uuid4

from model.app_model import AppSettings, IpcLogSettings, OpcUaServerSettings, Settings
from model.ipc_model import IpcMessage
from model.protocol_model import MsgDataDTO, MsgTypeEnum, ProtocolHeaderDTO
from model.tag_model import TagSettings
from forwarder import Forwarder
from opcua_server.values import status_for_error
from utils.message_log import MessageLog


class _FakeServer:
    def __init__(self) -> None:
        self.updates: list[tuple[str, object, object]] = []

    def has_tag(self, tag_id: str) -> bool:
        return False

    async def add_tag(self, spec: object) -> bool:
        return False

    async def update(self, tag_id: str, value: object, status: object = None, source_timestamp: object = None) -> bool:
        self.updates.append((tag_id, value, status))
        return True

    async def ensure_source(self, sid: str) -> None:
        return None

    async def add_identification(self, segments: object, fields: object) -> bool:
        return False

    async def set_source_state(self, *args: object, **kwargs: object) -> None:
        return None


def _settings(*tags: dict) -> Settings:
    return Settings(
        app=AppSettings(
            opcua=OpcUaServerSettings(auto_create_nodes=False),
            ipc_log=IpcLogSettings(summary=False, file_enable=False),
        ),
        tags=[TagSettings.model_validate(tag) for tag in tags],
    )


def _data_msg(body: dict) -> IpcMessage:
    return IpcMessage(
        kind="robot",
        topic="collector.robot.robot-1.data",
        header=ProtocolHeaderDTO(
            msg_id=uuid4(),
            gateway_address="gw",
            collector_address="robot-1",
            msg_type=MsgTypeEnum.DATA,
            timestamp_ms=1,
            msg_body=body,
        ),
    )


class SocketMapTest(unittest.IsolatedAsyncioTestCase):
    def test_data_keeps_sockets(self) -> None:
        body = MsgDataDTO.model_validate(
            {
                "device_key": "robot-1",
                "sample_ms": 1,
                "samples": [{"addr": "S0004", "value": 1}],
                "sockets": [{"name": "hw", "value": {"cpu_temperature": 36.5}}],
            }
        )
        self.assertEqual(body.sockets[0].name, "hw")
        self.assertEqual(body.sockets[0].value, {"cpu_temperature": 36.5})

    async def test_socket_field_updates_mapped_tag(self) -> None:
        server = _FakeServer()
        fwd = Forwarder(
            _settings(
                {
                    "tag_id": "hw.cpu_temperature",
                    "source": {"kind": "robot", "device_key": "robot-1", "socket": "hw", "field": "cpu_temperature"},
                    "target": {"path": "OperationalData", "name": "CpuTemperature", "data_type": "Float"},
                }
            ),
            ipc=object(),  # type: ignore[arg-type]
            server=server,  # type: ignore[arg-type]
            message_log=MessageLog(IpcLogSettings(summary=False, file_enable=False)),
        )
        await fwd.handle(
            _data_msg(
                {
                    "device_key": "robot-1",
                    "sample_ms": 1_000,
                    "samples": [],
                    "sockets": [{"name": "hw", "value": {"cpu_temperature": 36.5, "mainboard_temperature": 41.2}}],
                }
            )
        )
        self.assertEqual(server.updates, [("hw.cpu_temperature", 36.5, None)])

    async def test_socket_error_marks_mapped_fields(self) -> None:
        server = _FakeServer()
        fwd = Forwarder(
            _settings(
                {
                    "tag_id": "hw.cpu_temperature",
                    "source": {"kind": "robot", "device_key": "robot-1", "socket": "hw", "field": "cpu_temperature"},
                    "target": {"path": "OperationalData", "name": "CpuTemperature", "data_type": "Float"},
                }
            ),
            ipc=object(),  # type: ignore[arg-type]
            server=server,  # type: ignore[arg-type]
            message_log=MessageLog(IpcLogSettings(summary=False, file_enable=False)),
        )
        await fwd.handle(
            _data_msg(
                {
                    "device_key": "robot-1",
                    "sample_ms": 1_000,
                    "samples": [],
                    "sockets": [{"name": "hw", "value": None, "error": "E-3101"}],
                }
            )
        )
        self.assertEqual(len(server.updates), 1)
        tag_id, value, status = server.updates[0]
        self.assertEqual((tag_id, value), ("hw.cpu_temperature", None))
        self.assertEqual(status, status_for_error("E-3101"))


if __name__ == "__main__":
    unittest.main()
