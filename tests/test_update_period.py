from __future__ import annotations

import unittest
from uuid import uuid4

from model.app_model import AppSettings, IpcLogSettings, OpcUaServerSettings, Settings
from model.ipc_model import IpcMessage
from model.protocol_model import MsgTypeEnum, ProtocolHeaderDTO
from model.tag_model import TagSettings
from forwarder import Forwarder
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


def _settings(*, period_ms: int) -> Settings:
    return Settings(
        app=AppSettings(
            opcua=OpcUaServerSettings(auto_create_nodes=False, update_period_ms=period_ms),
            ipc_log=IpcLogSettings(summary=False, file_enable=False),
        ),
        tags=[
            TagSettings.model_validate(
                {
                    "tag_id": "robot1.power",
                    "source": {"kind": "robot", "device_key": "robot-1", "addr": "S0004"},
                    "target": {"path": "OperationalData", "name": "PowerYesno", "data_type": "Boolean"},
                }
            )
        ],
    )


def _data_msg(value: object) -> IpcMessage:
    return IpcMessage(
        kind="robot",
        topic="collector.robot.robot-1.data",
        header=ProtocolHeaderDTO(
            msg_id=uuid4(),
            gateway_address="gw",
            collector_address="robot-1",
            msg_type=MsgTypeEnum.DATA,
            timestamp_ms=1,
            msg_body={
                "device_key": "robot-1",
                "sample_ms": 1_000,
                "samples": [{"addr": "S0004", "value": value}],
            },
        ),
    )


def _forwarder(period_ms: int, server: _FakeServer) -> Forwarder:
    return Forwarder(
        _settings(period_ms=period_ms),
        ipc=object(),  # type: ignore[arg-type]
        server=server,  # type: ignore[arg-type]
        message_log=MessageLog(IpcLogSettings(summary=False, file_enable=False)),
    )


class UpdatePeriodTest(unittest.IsolatedAsyncioTestCase):
    async def test_zero_writes_immediately(self) -> None:
        server = _FakeServer()
        fwd = _forwarder(0, server)
        await fwd.handle(_data_msg(True))
        self.assertEqual(server.updates, [("robot1.power", True, None)])

    async def test_period_holds_until_flush(self) -> None:
        server = _FakeServer()
        fwd = _forwarder(1000, server)
        await fwd.handle(_data_msg(True))
        self.assertEqual(server.updates, [])
        await fwd._flush_pending()
        self.assertEqual(server.updates, [("robot1.power", True, None)])

    async def test_period_keeps_latest_value(self) -> None:
        server = _FakeServer()
        fwd = _forwarder(1000, server)
        await fwd.handle(_data_msg(False))
        await fwd.handle(_data_msg(True))
        await fwd._flush_pending()
        self.assertEqual(server.updates, [("robot1.power", True, None)])


if __name__ == "__main__":
    unittest.main()
