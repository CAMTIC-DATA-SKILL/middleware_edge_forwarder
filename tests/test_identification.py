from __future__ import annotations

import socket
import unittest

from asyncua import Client, ua

from model.app_model import OpcUaServerSettings
from model.tag_model import CollectorHeader
from opcua_server.server import NorthboundServer, TagNodeSpec

AAS_ID = "urn:example:aas:hyundai:hh7:pickplace:01"
GLOBAL_ASSET_ID = "urn:example:asset:hyundai:hh7:pickplace:01"
DEVICE_KEY = "robot-1"
COLLECTOR_PATH = "Collectors/robot/robot-1"
NS_URI = "urn:test:forwarder:tags"


def _free_endpoint() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return f"opc.tcp://127.0.0.1:{sock.getsockname()[1]}/edge/"


def _cfg(endpoint: str) -> OpcUaServerSettings:
    return OpcUaServerSettings(
        endpoint=endpoint,
        server_name="test_forwarder",
        application_uri="urn:test:forwarder",
        namespace_uri=NS_URI,
        auto_create_nodes=False,
    )


def _header(**overrides: object) -> CollectorHeader:
    values: dict[str, object] = {
        "kind": "robot",
        "device_key": DEVICE_KEY,
        "aas_id": AAS_ID,
        "global_asset_id": GLOBAL_ASSET_ID,
    }
    values.update(overrides)
    return CollectorHeader.model_validate(values)


def _tag_spec(tag_id: str, name: str) -> TagNodeSpec:
    return TagNodeSpec(
        tag_id=tag_id,
        segments=tuple(COLLECTOR_PATH.split("/")),
        name=name,
        vtype=ua.VariantType.Boolean,
        source_id="robot/robot-1",
    )


class IdentificationNodeTest(unittest.IsolatedAsyncioTestCase):
    async def test_identification_nodes_match_tag_map(self) -> None:
        endpoint = _free_endpoint()
        server = NorthboundServer(_cfg(endpoint))
        await server.start()
        try:
            await server.add_tag(_tag_spec("robot1.power", "PowerYesno"))
            await server.add_tag(_tag_spec("robot1.ready", "Ready"))
            added = await server.add_identification(
                tuple(COLLECTOR_PATH.split("/")), _header().identification_fields()
            )
            self.assertTrue(added)
            self.assertFalse(
                await server.add_identification(
                    tuple(COLLECTOR_PATH.split("/")), _header().identification_fields()
                )
            )
            async with Client(url=endpoint) as client:
                ns = await client.get_namespace_index(NS_URI)
                aas = client.get_node(ua.NodeId(f"{COLLECTOR_PATH}/Identification/AasId", ns))
                self.assertEqual(await aas.read_value(), AAS_ID)
                self.assertEqual(
                    await client.get_node(ua.NodeId(f"{COLLECTOR_PATH}/Identification/GlobalAssetId", ns)).read_value(),
                    GLOBAL_ASSET_ID,
                )
                self.assertEqual(
                    await client.get_node(ua.NodeId(f"{COLLECTOR_PATH}/Identification/DeviceKey", ns)).read_value(),
                    DEVICE_KEY,
                )
                children = await client.get_node(ua.NodeId(f"{COLLECTOR_PATH}/Identification", ns)).get_children()
                names = [(await child.read_browse_name()).Name for child in children]
                self.assertEqual(names.count("AasId"), 1)
                with self.assertRaises(ua.UaError):
                    await aas.write_value("tampered")
                self.assertEqual(await aas.read_value(), AAS_ID)
        finally:
            await server.stop()

    async def test_no_identification_without_aas_ids(self) -> None:
        endpoint = _free_endpoint()
        server = NorthboundServer(_cfg(endpoint))
        await server.start()
        try:
            await server.add_tag(_tag_spec("robot1.power", "PowerYesno"))
            added = await server.add_identification(
                tuple(COLLECTOR_PATH.split("/")), _header(aas_id="", global_asset_id="").identification_fields()
            )
            self.assertFalse(added)
            async with Client(url=endpoint) as client:
                ns = await client.get_namespace_index(NS_URI)
                self.assertEqual(
                    (await client.get_node(ua.NodeId("robot1.power", ns)).read_browse_name()).Name, "PowerYesno"
                )
                with self.assertRaises(ua.UaError):
                    await client.get_node(ua.NodeId(f"{COLLECTOR_PATH}/Identification/AasId", ns)).read_browse_name()
        finally:
            await server.stop()

    async def test_legacy_tag_has_no_identification(self) -> None:
        endpoint = _free_endpoint()
        server = NorthboundServer(_cfg(endpoint))
        await server.start()
        try:
            await server.add_tag(_tag_spec("legacy.temp", "Temperature"))
            async with Client(url=endpoint) as client:
                ns = await client.get_namespace_index(NS_URI)
                self.assertEqual(
                    (await client.get_node(ua.NodeId("legacy.temp", ns)).read_browse_name()).Name, "Temperature"
                )
                with self.assertRaises(ua.UaError):
                    await client.get_node(ua.NodeId(f"{COLLECTOR_PATH}/Identification", ns)).read_browse_name()
        finally:
            await server.stop()


if __name__ == "__main__":
    unittest.main()
