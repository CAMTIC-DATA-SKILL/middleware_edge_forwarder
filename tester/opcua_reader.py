"""
북향 OPC UA 서버 주소 공간을 훑어 변수 값·품질을 출력한다.

사용 (프로젝트 루트에서):
    python tester/opcua_reader.py
    python tester/opcua_reader.py --endpoint opc.tcp://192.168.0.20:4840/edge/ --watch 2
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from asyncua import Client, Node, ua

NAMESPACE_URI = "urn:middleware:edge:forwarder:tags"


async def dump(node: Node, ns: int, depth: int = 0, max_depth: int = 8) -> None:
    if depth > max_depth:
        return
    for child in await node.get_children():
        if child.nodeid.NamespaceIndex != ns:
            continue
        name = (await child.read_browse_name()).Name
        node_class = await child.read_node_class()
        indent = "  " * depth
        if node_class == ua.NodeClass.Variable:
            dv = await child.read_data_value(raise_on_bad_status=False)
            print(f"{indent}{name} = {dv.Value.Value!r}  [{dv.StatusCode.name}]  src={dv.SourceTimestamp}")
        else:
            print(f"{indent}{name}/")
        await dump(child, ns, depth + 1, max_depth)


async def main() -> None:
    parser = argparse.ArgumentParser(description="northbound OPC UA reader")
    parser.add_argument("--endpoint", default="opc.tcp://127.0.0.1:4840/edge/")
    parser.add_argument("--namespace", default=NAMESPACE_URI)
    parser.add_argument("--watch", type=float, default=0, help="N초마다 반복 (0 = 1회)")
    args = parser.parse_args()

    async with Client(args.endpoint) as client:
        ns = await client.get_namespace_index(args.namespace)
        while True:
            print(f"--- {args.endpoint} (ns={ns})")
            await dump(client.nodes.objects, ns)
            if args.watch <= 0:
                return
            await asyncio.sleep(args.watch)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
