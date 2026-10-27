"""
IPC(ZeroMQ) 수신 → 북향 OPC UA 반영 루프.

collector DATA 샘플을 tag map(또는 auto 노드)에 따라 북향 노드 값으로 쓰고,
HEALTH·수신 공백을 수집원 상태와 태그 품질에 반영한다.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from asyncua import ua
from pydantic import ValidationError

from client.errors import ClientError
from model.app_model import Settings
from model.ipc_model import IpcMessage, collector_ref, source_id
from model.protocol_model import MsgDataDTO, MsgHealthDTO, MsgSampleDTO, MsgSocketDTO, MsgTypeEnum
from model.tag_model import CollectorHeader, TagConversion, TagSettings, identification_placements
from opcua_server.server import NorthboundServer, TagNodeSpec
from opcua_server.values import infer_variant_type, status_for_error
from utils.message_log import MessageLog
from zeromq_client.sub_client import ZmqSubClient
from zeromq_client.topics import sub_topics_for

logger = logging.getLogger(__name__)

_UNCERTAIN = ua.StatusCode(ua.StatusCodes.UncertainLastUsableValue)
_NO_CONVERSION = TagConversion()


@dataclass
class _SourceState:
    last_data_mono: float | None = None
    stale: bool = False
    device_ok: bool | None = None


class Forwarder:
    def __init__(
        self,
        settings: Settings,
        *,
        ipc: ZmqSubClient | None = None,
        server: NorthboundServer | None = None,
        message_log: MessageLog | None = None,
    ) -> None:
        self.settings = settings
        app = settings.app
        self.ipc = ipc or ZmqSubClient(
            app.zmq.sub_endpoints,
            bind=app.zmq.sub_bind,
            linger_ms=app.zmq.linger_ms,
            topics=sub_topics_for(
                root=app.zmq.topic_root,
                kinds=app.zmq.collector_kinds,
                device_keys=app.zmq.device_keys,
            ),
            topic_root=app.zmq.topic_root,
        )
        self.server = server or NorthboundServer(app.opcua)
        self.message_log = message_log or MessageLog(app.ipc_log)
        self._index: dict[tuple[str, str, str], list[TagSettings]] = {}
        self._socket_index: dict[tuple[str, str, str, str], list[TagSettings]] = {}
        for tag in settings.tags:
            if not tag.enabled:
                continue
            if tag.source.is_socket:
                self._socket_index.setdefault(tag.source.socket_key, []).append(tag)
            else:
                self._index.setdefault(tag.source.key, []).append(tag)
        self._headers: dict[tuple[str, str], CollectorHeader] = {
            header.key: header for header in settings.collectors
        }
        self._sources: dict[str, _SourceState] = {}
        self._auto_skipped: set[str] = set()
        self.messages_received = 0
        self.messages_invalid = 0

    def _collector_ref(self, kind: str, device_key: str) -> str:
        return collector_ref(kind, device_key, root=self.settings.app.zmq.topic_root)

    def _collector_ref_sid(self, sid: str) -> str:
        kind, _, device_key = sid.partition("/")
        return self._collector_ref(kind, device_key) if device_key else sid

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        await self.server.start()
        try:
            await self._register_mapped_tags()
            self.message_log.check_writable()
            if not await self._connect_ipc(stop):
                return
            tasks = [
                asyncio.create_task(self._recv_loop(), name="ipc-recv"),
                asyncio.create_task(self._watch_loop(), name="source-watch"),
            ]
            try:
                await stop.wait()
            finally:
                for task in tasks:
                    task.cancel()
                for task in tasks:
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
        finally:
            self.ipc.close()
            self.message_log.close()
            await self.server.stop()

    async def handle(self, msg: IpcMessage) -> None:
        """메시지 1건 처리. ack / cmd 는 무시한다."""
        self.messages_received += 1
        self.message_log.record(msg)
        try:
            if msg.header.msg_type is MsgTypeEnum.DATA:
                await self._on_data(msg)
            elif msg.header.msg_type is MsgTypeEnum.HEALTH:
                await self._on_health(msg)
        except ValidationError as e:
            self.messages_invalid += 1
            logger.warning("[%s] %s body 파싱 실패: %s", msg.topic, msg.header.msg_type.value, e)

    async def _register_mapped_tags(self) -> None:
        if self.settings.tag_map_file is None:
            logger.warning("tag map 파일이 없어 auto 노드만 사용합니다 (TAG_MAP_FILE 확인)")
        enabled = [t for t in self.settings.tags if t.enabled]
        for tag in enabled:
            sid = source_id(tag.source.kind, tag.source.device_key)
            await self._ensure_source(sid)
            await self.server.add_tag(
                TagNodeSpec(
                    tag_id=tag.tag_id,
                    segments=tag.target.segments,
                    name=tag.target.name,
                    vtype=ua.VariantType[tag.target.data_type],
                    source_id=sid,
                    description=tag.target.description,
                    unit=tag.target.unit,
                    node_id=tag.node_id,
                )
            )
        await self._register_identifications()
        logger.info(
            "tag map 태그 %d개 등록 (전체 %d개, auto=%s)",
            len(enabled),
            len(self.settings.tags),
            self.settings.app.opcua.auto_create_nodes,
        )

    async def _register_identifications(self) -> None:
        count = 0
        for segments, header in identification_placements(
            self.settings.collectors, self.settings.app.opcua.auto_root
        ):
            if await self.server.add_identification(segments, header.identification_fields()):
                count += 1
        if count:
            logger.info("Identification %d개 등록 (collector %d개)", count, len(self.settings.collectors))

    async def _connect_ipc(self, stop: asyncio.Event) -> bool:
        """연결될 때까지 재시도. stop 되면 False."""
        delay_s = max(0.1, self.settings.app.reconnect_period_ms / 1000.0)
        while not stop.is_set():
            try:
                self.ipc.connect()
                logger.info(
                    "ZeroMQ SUB ready %s topics=%s",
                    self.ipc._target(),
                    self.ipc.topics,
                )
                return True
            except ClientError as e:
                logger.error("ZeroMQ connect failed: %s(%s) %s", e.code.value, e.code.label, e.detail or e)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay_s)
        return False

    async def _recv_loop(self) -> None:
        while True:
            try:
                msg = await self.ipc.recv()
            except ClientError as e:
                self.messages_invalid += 1
                logger.warning("IPC 수신 오류: %s(%s) %s", e.code.value, e.code.label, e.detail or e)
                continue
            try:
                await self.handle(msg)
            except Exception:
                logger.exception("[%s] 메시지 처리 실패", msg.topic)

    async def _watch_loop(self) -> None:
        """수신 공백 감시 + 카운터 갱신 + 메시지 로그 정리 (1초 주기)."""
        stale_ms = self.settings.app.source_stale_ms
        while True:
            self.message_log.maybe_cleanup()
            await asyncio.sleep(1.0)
            await self.server.set_counters(received=self.messages_received, invalid=self.messages_invalid)
            if stale_ms <= 0:
                continue
            now = time.monotonic()
            for sid, state in self._sources.items():
                if state.stale or state.last_data_mono is None:
                    continue
                if (now - state.last_data_mono) * 1000 < stale_ms:
                    continue
                state.stale = True
                await self.server.set_source_state(sid, stale=True)
                n = await self.server.mark_source_tags(sid, _UNCERTAIN)
                logger.warning("[%s] DATA %dms 동안 없음 → 태그 %d개 Uncertain", self._collector_ref_sid(sid), stale_ms, n)

    async def _on_data(self, msg: IpcMessage) -> None:
        body = MsgDataDTO.model_validate(msg.header.msg_body)
        sid = source_id(msg.kind, body.device_key)
        state = await self._ensure_source(sid)
        await self._ensure_collector_identification(msg.kind, body.device_key)
        sample_ts = datetime.fromtimestamp(body.sample_ms / 1000.0, tz=timezone.utc)

        for sample in body.samples:
            mapped = self._index.get((msg.kind, body.device_key, sample.addr))
            if mapped:
                for tag in mapped:
                    await self._write_sample(tag.tag_id, sample, sample_ts, tag.conversion)
                continue
            tag_id = await self._ensure_auto_tag(msg.kind, body.device_key, sample)
            if tag_id is not None:
                await self._write_sample(tag_id, sample, sample_ts, _NO_CONVERSION)

        for sock in body.sockets:
            await self._on_socket(msg.kind, body.device_key, sock, sample_ts)

        state.last_data_mono = time.monotonic()
        if state.stale:
            state.stale = False
            await self.server.set_source_state(sid, stale=False)
            logger.info("[%s] DATA 수신 재개", self._collector_ref(msg.kind, body.device_key))
        await self.server.set_source_state(sid, last_data=datetime.now(timezone.utc))

    async def _on_health(self, msg: IpcMessage) -> None:
        body = MsgHealthDTO.model_validate(msg.header.msg_body)
        sid = source_id(msg.kind, body.device_key)
        state = await self._ensure_source(sid)
        await self._ensure_collector_identification(msg.kind, body.device_key)
        changed = state.device_ok is not body.device_ok
        state.device_ok = body.device_ok
        await self.server.set_source_state(sid, device_ok=body.device_ok, reason=body.reason or "")
        if body.device_ok:
            if changed:
                logger.info("[%s] HEALTH device_ok=true", self._collector_ref(msg.kind, body.device_key))
            return
        n = await self.server.mark_source_tags(sid, _UNCERTAIN)
        logger.warning(
            "[%s] HEALTH device_ok=false reason=%s → 태그 %d개 Uncertain",
            self._collector_ref(msg.kind, body.device_key),
            body.reason,
            n,
        )

    async def _ensure_collector_identification(self, kind: str, device_key: str) -> None:
        """수신한 collector id 로 헤더를 찾아 Identification 을 붙인다."""
        header = self._headers.get((kind, device_key))
        if header is None or not header.has_identification:
            return
        await self.server.add_identification(
            header.object_segments(self.settings.app.opcua.auto_root),
            header.identification_fields(),
        )

    async def _ensure_source(self, sid: str) -> _SourceState:
        state = self._sources.get(sid)
        if state is None:
            state = self._sources[sid] = _SourceState()
            await self.server.ensure_source(sid)
        return state

    async def _on_socket(self, kind: str, device_key: str, sock: MsgSocketDTO, sample_ts: datetime) -> None:
        if sock.error:
            for tag in self._mapped_socket_tags(kind, device_key, sock.name):
                await self._write_value(tag.tag_id, None, sample_ts, tag.conversion, error=sock.error)
            return
        if sock.value is None:
            return
        for field, value in sock.value.items():
            mapped = self._socket_index.get((kind, device_key, sock.name, field))
            if mapped:
                for tag in mapped:
                    await self._write_value(tag.tag_id, value, sample_ts, tag.conversion)
                continue
            tag_id = await self._ensure_auto_socket_tag(kind, device_key, sock, field, value)
            if tag_id is not None:
                await self._write_value(tag_id, value, sample_ts, _NO_CONVERSION)

    def _mapped_socket_tags(self, kind: str, device_key: str, name: str) -> list[TagSettings]:
        tags: list[TagSettings] = []
        for (k, device, socket_name, _), mapped in self._socket_index.items():
            if (k, device, socket_name) == (kind, device_key, name):
                tags.extend(mapped)
        return tags

    async def _ensure_auto_tag(self, kind: str, device_key: str, sample: MsgSampleDTO) -> str | None:
        opcua = self.settings.app.opcua
        if not opcua.auto_create_nodes:
            return None
        tag_id = f"{kind}.{device_key}.{sample.addr}"
        if self.server.has_tag(tag_id):
            return tag_id
        if tag_id in self._auto_skipped:
            return None
        vtype = infer_variant_type(sample.type, sample.value)
        if vtype is None:
            # 값이 아직 없거나(error) 배열 등 지원하지 않는 형식. 값이 없으면 다음 샘플에서 다시 본다
            if sample.value is not None:
                self._auto_skipped.add(tag_id)
                logger.warning("auto 노드 형식을 정할 수 없어 건너뜀: %s type=%s value=%r", tag_id, sample.type, sample.value)
            return None
        added = await self.server.add_tag(
            TagNodeSpec(
                tag_id=tag_id,
                segments=(opcua.auto_root, kind, device_key),
                name=sample.addr,
                vtype=vtype,
                source_id=source_id(kind, device_key),
                description=sample.name or "",
            )
        )
        if added:
            logger.info("auto 노드 생성: %s (%s)", tag_id, vtype.name)
        return tag_id

    async def _ensure_auto_socket_tag(
        self,
        kind: str,
        device_key: str,
        sock: MsgSocketDTO,
        field: str,
        value: object,
    ) -> str | None:
        opcua = self.settings.app.opcua
        if not opcua.auto_create_nodes:
            return None
        tag_id = f"{kind}.{device_key}.{sock.name}.{field}"
        if self.server.has_tag(tag_id):
            return tag_id
        if tag_id in self._auto_skipped:
            return None
        vtype = infer_variant_type(None, value)
        if vtype is None:
            if value is not None:
                self._auto_skipped.add(tag_id)
                logger.warning("auto 노드 형식을 정할 수 없어 건너뜀: %s value=%r", tag_id, value)
            return None
        added = await self.server.add_tag(
            TagNodeSpec(
                tag_id=tag_id,
                segments=(opcua.auto_root, kind, device_key, sock.name),
                name=field,
                vtype=vtype,
                source_id=source_id(kind, device_key),
                description=f"{sock.command_name or sock.name} {field}".strip(),
            )
        )
        if added:
            logger.info("auto 노드 생성: %s (%s)", tag_id, vtype.name)
        return tag_id

    async def _write_sample(
        self,
        tag_id: str,
        sample: MsgSampleDTO,
        sample_ts: datetime,
        conversion: TagConversion,
    ) -> None:
        await self._write_value(tag_id, sample.value, sample_ts, conversion, error=sample.error)

    async def _write_value(
        self,
        tag_id: str,
        value: object,
        sample_ts: datetime,
        conversion: TagConversion,
        error: str | None = None,
    ) -> None:
        if error:
            await self.server.update(tag_id, None, status=status_for_error(error), source_timestamp=sample_ts)
            return
        if value is None:
            return
        await self.server.update(tag_id, conversion.apply(value), source_timestamp=sample_ts)


async def run(settings: Settings) -> None:
    """SIGTERM/SIGINT 로 정리 종료. Windows 는 KeyboardInterrupt 로 취소된다."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop.set)
    await Forwarder(settings).run(stop)
