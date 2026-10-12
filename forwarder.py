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
from model.ipc_model import IpcMessage, source_id
from model.protocol_model import MsgDataDTO, MsgHealthDTO, MsgSampleDTO, MsgTypeEnum
from model.tag_model import TagConversion, TagSettings
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
        for tag in settings.tags:
            if tag.enabled:
                self._index.setdefault(tag.source.key, []).append(tag)
        self._sources: dict[str, _SourceState] = {}
        self._auto_skipped: set[str] = set()
        self.messages_received = 0
        self.messages_invalid = 0

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        await self.server.start()
        try:
            await self._register_mapped_tags()
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
                )
            )
        logger.info(
            "tag map 태그 %d개 등록 (전체 %d개, auto=%s)",
            len(enabled),
            len(self.settings.tags),
            self.settings.app.opcua.auto_create_nodes,
        )

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
                ipc_log = self.settings.app.ipc_log
                logger.info(
                    "IPC 메시지 로그: summary=%s file=%s",
                    ipc_log.summary,
                    f"{self.message_log.root}/{{device_key}}" if ipc_log.file_enable else "off",
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
                logger.warning("[%s] DATA %dms 동안 없음 → 태그 %d개 Uncertain", sid, stale_ms, n)

    async def _on_data(self, msg: IpcMessage) -> None:
        body = MsgDataDTO.model_validate(msg.header.msg_body)
        sid = source_id(msg.kind, body.device_key)
        state = await self._ensure_source(sid)
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

        state.last_data_mono = time.monotonic()
        if state.stale:
            state.stale = False
            await self.server.set_source_state(sid, stale=False)
            logger.info("[%s] DATA 수신 재개", sid)
        await self.server.set_source_state(sid, last_data=datetime.now(timezone.utc))

    async def _on_health(self, msg: IpcMessage) -> None:
        body = MsgHealthDTO.model_validate(msg.header.msg_body)
        sid = source_id(msg.kind, body.device_key)
        state = await self._ensure_source(sid)
        changed = state.device_ok is not body.device_ok
        state.device_ok = body.device_ok
        await self.server.set_source_state(sid, device_ok=body.device_ok, reason=body.reason or "")
        if body.device_ok:
            if changed:
                logger.info("[%s] HEALTH device_ok=true", sid)
            return
        n = await self.server.mark_source_tags(sid, _UNCERTAIN)
        logger.warning("[%s] HEALTH device_ok=false reason=%s → 태그 %d개 Uncertain", sid, body.reason, n)

    async def _ensure_source(self, sid: str) -> _SourceState:
        state = self._sources.get(sid)
        if state is None:
            state = self._sources[sid] = _SourceState()
            await self.server.ensure_source(sid)
        return state

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

    async def _write_sample(
        self,
        tag_id: str,
        sample: MsgSampleDTO,
        sample_ts: datetime,
        conversion: TagConversion,
    ) -> None:
        if sample.error:
            await self.server.update(tag_id, None, status=status_for_error(sample.error), source_timestamp=sample_ts)
            return
        if sample.value is None:
            return
        await self.server.update(tag_id, conversion.apply(sample.value), source_timestamp=sample_ts)


async def run(settings: Settings) -> None:
    """SIGTERM/SIGINT 로 정리 종료. Windows 는 KeyboardInterrupt 로 취소된다."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop.set)
    await Forwarder(settings).run(stop)
