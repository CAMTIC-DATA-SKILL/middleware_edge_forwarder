"""collector DATA/HEALTH 를 받는 SUB 클라이언트 (asyncio)."""

from __future__ import annotations

import logging

import zmq
import zmq.asyncio
from pydantic import ValidationError

from client.errors import ClientError, to_client_error
from model.error_model import ClientErrorCode
from model.ipc_model import IpcMessage
from model.protocol_model import ProtocolHeaderDTO
from zeromq_client.base_client import BaseZmqClient
from zeromq_client.topics import parse_collector_topic

logger = logging.getLogger(__name__)


class ZmqSubClient(BaseZmqClient):
    """[topic, json] multipart SUB. recv 는 메시지가 올 때까지 대기한다 (취소 가능)."""

    socket_type = zmq.SUB

    def __init__(
        self,
        endpoints: list[str],
        *,
        bind: bool = False,
        linger_ms: int = 0,
        topics: list[str] | None = None,
        topic_root: str = "collector",
    ):
        super().__init__(endpoints, bind=bind, linger_ms=linger_ms)
        # topics=[] / None → 전부 구독("")
        self.topics = topics or [""]
        self.topic_root = topic_root

    def _configure_socket(self, sock: zmq.asyncio.Socket) -> None:
        for topic in self.topics:
            sock.setsockopt_string(zmq.SUBSCRIBE, topic)

    async def recv(self) -> IpcMessage:
        """한 건 수신. 프로토콜 오류는 ClientError(PROTOCOL_ERROR)."""
        try:
            frames = await self.sock.recv_multipart()
        except ClientError:
            raise
        except zmq.ZMQError as e:
            raise to_client_error(e, default=ClientErrorCode.READ_FAILED) from e

        if len(frames) < 2:
            raise ClientError(
                ClientErrorCode.PROTOCOL_ERROR,
                f"multipart 프레임 부족: {len(frames)}",
            )

        topic = frames[0].decode("utf-8", errors="replace")
        parsed = parse_collector_topic(topic, root=self.topic_root)
        if parsed is None:
            raise ClientError(ClientErrorCode.PROTOCOL_ERROR, f"토픽 형식 오류: {topic!r}")

        try:
            header = ProtocolHeaderDTO.model_validate_json(frames[-1])
        except ValidationError as e:
            raise ClientError(
                ClientErrorCode.PROTOCOL_ERROR,
                f"ProtocolHeaderDTO 파싱 실패 [{topic}]: {e}",
            ) from e

        if header.collector_address != parsed.collector_address:
            logger.debug(
                "topic/header collector_address 불일치: topic=%s header=%s",
                parsed.collector_address,
                header.collector_address,
            )

        logger.debug("zmq sub [%s] %s", topic, header.msg_type.value)
        return IpcMessage(
            kind=parsed.kind,
            header=header,
            topic=topic,
            address=parsed.collector_address,
        )
