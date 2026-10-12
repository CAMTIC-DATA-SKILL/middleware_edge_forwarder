"""ZeroMQ 소켓 공통 (asyncio): Context, bind/connect, close."""

from __future__ import annotations

import logging

import zmq
import zmq.asyncio

from client.errors import ClientError, to_client_error
from model.error_model import ClientErrorCode

logger = logging.getLogger(__name__)


class BaseZmqClient:
    """단일 소켓의 수명주기. 엔드포인트가 여럿이면 한 소켓으로 모두 bind/connect 한다."""

    socket_type: int

    def __init__(self, endpoints: list[str], *, bind: bool, linger_ms: int = 0):
        if not endpoints:
            raise ClientError(ClientErrorCode.INVALID_CONFIG, "ZeroMQ 엔드포인트가 비어 있습니다")
        self.endpoints = list(endpoints)
        self.bind = bind
        self.linger_ms = linger_ms
        self._sock: zmq.asyncio.Socket | None = None

    @property
    def is_open(self) -> bool:
        return self._sock is not None

    @property
    def sock(self) -> zmq.asyncio.Socket:
        if self._sock is None:
            raise ClientError(
                ClientErrorCode.CONNECT_FAILED,
                f"소켓 미연결: {self._target()}",
            )
        return self._sock

    def _target(self) -> str:
        mode = "bind" if self.bind else "connect"
        return f"{mode}:{','.join(self.endpoints)}"

    def connect(self) -> None:
        # 이미 열려 있으면 닫고 다시 연다 (재시도·재연결용)
        self.close()
        try:
            self._sock = zmq.asyncio.Context.instance().socket(self.socket_type)
            self._sock.setsockopt(zmq.LINGER, self.linger_ms)
            self._configure_socket(self._sock)
            for endpoint in self.endpoints:
                if self.bind:
                    self._sock.bind(endpoint)
                else:
                    self._sock.connect(endpoint)
        except Exception as e:
            self.close()
            raise to_client_error(e, default=ClientErrorCode.CONNECT_FAILED) from e
        logger.debug("zmq connected: %s type=%s", self._target(), self.socket_type)

    def _configure_socket(self, sock: zmq.asyncio.Socket) -> None:
        """서브클래스에서 구독 토픽 등 설정."""

    def close(self) -> None:
        sock, self._sock = self._sock, None
        if sock is None:
            return
        try:
            sock.close(linger=self.linger_ms)
        except Exception as e:
            logger.debug("zmq close failed: %s", e)
