"""클라이언트 예외. 와이어에는 code만, 로그에는 detail을 쓴다."""

from __future__ import annotations

from model.error_model import ClientErrorCode


class ClientError(Exception):
    def __init__(self, code: ClientErrorCode, detail: str = ""):
        self.code = code
        self.detail = detail
        super().__init__(detail or f"{code.value}({code.label})")


def to_client_error(
    exc: BaseException,
    *,
    default: ClientErrorCode = ClientErrorCode.UNKNOWN,
) -> ClientError:
    """라이브러리/표준 예외를 ClientError로 정규화한다."""
    if isinstance(exc, ClientError):
        return exc

    detail = str(exc)

    if isinstance(exc, TimeoutError) or "timeout" in detail.lower():
        return ClientError(ClientErrorCode.TIMEOUT, detail)

    if isinstance(exc, (ConnectionResetError, BrokenPipeError)):
        return ClientError(ClientErrorCode.CONNECTION_CLOSED, detail)

    if isinstance(exc, (ConnectionError, OSError)):
        return ClientError(ClientErrorCode.CONNECT_FAILED, detail)

    return ClientError(default, detail)
