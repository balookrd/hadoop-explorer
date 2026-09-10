import httpx
import logging
from typing import Any

logger = logging.getLogger(__name__)

_HTTP2_SUPPORTED = False
try:
    import h2  # noqa: F401

    _HTTP2_SUPPORTED = True
except ImportError:
    _HTTP2_SUPPORTED = False


def create_async_http_client(
    timeout: float = 30.0,
    max_keepalive: int = 20,
    max_connections: int = 50,
    keepalive_expiry: float = 30.0,
    follow_redirects: bool = False,
    enable_http2: bool = True,
    **kwargs: Any,
) -> httpx.AsyncClient:
    """
    Создает оптимизированный экземпляр httpx.AsyncClient с пулингом соединений
    и автосогласованием HTTP/2 (при наличии пакета h2) / HTTP/1.1 fallback.
    """
    use_http2 = _HTTP2_SUPPORTED if enable_http2 else False
    limits = httpx.Limits(
        max_keepalive_connections=max_keepalive,
        max_connections=max_connections,
        keepalive_expiry=keepalive_expiry,
    )
    timeout_config = httpx.Timeout(timeout, connect=min(float(timeout), 10.0))

    return httpx.AsyncClient(
        http2=use_http2,
        timeout=timeout_config,
        limits=limits,
        follow_redirects=follow_redirects,
        **kwargs,
    )
