"""
Модуль распределенной трассировки OpenTelemetry и W3C Trace Context (RFC 00-traceid-spanid-01)
для платформы Hadoop Explorer.
"""

import os
import secrets
import time
from contextvars import ContextVar
from typing import Any, Callable, Dict, Optional, Awaitable
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

_current_trace_id: ContextVar[Optional[str]] = ContextVar("current_trace_id", default=None)
_current_span_id: ContextVar[Optional[str]] = ContextVar("current_span_id", default=None)


def get_current_trace_id() -> Optional[str]:
    """Возвращает текущий W3C trace_id (32 hex-символа)."""
    return _current_trace_id.get()


def get_current_span_id() -> Optional[str]:
    """Возвращает текущий W3C span_id (16 hex-символов)."""
    return _current_span_id.get()


def generate_trace_id() -> str:
    """Генерирует уникальный 128-битный trace_id в виде 32 hex-символов."""
    return secrets.token_hex(16)


def generate_span_id() -> str:
    """Генерирует уникальный 64-битный span_id в виде 16 hex-символов."""
    return secrets.token_hex(8)


def format_w3c_traceparent(trace_id: str, span_id: str, sampled: bool = True) -> str:
    """Формирует канонический W3C traceparent заголовок."""
    flags = "01" if sampled else "00"
    return f"00-{trace_id}-{span_id}-{flags}"


def parse_w3c_traceparent(header_value: str) -> Optional[tuple[str, str, bool]]:
    """Парсит заголовок W3C traceparent."""
    if not header_value:
        return None
    parts = header_value.strip().split("-")
    if len(parts) >= 4 and parts[0] == "00":
        trace_id = parts[1]
        span_id = parts[2]
        sampled = parts[3] == "01"
        if len(trace_id) == 32 and len(span_id) == 16:
            return trace_id, span_id, sampled
    return None


class Span:
    """Представляет локальный спан распределенной трассировки."""

    def __init__(self, name: str, trace_id: str, span_id: str, parent_span_id: Optional[str] = None):
        self.name = name
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent_span_id = parent_span_id
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.attributes: Dict[str, Any] = {}
        self.status = "OK"

    def set_attribute(self, key: str, value: Any) -> "Span":
        self.attributes[key] = value
        return self

    def set_status(self, status: str, error: Optional[Exception] = None) -> "Span":
        self.status = status
        if error:
            self.attributes["error.type"] = type(error).__name__
            self.attributes["error.message"] = str(error)
        return self

    def finish(self) -> None:
        self.end_time = time.time()

    def __enter__(self) -> "Span":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            self.set_status("ERROR", exc_val)
        self.finish()

    async def __aenter__(self) -> "Span":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            self.set_status("ERROR", exc_val)
        self.finish()


class OpenTelemetryTracer:
    """Легковесный трейсер OpenTelemetry с контекстным распространением W3C."""

    def start_span(self, name: str, parent_span_id: Optional[str] = None) -> Span:
        trace_id = get_current_trace_id() or generate_trace_id()
        span_id = generate_span_id()
        _current_trace_id.set(trace_id)
        _current_span_id.set(span_id)
        return Span(name=name, trace_id=trace_id, span_id=span_id, parent_span_id=parent_span_id)


global_tracer = OpenTelemetryTracer()


class OpenTelemetryMiddleware(BaseHTTPMiddleware):
    """
    HTTP Middleware для автоматического извлечения/генерации W3C Trace Context
    и проброса заголовков traceparent клиенту и дочерним службам.
    """

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        traceparent_header = request.headers.get("traceparent")
        parsed = parse_w3c_traceparent(traceparent_header) if traceparent_header else None

        if parsed:
            trace_id, parent_span_id, _ = parsed
        else:
            trace_id = generate_trace_id()
            parent_span_id = None

        span_id = generate_span_id()
        token_trace = _current_trace_id.set(trace_id)
        token_span = _current_span_id.set(span_id)

        try:
            span = Span(
                name=f"{request.method} {request.url.path}",
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=parent_span_id,
            )
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.url", str(request.url))
            span.set_attribute("http.target", request.url.path)

            response = await call_next(request)

            span.set_attribute("http.status_code", response.status_code)
            span.finish()

            # Добавляем W3C traceparent в ответ
            response.headers["traceparent"] = format_w3c_traceparent(trace_id, span_id)
            return response
        except Exception as exc:
            span.set_status("ERROR", exc)
            span.finish()
            raise
        finally:
            _current_trace_id.reset(token_trace)
            _current_span_id.reset(token_span)
