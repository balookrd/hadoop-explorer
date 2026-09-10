"""
ETag Middleware для автоматической генерации ETag и поддержки
условных HTTP-запросов (If-None-Match -> 304 Not Modified).
"""

import hashlib
from typing import Callable, Awaitable
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class ETagMiddleware(BaseHTTPMiddleware):
    """
    Автоматически генерирует слабый ETag (W/"<sha256-hash>") для GET/HEAD ответов
    и возвращает HTTP 304 Not Modified, если клиентский заголовок If-None-Match совпадает.
    """

    def __init__(self, app, weak: bool = True):
        super().__init__(app)
        self.weak = weak

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        if request.method not in ("GET", "HEAD"):
            return await call_next(request)

        response = await call_next(request)

        # Не обрабатываем ответы с ошибками или стримами событий SSE
        if response.status_code != 200:
            return response

        content_type = response.headers.get("content-type", "")
        if "text/event-stream" in content_type:
            return response

        # Считываем тело ответа
        body_chunks = []
        async for chunk in response.body_iterator:
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            body_chunks.append(chunk)

        body = b"".join(body_chunks)
        digest = hashlib.sha256(body).hexdigest()[:16]
        etag = f'W/"{digest}"' if self.weak else f'"{digest}"'

        if_none_match = request.headers.get("if-none-match")
        if if_none_match:
            client_tags = [t.strip() for t in if_none_match.split(",")]
            if etag in client_tags or "*" in client_tags or etag.replace('W/"', '"') in client_tags:
                not_modified = Response(status_code=304)
                not_modified.headers["ETag"] = etag
                for h in ("date", "cache-control", "traceparent", "x-request-id"):
                    if h in response.headers:
                        not_modified.headers[h] = response.headers[h]
                return not_modified

        new_response = Response(
            content=body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )
        new_response.headers["ETag"] = etag
        return new_response
