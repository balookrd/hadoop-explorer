import os
import time
from typing import Optional
from fastapi import Request, HTTPException, status

from app.core.security import get_client_ip
from app.services.storage import storage_service


class RateLimiter:
    """
    Универсальный ограничитель частоты запросов (Rate Limiter) для sql-explorer.
    Поддерживает Redis, PostgreSQL и SQLite через StorageService.
    """
    def __init__(self, max_requests: int = 10, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds

    def is_allowed(self, key: str, now: Optional[float] = None) -> tuple[bool, int]:
        return storage_service.check_and_record_rate_limit(
            key=key,
            max_requests=self.max_requests,
            window_seconds=self.window_seconds,
            now=now,
        )

    def check_limit(self, key: str, request: Request):
        allowed, retry_after = self.is_allowed(key=key)
        if not allowed:
            client_ip = get_client_ip(request)
            from app.core.audit import audit_log
            audit_log(
                action="RATE_LIMIT_EXCEEDED",
                username="anonymous",
                client_ip=client_ip,
                details={"path": request.url.path, "key": key, "retry_after": retry_after},
                status="WARNING",
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Слишком много попыток. Пожалуйста, повторите через {retry_after} сек.",
                headers={"Retry-After": str(retry_after)},
            )

    def __call__(self, request: Request):
        client_ip = get_client_ip(request)
        self.check_limit(key=f"ip:{client_ip}", request=request)


auth_rate_limiter = RateLimiter(max_requests=5, window_seconds=60)
