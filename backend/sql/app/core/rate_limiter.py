from typing import Optional, Callable
from backend.common.core.rate_limiter import (
    RateLimiter as _CommonRateLimiter,
    get_client_ip,
    is_trusted_proxy,
)
from app.services.storage import storage_service


class RateLimiter(_CommonRateLimiter):
    def __init__(
        self,
        max_requests: int = 10,
        window_seconds: int = 60,
        storage_getter: Optional[Callable] = None,
    ):
        super().__init__(
            max_requests=max_requests,
            window_seconds=window_seconds,
            storage_getter=storage_getter or (lambda: storage_service),
        )


auth_rate_limiter = RateLimiter(max_requests=5, window_seconds=60)

__all__ = ["RateLimiter", "get_client_ip", "is_trusted_proxy", "auth_rate_limiter"]
