from backend.common.core.rate_limiter import (
    RateLimiter,
    get_client_ip,
    is_trusted_proxy,
)
from app.services.storage import storage_service

auth_rate_limiter = RateLimiter(max_requests=5, window_seconds=60, storage_getter=lambda: storage_service)

__all__ = ["RateLimiter", "get_client_ip", "is_trusted_proxy", "auth_rate_limiter"]
