import os
import ipaddress
from typing import Optional, Callable
from fastapi import Request, HTTPException, status


def _get_trusted_proxies() -> set[str]:
    base = {"127.0.0.1", "::1", "localhost", "testclient"}
    env_p = os.getenv("TRUSTED_PROXIES", "")
    if env_p:
        base.update(p.strip() for p in env_p.split(",") if p.strip())
    return base


def _get_trusted_cidrs() -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    env_c = os.getenv("TRUSTED_CIDRS", "")
    cidrs = []
    if env_c:
        for net in env_c.split(","):
            net = net.strip()
            if net:
                try:
                    cidrs.append(ipaddress.ip_network(net, strict=False))
                except ValueError:
                    pass
    return cidrs


def is_trusted_proxy(host: str) -> bool:
    if host in _get_trusted_proxies():
        return True
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_loopback:
            return True
        for cidr in _get_trusted_cidrs():
            if ip in cidr:
                return True
        return False
    except ValueError:
        return False


def get_client_ip(request: Request) -> str:
    """
    Безопасное определение IP-адреса клиента с защитой от IP Spoofing (CWE-348 / CWE-290).
    X-Forwarded-For считывается только если непосредственный request.client.host является доверенным прокси.
    """
    if not request.client or not request.client.host:
        return "unknown"

    direct_ip = request.client.host
    if is_trusted_proxy(direct_ip):
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            ips = [ip.strip() for ip in forwarded_for.split(",") if ip.strip()]
            if ips:
                return ips[0]
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()

    return direct_ip


class RateLimiter:
    """
    Ограничитель частоты запросов на базе скользящего окна (sliding window).
    Делегирует проверку и хранение в переданный storage_service (или по умолчанию в backend.common.db.storage.storage_service).
    """
    def __init__(self, max_requests: int = 10, window_seconds: int = 60, storage_getter: Optional[Callable] = None):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._storage_getter = storage_getter

    def _get_storage(self):
        if self._storage_getter:
            return self._storage_getter()
        from backend.common.db.storage import storage_service
        return storage_service

    def _get_client_ip(self, request: Request) -> str:
        return get_client_ip(request)

    def is_allowed(self, key: str, now: Optional[float] = None) -> tuple[bool, int]:
        storage = self._get_storage()
        return storage.check_and_record_rate_limit(
            key=key,
            max_requests=self.max_requests,
            window_seconds=self.window_seconds,
            now=now,
        )

    async def is_allowed_async(self, key: str, now: Optional[float] = None) -> tuple[bool, int]:
        """Неблокирующая проверка rate limit через асинхронный метод storage."""
        storage = self._get_storage()
        if hasattr(storage, "check_and_record_rate_limit_async"):
            return await storage.check_and_record_rate_limit_async(
                key=key,
                max_requests=self.max_requests,
                window_seconds=self.window_seconds,
                now=now,
            )
        import anyio
        return await anyio.to_thread.run_sync(
            storage.check_and_record_rate_limit, key, self.max_requests, self.window_seconds, now
        )

    def check_limit(self, key: str, request: Request):
        allowed, retry_after = self.is_allowed(key=key)
        if not allowed:
            self._raise_rate_limit_exceeded(key=key, request=request, retry_after=retry_after)

    async def check_limit_async(self, key: str, request: Request):
        """Асинхронная неблокирующая проверка лимита частоты запросов."""
        allowed, retry_after = await self.is_allowed_async(key=key)
        if not allowed:
            self._raise_rate_limit_exceeded(key=key, request=request, retry_after=retry_after)

    def _raise_rate_limit_exceeded(self, key: str, request: Request, retry_after: int):
        client_ip = self._get_client_ip(request)
        from backend.common.core.audit import audit_log
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

    async def __call__(self, request: Request):
        client_ip = self._get_client_ip(request)
        await self.check_limit_async(key=f"ip:{client_ip}", request=request)


auth_rate_limiter = RateLimiter(max_requests=10, window_seconds=60)

