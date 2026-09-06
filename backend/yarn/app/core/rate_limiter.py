import os
import time
import ipaddress
from typing import Optional
from fastapi import Request, HTTPException, status

from app.services.storage import storage_service

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
    Ограничитель частоты запросов на базе скользящего окна (sliding window),
    сохраняющий состояние в SQLite (yarn_explorer.db).
    
    Поддерживает совместную работу между процессами/воркерами Uvicorn
    и переживает перезапуск приложения.
    """
    def __init__(self, max_requests: int = 10, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds

    def _get_client_ip(self, request: Request) -> str:
        return get_client_ip(request)

    def is_allowed(self, key: str, now: Optional[float] = None) -> tuple[bool, int]:
        """Проверяет лимит и записывает попытку. Возвращает (is_allowed, retry_after)."""
        return storage_service.check_and_record_rate_limit(
            key=key,
            max_requests=self.max_requests,
            window_seconds=self.window_seconds,
            now=now,
        )

    def check_limit(self, key: str, request: Request):
        allowed, retry_after = self.is_allowed(key=key)
        if not allowed:
            client_ip = self._get_client_ip(request)
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
        client_ip = self._get_client_ip(request)
        self.check_limit(key=f"ip:{client_ip}", request=request)


auth_rate_limiter = RateLimiter(max_requests=10, window_seconds=60)

