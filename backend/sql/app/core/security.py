import os
import uuid
import datetime
import ipaddress
from typing import Optional, List
from pydantic import BaseModel
import jwt
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from backend.common.core.security import (
    verify_csrf as common_verify_csrf,
    hash_token,
    create_jwt_token,
    decode_jwt_token
)
from app.core.config import settings

security_bearer = HTTPBearer(auto_error=False)


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


def verify_csrf(request: Request, is_cookie_auth: bool):
    """
    Защита от Cross-Site Request Forgery (CWE-352) через унифицированный backend.common.core.security.
    """
    common_verify_csrf(request, is_cookie_auth, allowed_cors=settings.server.cors_origins)


class UserSession(BaseModel):
    username: str
    display_name: str
    email: Optional[str] = None
    groups: List[str] = []
    is_admin: bool = False
    auth_method: str = "ldap"  # ldap, kerberos, mock


def create_access_token(data: dict, expires_delta: Optional[datetime.timedelta] = None) -> str:
    return create_jwt_token(
        data=data,
        secret_key=settings.auth.jwt.secret_key,
        algorithm=settings.auth.jwt.algorithm,
        expires_minutes=settings.auth.jwt.expire_minutes,
        expires_delta=expires_delta
    )


def decode_access_token(token: str) -> Optional[dict]:
    return decode_jwt_token(
        token=token,
        secret_key=settings.auth.jwt.secret_key,
        algorithms=[settings.auth.jwt.algorithm]
    )


# L1 In-memory кэш отозванных токенов для мгновенной проверки (с ограничением размера)
_revoked_tokens_cache: set[str] = set()
_MAX_REVOKED_CACHE_SIZE = 10000


def _add_to_revoked_cache(h: str):
    if len(_revoked_tokens_cache) >= _MAX_REVOKED_CACHE_SIZE:
        _revoked_tokens_cache.clear()
    _revoked_tokens_cache.add(h)


async def revoke_token_in_db(
    token: str,
    username: str = "unknown",
    expires_at: Optional[datetime.datetime] = None
):
    """
    Отзывает JWT токен через StorageService (Redis/PostgreSQL/SQLite).
    """
    h = hash_token(token)
    _add_to_revoked_cache(h)

    from app.services.storage import storage_service
    storage_service.revoke_token(token, username=username, expires_at=expires_at)


async def is_token_revoked_in_db(token: str) -> bool:
    """
    Проверяет отзыв токена через L1 in-memory кэш и StorageService.
    """
    h = hash_token(token)
    if h in _revoked_tokens_cache:
        return True

    from app.services.storage import storage_service
    if storage_service.is_token_revoked(token):
        _add_to_revoked_cache(h)
        return True
    return False


async def get_current_user(
    request: Request,
    auth_header: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer)
) -> UserSession:
    token = None
    is_cookie_auth = False
    # 1. Сначала проверяем Bearer заголовок
    if auth_header and auth_header.credentials:
        token = auth_header.credentials
    # 2. Либо Cookie сессии (удобно для браузера)
    elif "access_token" in request.cookies:
        token = request.cookies.get("access_token")
        is_cookie_auth = True

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется авторизация",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Защита от CSRF атак при Cookie аутентификации
    verify_csrf(request, is_cookie_auth)

    if await is_token_revoked_in_db(token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Токен отозван при выходе из системы",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = decode_access_token(token)
    if not payload or "sub" not in payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недействительный или просроченный токен",
            headers={"WWW-Authenticate": "Bearer"},
        )

    groups = payload.get("groups", [])
    admin_groups = set(settings.acl.ui_access.admin_groups)
    is_admin = bool(set(groups) & admin_groups)

    return UserSession(
        username=payload["sub"],
        display_name=payload.get("display_name", payload["sub"]),
        email=payload.get("email"),
        groups=groups,
        is_admin=is_admin,
        auth_method=payload.get("auth_method", "unknown")
    )
