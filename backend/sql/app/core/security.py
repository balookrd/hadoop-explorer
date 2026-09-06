import os
import uuid
import datetime
import ipaddress
from typing import Optional, List
from pydantic import BaseModel
import jwt
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
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

from urllib.parse import urlparse

def _is_allowed_origin(url_str: str, request: Request, allowed_cors: list[str]) -> bool:
    if not url_str:
        return False
    try:
        parsed = urlparse(url_str)
        if not parsed.scheme or not parsed.netloc:
            return False
        if parsed.scheme.lower() not in ("http", "https"):
            return False

        target_origin = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}".rstrip("/")
        target_netloc = parsed.netloc.lower()

        for allowed in allowed_cors:
            if allowed == "*":
                return True
            p_allowed = urlparse(allowed)
            if p_allowed.netloc:
                if f"{p_allowed.scheme.lower()}://{p_allowed.netloc.lower()}".rstrip("/") == target_origin:
                    return True
            elif allowed.rstrip("/").lower() == target_origin:
                return True

        req_host = request.headers.get("host", "").lower()
        if req_host and target_netloc == req_host:
            return True

        base_netloc = request.base_url.netloc.lower()
        if base_netloc and target_netloc == base_netloc:
            return True

        base_url_str = str(request.base_url).rstrip("/").lower()
        if target_origin == base_url_str:
            return True

        return False
    except Exception:
        return False

def verify_csrf(request: Request, is_cookie_auth: bool):
    """
    Защита от Cross-Site Request Forgery (CWE-352).
    Если запрос аутентифицирован через Cookie и изменяет состояние (POST, PUT, DELETE, PATCH),
    требуется подтверждение легитимности источника (Sec-Fetch-Site, Origin, Referer, X-Requested-With).
    """
    if not is_cookie_auth:
        return

    if request.method in ("POST", "PUT", "DELETE", "PATCH"):
        sec_fetch_site = request.headers.get("Sec-Fetch-Site")
        if sec_fetch_site and sec_fetch_site.lower() == "cross-site":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="CSRF protection: межсайтовый запрос отклонен (Sec-Fetch-Site: cross-site)"
            )

        x_requested_with = request.headers.get("X-Requested-With")
        if x_requested_with == "XMLHttpRequest":
            return

        origin = request.headers.get("Origin")
        if origin and _is_allowed_origin(origin, request, settings.server.cors_origins):
            return

        referer = request.headers.get("Referer")
        if referer and _is_allowed_origin(referer, request, settings.server.cors_origins):
            return

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF protection: запрос отклонен политикой безопасности источника"
        )

class UserSession(BaseModel):
    username: str
    display_name: str
    email: Optional[str] = None
    groups: List[str] = []
    is_admin: bool = False
    auth_method: str = "ldap"  # ldap, kerberos, mock

def create_access_token(data: dict, expires_delta: Optional[datetime.timedelta] = None) -> str:
    to_encode = data.copy()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    if expires_delta:
        expire = now_utc + expires_delta
    else:
        expire = now_utc + datetime.timedelta(minutes=settings.auth.jwt.expire_minutes)
    to_encode.update({
        "exp": expire,
        "iat": now_utc,
        "jti": uuid.uuid4().hex
    })
    encoded_jwt = jwt.encode(to_encode, settings.auth.jwt.secret_key, algorithm=settings.auth.jwt.algorithm)
    return encoded_jwt

def decode_access_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, settings.auth.jwt.secret_key, algorithms=[settings.auth.jwt.algorithm])
        return payload
    except jwt.PyJWTError:
        return None

import hashlib
from sqlalchemy import select, delete
from app.db.session import AsyncSessionLocal
from app.models.models import RevokedToken

def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()

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
