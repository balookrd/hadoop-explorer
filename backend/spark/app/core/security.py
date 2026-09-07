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

from app.services.storage import storage_service

security_bearer = HTTPBearer(auto_error=False)

def _get_trusted_proxies() -> set[str]:
    base = {"127.0.0.1", "::1", "localhost", "testclient"}
    env_p = os.getenv("TRUSTED_PROXIES", "")
    if env_p:
        base.update(p.strip() for p in env_p.split(",") if p.strip())
    return base

def get_client_ip(request: Request) -> str:
    client_host = request.client.host if request.client else "unknown"
    if client_host in _get_trusted_proxies():
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()
    return client_host

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
    auth_method: str = "mock"
    token_jti: Optional[str] = None

def create_access_token(user_data: dict, expires_delta: Optional[datetime.timedelta] = None) -> str:
    to_encode = user_data.copy()
    expire = datetime.datetime.now(datetime.timezone.utc) + (
        expires_delta or datetime.timedelta(minutes=settings.auth.jwt.expire_minutes)
    )
    jti = to_encode.get("jti") or str(uuid.uuid4())
    to_encode.update({"exp": expire, "jti": jti})
    return create_jwt_token(
        data=to_encode,
        secret_key=settings.auth.jwt.secret_key,
        algorithm=settings.auth.jwt.algorithm,
        expires_delta=expires_delta or datetime.timedelta(minutes=settings.auth.jwt.expire_minutes)
    )

async def get_current_user_optional(
    request: Request,
    auth_header: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer)
) -> Optional[UserSession]:
    token = None
    is_cookie_auth = False

    # 1. Заголовок Authorization: Bearer <token>
    if auth_header and auth_header.scheme.lower() == "bearer" and auth_header.credentials:
        token = auth_header.credentials
    # 2. HttpOnly Cookie сессии (защита от XSS/CWE-312)
    elif "session_token" in request.cookies:
        token = request.cookies.get("session_token")
        is_cookie_auth = True
    elif "access_token" in request.cookies:
        token = request.cookies.get("access_token")
        is_cookie_auth = True

    if not token:
        return None

    # Защита от CSRF атак при Cookie-аутентификации (CWE-352)
    verify_csrf(request, is_cookie_auth)

    # 1. Проверка на отзыв токена (CWE-613) через StorageService
    if storage_service.is_token_revoked(token):
        return None

    admin_groups = set(settings.acl.ui_access.admin_groups)

    # 2. Проверка активной сессии в персистентной БД SessionStore
    session_data = storage_service.get_session(token)
    if session_data:
        groups = session_data.get("groups", [])
        is_admin = session_data.get("is_admin")
        if is_admin is None:
            is_admin = bool(set(groups) & admin_groups)
        return UserSession(
            username=session_data["username"],
            display_name=session_data.get("display_name", session_data["username"]),
            email=session_data.get("email"),
            groups=groups,
            is_admin=bool(is_admin),
            auth_method=session_data.get("auth_method", "mock"),
            token_jti=session_data.get("jti")
        )

    # 3. Fallback: декодирование JWT
    try:
        payload = decode_jwt_token(
            token=token,
            secret_key=settings.auth.jwt.secret_key,
            algorithms=[settings.auth.jwt.algorithm]
        )
        if not payload:
            return None

        jti = payload.get("jti")
        if jti and storage_service.is_token_revoked(jti):
            return None

        username: str = payload.get("sub") or payload.get("username")
        if not username:
            return None

        groups: List[str] = payload.get("groups", [])
        is_admin = bool(set(groups) & admin_groups) or payload.get("is_admin", False)
        return UserSession(
            username=username,
            display_name=payload.get("display_name", username),
            email=payload.get("email"),
            groups=groups,
            is_admin=is_admin,
            auth_method=payload.get("auth_method", "jwt"),
            token_jti=jti
        )
    except (jwt.PyJWTError, Exception):
        return None

async def get_current_user(
    request: Request,
    auth_header: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer)
) -> UserSession:
    user = await get_current_user_optional(request, auth_header)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Необходима авторизация или токен был отозван",
            headers={"WWW-Authenticate": "Bearer"}
        )
    return user

def require_admin(current_user: UserSession = Depends(get_current_user)) -> UserSession:
    if not current_user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Требуются права администратора")
    return current_user

