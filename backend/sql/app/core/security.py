import datetime
from typing import Optional, List
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from backend.common.core.security import (
    CommonUserSession,
    verify_csrf as common_verify_csrf,
    hash_token,
    create_jwt_token,
    decode_jwt_token,
    make_get_current_user,
)
from backend.common.core.rate_limiter import get_client_ip, is_trusted_proxy
from app.core.config import settings
from app.services.storage import storage_service

security_bearer = HTTPBearer(auto_error=False)


def verify_csrf(request: Request, is_cookie_auth: bool):
    """
    Защита от Cross-Site Request Forgery (CWE-352) через унифицированный backend.common.core.security.
    """
    common_verify_csrf(request, is_cookie_auth, allowed_cors=settings.server.cors_origins)


class UserSession(CommonUserSession):
    auth_method: str = "ldap"  # ldap, kerberos, mock


def create_access_token(data: dict, expires_delta: Optional[datetime.timedelta] = None) -> str:
    return create_jwt_token(
        data=data,
        secret_key=settings.auth.jwt.secret_key,
        algorithm=settings.auth.jwt.algorithm,
        expires_minutes=settings.auth.jwt.expire_minutes,
        expires_delta=expires_delta,
    )


def decode_access_token(token: str) -> Optional[dict]:
    return decode_jwt_token(
        token=token, secret_key=settings.auth.jwt.secret_key, algorithms=[settings.auth.jwt.algorithm]
    )


class _RevokedCacheCompat(set):
    def clear(self):
        super().clear()
        if hasattr(storage_service, "l1_cache") and hasattr(storage_service.l1_cache, "clear"):
            storage_service.l1_cache.clear()


_revoked_tokens_cache = _RevokedCacheCompat()


async def revoke_token_in_db(token: str, username: str = "unknown", expires_at: Optional[datetime.datetime] = None):
    """
    Отзывает JWT токен через StorageService (Redis/PostgreSQL/SQLite).
    """
    h = hash_token(token)
    _revoked_tokens_cache.add(h)
    storage_service.revoke_token(token, username=username, expires_at=expires_at)


async def is_token_revoked_in_db(token: str) -> bool:
    """
    Проверяет отзыв токена через StorageService.
    """
    h = hash_token(token)
    if h in _revoked_tokens_cache:
        return True
    if storage_service.is_token_revoked(token):
        _revoked_tokens_cache.add(h)
        return True
    return False


_get_current_user, _get_current_user_optional = make_get_current_user(
    get_secret_key=lambda: settings.auth.jwt.secret_key,
    get_algorithm=lambda: settings.auth.jwt.algorithm,
    get_cookie_names=lambda: ["access_token", "hdfs_explorer_session", "session_token", "hadoop_explorer_session"],
    get_cors_origins=lambda: settings.server.cors_origins,
    get_storage_service=lambda: storage_service,
    admin_resolver=lambda username, groups, data: bool(set(groups) & set(settings.acl.ui_access.admin_groups)),
    user_session_class=UserSession,
)


async def get_current_user(
    request: Request, auth_header: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer)
) -> UserSession:
    token = None
    is_cookie_auth = False
    if auth_header and auth_header.credentials:
        token = auth_header.credentials
    else:
        for c_name in ["access_token", "hdfs_explorer_session", "session_token", "hadoop_explorer_session"]:
            if c_name in request.cookies:
                token = request.cookies.get(c_name)
                is_cookie_auth = True
                break

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется авторизация",
            headers={"WWW-Authenticate": "Bearer"},
        )

    verify_csrf(request, is_cookie_auth)

    if storage_service.is_token_revoked(token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Токен отозван при выходе из системы",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = await _get_current_user_optional(request)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недействительный или просроченный токен",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user
