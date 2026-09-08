import datetime
from typing import Optional
from fastapi import Request

from backend.common.core.security import (
    CommonUserSession,
    verify_csrf as common_verify_csrf,
    create_jwt_token,
    decode_jwt_token,
    make_get_current_user,
)
from backend.common.core.rate_limiter import get_client_ip, is_trusted_proxy
from app.core.config import settings
from app.services.storage import storage_service


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


get_current_user, get_current_user_optional = make_get_current_user(
    get_secret_key=lambda: settings.auth.jwt.secret_key,
    get_algorithm=lambda: settings.auth.jwt.algorithm,
    get_cookie_names=lambda: ["access_token", "sql_explorer_session", "session_token", "hadoop_explorer_session"],
    get_cors_origins=lambda: settings.server.cors_origins,
    get_storage_service=lambda: storage_service,
    admin_resolver=lambda username, groups, data: bool(set(groups) & set(settings.acl.ui_access.admin_groups)),
    user_session_class=UserSession,
)
