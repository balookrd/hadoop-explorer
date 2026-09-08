from datetime import timedelta
from typing import Optional
from fastapi import Request, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from backend.common.core.security import (
    verify_csrf as common_verify_csrf,
    hash_token,
    create_jwt_token,
    decode_jwt_token,
    make_get_current_user,
)
from app.core.config import settings
from app.services.storage import storage_service
from app.models.auth import UserSession, Role
from app.core.acl import check_ui_access

security_scheme = HTTPBearer(auto_error=False)


def verify_csrf(request: Request, is_cookie_auth: bool):
    """
    Защита от Cross-Site Request Forgery (CWE-352) через унифицированный backend.common.core.security.
    """
    common_verify_csrf(request, is_cookie_auth, allowed_cors=settings.server.cors_origins)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    return create_jwt_token(
        data=data,
        secret_key=settings.auth.jwt.secret_key,
        algorithm=settings.auth.jwt.algorithm,
        expires_minutes=settings.auth.jwt.expire_minutes,
        expires_delta=expires_delta,
    )


def decode_access_token(token: str) -> Optional[dict]:
    payload = decode_jwt_token(
        token=token, secret_key=settings.auth.jwt.secret_key, algorithms=[settings.auth.jwt.algorithm]
    )
    if not payload:
        return None

    jti = payload.get("jti")
    if jti and storage_service.is_token_revoked(jti):
        return None
    return payload


def _resolve_session_fields(data: dict) -> dict:
    from app.api.auth import _resolve_global_role

    role = data.get("system_role")
    if isinstance(role, str):
        try:
            role = Role(role)
        except ValueError:
            role = Role.READER
    elif not isinstance(role, Role):
        role = _resolve_global_role(data.get("username", ""), data.get("groups", []))
    return {
        "system_role": role,
        "is_admin": (role == Role.ADMIN) or bool(data.get("is_admin", False)),
    }


_get_current_user, _get_current_user_optional = make_get_current_user(
    get_secret_key=lambda: settings.auth.jwt.secret_key,
    get_algorithm=lambda: settings.auth.jwt.algorithm,
    get_cookie_names=lambda: ["access_token", "hdfs_explorer_session", "session_token", "hadoop_explorer_session"],
    get_cors_origins=lambda: settings.server.cors_origins,
    get_storage_service=lambda: storage_service,
    admin_resolver=lambda username, groups, data: _resolve_session_fields(data if isinstance(data, dict) else {})[
        "is_admin"
    ],
    user_session_class=UserSession,
    extra_user_fields=_resolve_session_fields,
)


async def get_current_user(
    request: Request, credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_scheme)
) -> UserSession:
    user = await _get_current_user_optional(request)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется авторизация",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not check_ui_access(user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ к системе запрещен политикой UI Access",
        )

    return user
