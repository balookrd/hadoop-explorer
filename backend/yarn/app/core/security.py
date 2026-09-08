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
    make_token_helpers,
)
from app.core.config import settings
from app.services.storage import storage_service
from backend.common.models.auth import UserSession, Role
from app.core.acl import check_ui_access

security_scheme = HTTPBearer(auto_error=False)


def verify_csrf(request: Request, is_cookie_auth: bool):
    """
    Защита от Cross-Site Request Forgery (CWE-352) через унифицированный backend.common.core.security.
    """
    common_verify_csrf(request, is_cookie_auth, allowed_cors=settings.server.cors_origins)


create_access_token, _raw_decode_access_token = make_token_helpers(
    get_secret_key=lambda: settings.auth.jwt.secret_key,
    get_algorithm=lambda: settings.auth.jwt.algorithm,
    default_expire_minutes=settings.auth.jwt.expire_minutes,
)


def decode_access_token(token: str) -> Optional[dict]:
    payload = _raw_decode_access_token(token)
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
    get_cookie_names=lambda: ["yarn_explorer_session", "hadoop_explorer_session"],
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
