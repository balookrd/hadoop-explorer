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
from backend.common.core.rate_limiter import get_client_ip
from app.core.config import settings
from app.services.storage import storage_service

security_bearer = HTTPBearer(auto_error=False)


def verify_csrf(request: Request, is_cookie_auth: bool):
    """
    Защита от Cross-Site Request Forgery (CWE-352) через унифицированный backend.common.core.security.
    """
    common_verify_csrf(request, is_cookie_auth, allowed_cors=settings.server.cors_origins)


class UserSession(CommonUserSession):
    auth_method: str = "mock"
    token_jti: Optional[str] = None


def create_access_token(user_data: dict, expires_delta: Optional[datetime.timedelta] = None) -> str:
    to_encode = user_data.copy()
    return create_jwt_token(
        data=to_encode,
        secret_key=settings.auth.jwt.secret_key,
        algorithm=settings.auth.jwt.algorithm,
        expires_minutes=settings.auth.jwt.expire_minutes,
        expires_delta=expires_delta,
    )


_get_current_user, _get_current_user_optional = make_get_current_user(
    get_secret_key=lambda: settings.auth.jwt.secret_key,
    get_algorithm=lambda: settings.auth.jwt.algorithm,
    get_cookie_names=lambda: ["spark_explorer_session", "hadoop_explorer_session"],
    get_cors_origins=lambda: settings.server.cors_origins,
    get_storage_service=lambda: storage_service,
    admin_resolver=lambda username, groups, data: (
        bool(set(groups) & set(settings.acl.ui_access.admin_groups))
        or (isinstance(data, dict) and data.get("is_admin", False))
    ),
    user_session_class=UserSession,
    extra_user_fields=lambda data: {"token_jti": data.get("jti") or data.get("token_jti")},
)


async def get_current_user_optional(
    request: Request, auth_header: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer)
) -> Optional[UserSession]:
    return await _get_current_user_optional(request)


async def get_current_user(
    request: Request, auth_header: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer)
) -> UserSession:
    user = await get_current_user_optional(request, auth_header)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Необходима авторизация или токен был отозван",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def require_admin(current_user: UserSession = Depends(get_current_user)) -> UserSession:
    if not current_user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Требуются права администратора")
    return current_user
