from typing import Optional
from fastapi import Request
from fastapi.security import HTTPBearer

from backend.common.core.security import (
    verify_csrf as common_verify_csrf,
    hash_token,
    create_jwt_token,
    decode_jwt_token,
    extract_token_from_request as common_extract_token,
    make_get_current_user,
    make_token_helpers,
)
from app.core.config import settings
from app.services.storage import storage_service
from backend.common.models.auth import TokenPayload, UserInfo
from app.core.acl import is_global_admin


bearer_scheme = HTTPBearer(auto_error=False)

_raw_create_access_token, _raw_decode_access_token = make_token_helpers(
    get_secret_key=lambda: settings.security.secret_key,
    get_algorithm=lambda: settings.security.algorithm,
    default_expire_minutes=settings.security.access_token_expire_minutes,
)


def create_access_token(user: UserInfo) -> str:
    payload = {
        "sub": user.username,
        "display_name": user.display_name,
        "email": user.email,
        "groups": user.groups,
        "is_admin": user.is_admin,
    }
    return _raw_create_access_token(payload)


def decode_access_token(token: str) -> Optional[TokenPayload]:
    payload = _raw_decode_access_token(token)
    if not payload or "sub" not in payload:
        return None

    return TokenPayload(
        sub=payload["sub"],
        display_name=payload.get("display_name", payload["sub"]),
        email=payload.get("email"),
        groups=payload.get("groups", []),
        exp=payload.get("exp", 0),
        jti=payload.get("jti"),
    )


def verify_csrf(request: Request, is_cookie_auth: bool):
    """
    Защита от CSRF через унифицированный модуль backend.common.core.security.
    """
    common_verify_csrf(request, is_cookie_auth, allowed_cors=settings.server.cors_origins)


def extract_token_from_request(request: Request) -> Optional[str]:
    """
    Извлекает JWT-токен из Cookie или заголовка Authorization: Bearer.
    """
    token, _ = common_extract_token(request, [settings.security.cookie_name])
    return token


get_current_user, get_current_user_optional = make_get_current_user(
    get_secret_key=lambda: settings.security.secret_key,
    get_algorithm=lambda: settings.security.algorithm,
    get_cookie_names=lambda: [settings.security.cookie_name, "hadoop_explorer_session"],
    get_cors_origins=lambda: settings.server.cors_origins,
    get_storage_service=lambda: storage_service,
    admin_resolver=lambda username, groups, data: is_global_admin(username, groups),
    user_session_class=UserInfo,
)
